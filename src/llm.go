package src

import (
	"context"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/goccy/go-json"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"go.yaml.in/yaml/v4"

	"github.com/Thearas/dodo/src/parser"
	"github.com/Thearas/dodo/src/prompt"
)

const (
	LLMYAMLPrefix = "```yaml\n"
	LLMJSONPrefix = "```json\n"
)

// LLMTableInput holds pre-parsed table information for LLM gendata config generation.
// All data should be prepared by the caller - no file reading happens in llm.go.
type LLMTableInput struct {
	DB           string // database name
	Name         string // table/view name
	Type         string // "table", "view", or "materialized_view"
	DDLContent   string // CREATE TABLE/VIEW statement
	StatsContent string // column statistics YAML (optional)
}

// partitionLineRe matches a single PARTITION line in a DDL statement.
var partitionLineRe = regexp.MustCompile(`(?im)^\s*PARTITION\s+\S+\s+VALUES\s+.*$`)

// TrimDDLPartitions reduces the number of PARTITION lines in a DDL to avoid
// blowing up LLM token limits. When a DDL has more than maxKeep partitions,
// only the first keepEdge and last keepEdge are retained, with a summary
// comment replacing the middle ones.
func TrimDDLPartitions(ddl string, maxKeep int) string {
	if maxKeep <= 0 {
		maxKeep = 5
	}

	matches := partitionLineRe.FindAllStringIndex(ddl, -1)
	if len(matches) <= maxKeep {
		return ddl
	}

	// Keep first keepEdge and last keepEdge partitions
	keepEdge := max(maxKeep/2, 1)
	omitted := len(matches) - keepEdge*2

	// Region to replace: from end of last "first-edge" match to start of first "last-edge" match
	cutStart := matches[keepEdge-1][1]          // end of last kept first-edge line
	cutEnd := matches[len(matches)-keepEdge][0] // start of first kept last-edge line

	summary := fmt.Sprintf("\n-- ... %d more partitions omitted ... (values are ordered and ranged between the above and below partitions)\n", omitted)

	var b strings.Builder
	b.Grow(len(ddl))
	b.WriteString(ddl[:cutStart])
	b.WriteString(summary)
	b.WriteString(ddl[cutEnd:])
	return b.String()
}

// Regexps for stripping DDL sections that waste LLM tokens.
var (
	// propertiesBlockRe matches a PROPERTIES(...) block (possibly multi-line) to end of DDL.
	propertiesBlockRe = regexp.MustCompile(`(?im)^\s*PROPERTIES\s*\([\s\S]*`)
	// statsDataSizeRe matches data_size lines in stats YAML.
	statsDataSizeRe = regexp.MustCompile(`(?im)^\s*data_size:.*$`)
	// statsMethodRe matches method lines in stats YAML.
	statsMethodRe = regexp.MustCompile(`(?im)^\s*method:.*$`)
)

// TrimDDLForLLM strips DDL sections that are useless for LLM data-generation
// decisions: PROPERTIES block and excessive partitions.
// Column definitions and COMMENTs are preserved.
func TrimDDLForLLM(ddl string) string {
	ddl = TrimDDLPartitions(ddl, 5)
	ddl = propertiesBlockRe.ReplaceAllString(ddl, "")
	ddl = string(RemoveBlankLines([]byte(ddl)))
	return strings.TrimSpace(ddl)
}

// ExtractColumnsFromSQL uses the Doris SQL parser to extract column reference
// identifiers from SQL queries. Returns a case-insensitive set (all keys lowercase).
// Returns nil if any query fails to parse, meaning all columns should be kept.
func ExtractColumnsFromSQL(sqls []string) map[string]bool {
	cols := make(map[string]bool)
	for i, sql := range sqls {
		extracted := parser.ExtractColumnReferences(fmt.Sprintf("sql-%d", i), sql)
		if extracted == nil {
			// Parse failed, don't filter any columns
			return nil
		}
		maps.Copy(cols, extracted)
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// TrimStatsForLLM removes stats YAML fields that the LLM never uses for
// data-generation decisions: data_size and method.
func TrimStatsForLLM(statsYAML string) string {
	if statsYAML == "" {
		return statsYAML
	}
	statsYAML = statsDataSizeRe.ReplaceAllString(statsYAML, "")
	statsYAML = statsMethodRe.ReplaceAllString(statsYAML, "")
	statsYAML = string(RemoveBlankLines([]byte(statsYAML)))
	return strings.TrimSpace(statsYAML)
}

// filterStatsColumns filters column-level statistics YAML to only keep entries
// for columns present in keepCols (case-insensitive). If keepCols is nil, the
// stats are returned unchanged.
func filterStatsColumns(statsYAML string, keepCols map[string]bool) string {
	if keepCols == nil || statsYAML == "" {
		return statsYAML
	}

	var stats TableStats
	if err := yaml.Unmarshal([]byte(statsYAML), &stats); err != nil {
		logrus.Warnf("failed to parse stats YAML for column filtering: %v", err)
		return statsYAML
	}

	stats.Columns = lo.Filter(stats.Columns, func(c *ColumnStats, _ int) bool {
		return keepCols[strings.ToLower(c.Name)]
	})

	out, err := yaml.Marshal(&stats)
	if err != nil {
		logrus.Warnf("failed to marshal filtered stats YAML: %v", err)
		return statsYAML
	}
	return strings.TrimSpace(string(out))
}

// extractYAMLFromResponse extracts YAML content from the LLM response
func extractYAMLFromResponse(content string) string {
	content = strings.TrimPrefix(content, LLMYAMLPrefix)
	// Also handle the closing code fence if present
	if idx := strings.LastIndex(content, "\n```"); idx != -1 {
		content = content[:idx]
	}
	return strings.TrimSpace(content)
}

// LLMGendataConfig generates gendata config using multi-turn conversation for validation.
// Validation errors are sent back as user messages for the LLM to fix.
// tables: pre-parsed table information (DB, Name, Type, DDLContent, StatsContent)
// sqls: SQL queries to analyze
func LLMGendataConfig(
	ctx context.Context,
	apiKey, baseURL, model, userPrompt string,
	llmTables []LLMTableInput,
	sqls []string,
	validator func(context.Context, string) error,
) (string, error) {
	// Extract column references from SQL queries to filter DDL columns
	sqlCols := ExtractColumnsFromSQL(sqls)

	// --- Build the initial user message (prompt stuffing) ---
	var tablesBuilder, statsBuilder strings.Builder
	for _, t := range llmTables {
		ddl := TrimDDLForLLM(t.DDLContent)
		// Filter DDL to only keep columns used in queries + structural columns,
		// or keep first 5 + structural columns when no SQL is provided.
		if len(sqlCols) > 0 {
			ddl = parser.FilterDDLColumns(t.Name, ddl, sqlCols)
		} else {
			ddl = parser.FilterDDLColumnsTopN(t.Name, ddl, 5)
		}
		tablesBuilder.WriteString(ddl)
		tablesBuilder.WriteString("\n\n")
		if t.StatsContent != "" {
			stats := TrimStatsForLLM(t.StatsContent)
			if len(sqlCols) > 0 {
				stats = filterStatsColumns(stats, sqlCols)
			}
			statsBuilder.WriteString(stats)
			statsBuilder.WriteString("\n---\n")
		}
	}

	sqlPrompt := "No specific query provided. Generate data for all tables."
	if len(sqls) > 0 {
		sqlPrompt = strings.Join(sqls, "\n\n")
	}
	fullUserPrompt := fmt.Sprintf(`
<tables>
%s
</tables>

<column-stats>
%s
</column-stats>

<queries>
%s
</queries>
`,
		tablesBuilder.String(),
		statsBuilder.String(),
		sqlPrompt,
	)

	if userPrompt != "" {
		fullUserPrompt = fmt.Sprintf("%s\n\n<additional-user-prompt>\n%s\n</additional-user-prompt>\n", fullUserPrompt, userPrompt)
	}
	// --- End of user message construction ---

	// Calculate total DDL size to decide which mode to use
	logrus.Infof("Generating gendata.yaml via LLM model: %s (%d tables, %d sqls, %d prompt bytes)",
		model, len(llmTables), len(sqls), len(prompt.Gendata)+len(fullUserPrompt))

	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(baseURL),
	)

	var (
		reasoningPrinter = color.New(color.FgHiBlack)
		resultPrinter    = color.New(color.FgHiWhite)
		maxIterations    = 10
		// isReasonerModel  = strings.Contains(strings.ToLower(model), "reasoner")
	)

	// Initialize messages
	messages := []openai.ChatCompletionMessageParamUnion{
		openai.SystemMessage(prompt.Gendata),
		openai.UserMessage(fullUserPrompt),
	}
	logrus.Traceln("LLM gendata system prompt:", prompt.Gendata, "\nuser prompt:\n", fullUserPrompt)

	for i := range maxIterations {
		// Build request params (no tools)
		params := openai.ChatCompletionNewParams{
			Model:    model,
			Messages: messages,
		}
		// // DeepSeek reasoner does not support temperature and top_p
		// if !isReasonerModel {
		// 	params.Temperature = openai.Float(0.1)
		// 	params.TopP = openai.Float(0.9)
		// }

		var (
			content          string
			reasoningContent string
		)

		resp := client.Chat.Completions.NewStreaming(ctx, params)
		for resp.Next() {
			data := resp.Current().Choices[0].Delta
			// Handle reasoning content (DeepSeek reasoner specific)
			r := map[string]any{}
			if err := json.Unmarshal([]byte(data.RawJSON()), &r); err == nil {
				var ok bool
				reasoningContent, ok = r["reasoning_content"].(string)
				if !ok {
					reasoningContent = ""
				}
			}
			if reasoningContent != "" {
				reasoningPrinter.Fprint(os.Stderr, reasoningContent)
			} else {
				resultPrinter.Fprint(os.Stdout, data.Content)
				content += data.Content
			}
		}
		_ = resp.Close()

		if err := resp.Err(); err != nil {
			return "", fmt.Errorf("error from llm: %w", err)
		}

		// Extract YAML from response
		yamlContent := extractYAMLFromResponse(content)
		if yamlContent == "" {
			// No YAML found, ask LLM to provide it
			messages = append(messages, openai.AssistantMessage(content))
			messages = append(messages, openai.UserMessage("Please provide the YAML configuration. Output only valid YAML wrapped in ```yaml code blocks."))
			continue
		}

		// Validate the YAML
		if validator == nil {
			// No validator, return as-is
			return yamlContent, nil
		}

		validationErr := validator(ctx, yamlContent)
		if validationErr == nil {
			// Check if validation passed
			return yamlContent, nil
		}

		// Validation failed, send error back to LLM
		logrus.Warnf("Validation failed (#%d): %v", i+1, validationErr)

		// Add assistant response and validation error as new user message
		messages = append(messages, openai.AssistantMessage(content))
		messages = append(messages, openai.UserMessage(fmt.Sprintf(`The YAML configuration has validation errors:

<validation-error>
%v
</validation-error>

Please fix the YAML configuration based on the log/error above and output the corrected YAML. Output only the complete fixed YAML wrapped in `+"```yaml"+` code blocks.`, validationErr)))
	}

	return "", fmt.Errorf("max iterations %d reached without valid YAML", maxIterations)
}

// LLMExtractTablesResult represents the result of LLMExtractTables
type LLMExtractTablesResult struct {
	Tables []string `json:"tables"` // list of "database.table" or "database.view" names
}

// LLMExtractTables uses LLM to extract table/view names from SQL queries.
// Returns a list of "db.table" formatted strings referenced in the queries.
func LLMExtractTables(
	ctx context.Context,
	apiKey, baseURL, model string,
	defaultDB string, sqls []string,
) ([]string, error) {
	defaultDBPrompt := ""
	if defaultDB != "" {
		defaultDBPrompt = fmt.Sprintf("If a table/view name appears without a database prefix and there is no dodo comment specifying a default db, use the '%s' database as the prefix.\n", defaultDB)
	}
	systemPrompt := `You are a SQL analyzer. Your task is to extract all table and view names referenced in SQL queries.

Instructions:
1. Analyze the provided SQL queries carefully
2. Extract ALL table/view names that appear in: FROM clauses, JOIN clauses, subqueries, CTEs (WITH clause), INSERT INTO, UPDATE, DELETE FROM, etc.
3. Return the result in JSON format with a "tables" array containing "database.table" formatted strings
4. **Important**: If a query has a dodo comment prefix like ` + "`/*dodo{\"db\":\"mydb\",...}*/`" + `, use that "db" value as the database prefix for any tables in that query that don't have an explicit database prefix
5. Remove duplicates - each table should appear only once
6. Do NOT include column names, aliases, or other identifiers - only table/view names
7. Always try to output in "database.table" format when possible
` + defaultDBPrompt + `

Example:
- Query: ` + "`/*dodo{\"db\":\"sales\"}*/ SELECT * FROM orders JOIN customers ON ...`" + `
- Since "orders" and "customers" have no db prefix but the dodo comment has db="sales", output: ["sales.orders", "sales.customers"]

Output format:
` + "```json" + `
{"tables": ["db1.table1", "db2.table2"]}
` + "```"

	userPrompt := fmt.Sprintf("<queries>\n%s\n</queries>", strings.Join(sqls, "\n\n"))
	logrus.Traceln("LLM extract tables user prompt:", userPrompt)

	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(baseURL),
	)

	var (
		reasoningPrinter = color.New(color.FgHiBlack)
		resultPrinter    = color.New(color.FgHiWhite)
		result           strings.Builder
		isReasonerModel  = strings.Contains(strings.ToLower(model), "reasoner")
	)

	assistantMsg := openai.AssistantMessage(LLMJSONPrefix)
	assistantMsg.SetExtraFields(map[string]any{"prefix": true})

	messages := []openai.ChatCompletionMessageParamUnion{
		openai.SystemMessage(systemPrompt),
		openai.UserMessage(userPrompt),
	}
	// Prefix fill: add an assistant message to steer the model's output format.
	// DeepSeek reasoner does not support this pattern.
	if !isReasonerModel {
		messages = append(messages, assistantMsg)
	}

	params := openai.ChatCompletionNewParams{
		Model: model,
		Stop: openai.ChatCompletionNewParamsStopUnion{
			OfString: openai.String("\n```"),
		},
		Messages: messages,
	}
	// DeepSeek reasoner does not support temperature and top_p
	if !isReasonerModel {
		params.Temperature = openai.Float(0.1)
		params.TopP = openai.Float(0.9)
	}

	stream := client.Chat.Completions.NewStreaming(ctx, params)
	defer stream.Close()

	for stream.Next() {
		chunk := stream.Current()
		if len(chunk.Choices) == 0 {
			continue
		}

		delta := chunk.Choices[0].Delta

		// Handle reasoning content (DeepSeek reasoner specific)
		r := map[string]any{}
		if err := json.Unmarshal([]byte(delta.RawJSON()), &r); err == nil {
			if reasoningContent, ok := r["reasoning_content"].(string); ok && reasoningContent != "" {
				reasoningPrinter.Fprint(os.Stderr, reasoningContent)
			}
		}

		// Handle regular content
		if delta.Content != "" {
			result.WriteString(delta.Content)
			resultPrinter.Fprint(os.Stdout, delta.Content)
		}
	}

	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("error from llm stream: %w", err)
	}

	// Parse the JSON response
	jsonStr := strings.TrimPrefix(result.String(), LLMJSONPrefix)
	jsonStr = strings.TrimSpace(jsonStr)
	logrus.Debugf("LLM extract tables result: %s", jsonStr)

	var extractResult LLMExtractTablesResult
	if err := json.Unmarshal([]byte(jsonStr), &extractResult); err != nil {
		return nil, fmt.Errorf("failed to parse LLM response as JSON: %w, response: %s", err, jsonStr)
	}

	return extractResult.Tables, nil
}
