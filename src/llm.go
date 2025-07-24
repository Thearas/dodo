package src

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/goccy/go-json"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/sirupsen/logrus"

	"github.com/Thearas/dodo/src/prompt"
)

const (
	LLMOutputPrefix = "```yaml\n"
)

// Use Deepseek by default, but you can use OpenAI by setting the apiKey and baseURL
func LLMGendataConfig(
	ctx context.Context,
	apiKey, baseURL, model, prompt_ string,
	tables, columnStats, sqls []string,
) (string, error) {
	if model == "" {
		model = "deepseek-chat"
	}
	if baseURL == "" {
		if strings.HasPrefix(strings.ToLower(model), "deepseek") {
			baseURL = "https://api.deepseek.com/beta"
		} else {
			baseURL = "https://api.openai.com/v1/"
		}
	}

	userPrompt := fmt.Sprintf(`
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
		strings.Join(tables, "\n"),
		strings.Join(columnStats, "\n---\n"),
		strings.Join(sqls, "\n"))
	if prompt_ != "" {
		userPrompt = fmt.Sprintf(`%s

<additional-user-prompt>
%s
</additional-user-prompt>
`, userPrompt, prompt_)
	}

	logrus.Debugln("LLM user prompt:", userPrompt)

	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(baseURL),
	)
	stop := openai.ChatCompletionNewParamsStopUnion{
		OfString: openai.String("\n```"),
	}
	stop.SetExtraFields(map[string]any{"prefix": true})
	c := client.Chat.Completions.NewStreaming(ctx, openai.ChatCompletionNewParams{
		Model:       model,
		Temperature: openai.Float(0.1),
		TopP:        openai.Float(0.9),
		Stop:        stop,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(prompt.Gendata),
			openai.AssistantMessage(LLMOutputPrefix),
			openai.UserMessage(userPrompt),
		},
	})
	defer c.Close()

	var (
		reasoningPrinter = color.New(color.FgHiBlack)
		resultPrinter    = color.New(color.FgHiWhite)
		result           strings.Builder
	)
	for c.Next() {
		content := c.Current().Choices[0].Delta.Content

		// https://github.com/openai/openai-go?tab=readme-ov-file#undocumented-response-properties
		r := map[string]any{}
		if err := json.Unmarshal([]byte(c.Current().Choices[0].Delta.RawJSON()), &r); err != nil {
			return "", err
		}

		reasoningContent, ok := r["reasoning_content"].(string)
		if ok {
			reasoningPrinter.Fprint(os.Stderr, reasoningContent)
			continue
		}
		resultPrinter.Fprint(os.Stdout, content)
		result.WriteString(content)
	}
	if err := c.Err(); err != nil {
		return "", fmt.Errorf("error from llm: %w", err)
	}

	return strings.TrimPrefix(result.String(), LLMOutputPrefix), nil
}
