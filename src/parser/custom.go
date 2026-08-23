package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var (
	// ErrNotCreateTable is returned when the SQL statement is not a CREATE TABLE statement (e.g., CREATE VIEW).
	ErrNotCreateTable = errors.New("not a valid CREATE TABLE statement")

	// The properties which value contains identifier.
	propertiesWithValueIds = lo.SliceToMap([]string{
		"bloom_filter_columns",
		"function_column.sequence_col",
	}, func(s string) (string, struct{}) { return s, struct{}{} })
)

func NewListener(hideSqlComment bool, modifyIdentifier func(id string) string) DorisParserListener {
	return &listener{hideSQLComment: hideSqlComment, modifyIdentifier: modifyIdentifier}
}

func NewErrListener(sqlId string) *ErrListener {
	return &ErrListener{ConsoleErrorListener: antlr.NewConsoleErrorListener(), sqlId: sqlId}
}

func NewErrHandler() antlr.ErrorStrategy {
	return &errHandler{DefaultErrorStrategy: antlr.NewDefaultErrorStrategy()}
}

func NewParser(sqlId string, sqls string, listeners ...antlr.ParseTreeListener) *Parser {
	input := antlr.NewInputStream(sqls)
	lexer := NewDorisLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := NewDorisParser(stream)

	errListener := NewErrListener(sqlId)
	p.RemoveErrorListeners()
	p.AddErrorListener(errListener)

	for _, listener := range listeners {
		p.AddParseListener(listener)
	}
	if len(listeners) > 0 {
		p.SetErrorHandler(NewErrHandler())
	}

	return &Parser{DorisParser: p, ErrListener: errListener}
}

type errHandler struct {
	*antlr.DefaultErrorStrategy
}

func (h *errHandler) ReportMatch(p antlr.Parser) {
	h.DefaultErrorStrategy.ReportMatch(p)

	// Do not modify ENGINE name.
	tokenType := p.GetCurrentToken().GetTokenType()
	switch tokenType {
	case DorisParserENGINE:
		for _, l := range p.GetParseListeners() {
			if l, ok := l.(*listener); ok {
				l.ignoreCurrentIdentifier = true
			}
		}
	case DorisParserCOMMENT:
		// hide next string literal, e.g. COMMENT '***'
		for _, l := range p.GetParseListeners() {
			if l, ok := l.(*listener); ok && l.hideSQLComment {
				l.hideNextString = true
			}
		}
	case DorisParserSTRING_LITERAL:
		for _, l := range p.GetParseListeners() {
			if l, ok := l.(*listener); ok && l.hideNextString {
				l.hideNextString = false
				hideComment(p.GetCurrentToken())
			}
		}
	default:
	}
}

type ErrListener struct {
	*antlr.ConsoleErrorListener
	sqlId   string
	LastErr error
}

func (l *ErrListener) SyntaxError(_ antlr.Recognizer, _ any, line, column int, message string, _ antlr.RecognitionException) {
	// remove string after 'expecting', it's too annoying
	msg := strings.Split(message, "expecting")[0]
	l.LastErr = errors.New(msg)
	logrus.Errorf("sql %s parse error at line %d:%d %s", l.sqlId, line, column, msg)
}

type listener struct {
	*BaseDorisParserListener

	hideSQLComment   bool
	modifyIdentifier func(id string) string

	// state variables
	ignoreCurrentIdentifier bool
	hideNextString          bool
	lastModifiedIdentifier  string
}

// Do not modify variable name.
func (l *listener) ExitUserVariable(ctx *UserVariableContext) {
	if ctx.IdentifierOrText().Identifier() == nil {
		return
	}

	childern := ctx.GetChildren()
	id, ok := childern[len(childern)-1].GetChild(0).GetChild(0).GetChild(0).(*antlr.TerminalNodeImpl)
	if !ok {
		return
	}
	l.recoverSymbolText(id)
}

// Do not modify variable name.
func (l *listener) ExitSystemVariable(ctx *SystemVariableContext) {
	childern := ctx.GetChildren()
	id, ok := childern[len(childern)-1].GetChild(0).GetChild(0).(*antlr.TerminalNodeImpl)
	if !ok {
		return
	}
	l.recoverSymbolText(id)
}

// Do not modify function name.
func (l *listener) ExitFunctionNameIdentifier(ctx *FunctionNameIdentifierContext) {
	if ctx.Identifier() == nil {
		return
	}

	id, ok := ctx.GetChild(0).GetChild(0).GetChild(0).(*antlr.TerminalNodeImpl)
	if !ok {
		// maybe a non-reserved id, need one more GetChild(0)
		id, ok = ctx.GetChild(0).GetChild(0).GetChild(0).GetChild(0).(*antlr.TerminalNodeImpl)
		if !ok {
			panic("unreachable: can not find function name")
		}
	}
	l.recoverSymbolText(id)
}

// Modify id.
func (l *listener) ExitUnquotedIdentifier(ctx *UnquotedIdentifierContext) {
	child := ctx.GetChild(0)
	_, ok := child.(*NonReservedContext)
	if ok {
		child = child.GetChild(0)
	}
	l.modifySymbolText(child.(*antlr.TerminalNodeImpl))
}

// Modify `id`.
func (l *listener) ExitQuotedIdentifier(ctx *QuotedIdentifierContext) {
	child := ctx.GetChild(0)
	l.modifySymbolText(child.(*antlr.TerminalNodeImpl))
}

// Modify property value
func (l *listener) ExitPropertyItem(ctx *PropertyItemContext) {
	// e.g. "bloom_filter_columns" = "col1,col2"
	key := strings.Trim(ctx.GetKey().GetText(), `'"`)
	if _, ok := propertiesWithValueIds[key]; !ok {
		return
	}

	pvalue := ctx.PropertyValue()
	if pvalue.Constant() != nil {
		constant := pvalue.Constant()
		rawText := constant.GetText()
		quote := rawText[0]

		ids := strings.Split(rawText[1:len(rawText)-1], ",")
		for i, id := range ids {
			ids[i] = l.modifyIdentifier(strings.Trim(strings.TrimSpace(id), "`"))
		}

		symbol := constant.GetChild(0).(*antlr.TerminalNodeImpl).GetSymbol()
		symbol.SetText(fmt.Sprintf("%c%s%c", quote, strings.Join(ids, ","), quote))
	}
}

func (l *listener) modifySymbolText(node antlr.TerminalNode) {
	symbol := node.GetSymbol()
	text := symbol.GetText()

	if l.ignoreCurrentIdentifier {
		l.ignoreCurrentIdentifier = false
	} else {
		id := strings.Trim(text, "`")
		symbol.SetText(l.modifyIdentifier(id))
	}

	// record original identifier text
	l.lastModifiedIdentifier = text
}

func (l *listener) recoverSymbolText(node antlr.TerminalNode) {
	if l.lastModifiedIdentifier != "" {
		node.GetSymbol().SetText(l.lastModifiedIdentifier)
		l.lastModifiedIdentifier = ""
	}
}

func hideComment(comment antlr.Token) {
	if comment == nil {
		return
	}
	text := comment.GetText()
	if len(text) <= len(`''`) {
		// empty comment
		return
	}

	newText := fmt.Sprintf(`'%s'`, strings.Repeat("*", len(text)-len(`''`)))
	comment.SetText(newText)
}

type Parser struct {
	*DorisParser
	ErrListener *ErrListener
}

func (p *Parser) Parse() (IMultiStatementsContext, error) {
	// parser and modify
	ms := p.MultiStatements()
	return ms, p.ErrListener.LastErr
}

func (p *Parser) ToSQL() (string, error) {
	// parser and modify
	ms, err := p.Parse()
	if err != nil {
		return "", err
	}

	// get modified sql
	interval := antlr.NewInterval(ms.GetStart().GetTokenIndex(), ms.GetStop().GetTokenIndex())
	s := p.GetTokenStream().GetTextFromInterval(interval)

	return s, nil
}

func GetTableNameAndCols(sqlId, createTableStmt string) (string, []string, error) {
	p := NewParser(sqlId, createTableStmt)
	c, ok := p.SupportedCreateStatement().(*CreateTableContext)
	if !ok {
		if p.ErrListener.LastErr != nil {
			return "", nil, fmt.Errorf("SQL parser error for '%s': %w", sqlId, p.ErrListener.LastErr)
		}
		return "", nil, fmt.Errorf("%w: %s", ErrNotCreateTable, sqlId)
	} else if p.ErrListener.LastErr != nil {
		return "", nil, p.ErrListener.LastErr
	}
	ns := strings.Split(SqlIdentifier(c.GetName().GetText()), ".")
	tableName := ns[len(ns)-1]

	return tableName, lo.Map(c.ColumnDefs().GetCols(), func(col IColumnDefContext, _ int) string { return strings.Trim(col.GetColName().GetText(), "`") }), nil
}

// GetTableName extracts db and table name from CREATE TABLE statement.
// Returns (db, table, error). db may be empty if not specified in the statement.
func GetTableName(sqlId, createTableStmt string) (string, string, error) {
	p := NewParser(sqlId, createTableStmt)
	c, ok := p.SupportedCreateStatement().(*CreateTableContext)
	if !ok {
		return "", "", errors.New("not a valid CREATE TABLE statement")
	} else if p.ErrListener.LastErr != nil {
		return "", "", p.ErrListener.LastErr
	}
	ns := strings.Split(SqlIdentifier(c.GetName().GetText()), ".")
	if len(ns) >= 2 {
		return ns[len(ns)-2], ns[len(ns)-1], nil
	}
	return "", ns[len(ns)-1], nil
}

// return empty if type is unsupport.
func GetBaseType(type_ IDataTypeContext) (t string) {
	switch ty := type_.(type) {
	case *ComplexDataTypeContext:
		t = ty.GetComplex_().GetText()
	case *VariantPredefinedFieldsContext:
		t = ty.GetComplex_().GetText()
		t = strings.Split(t, "<")[0]
	case *AggStateDataTypeContext:
		t = "AGG_STATE"
	case *PrimitiveDataTypeContext:
		t = ty.PrimitiveColType().GetType_().GetText()
	default:
	}
	return strings.ToUpper(strings.TrimSpace(t))
}

func SqlIdentifier(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "`", ""), " ", "")
}

// GetPartitionColumns extracts partition column names from a CreateTableContext.
// For function-based partitions like `date_trunc(col, 'day')`, extracts the first argument as the column name.
// Returns nil if the table has no PARTITION BY clause.
func GetPartitionColumns(c *CreateTableContext) []string {
	pt := c.GetPartition()
	if pt == nil {
		return nil
	}

	partList := pt.GetPartitionList()
	if partList == nil {
		return nil
	}

	partitions := partList.AllIdentityOrFunction()
	if len(partitions) == 0 {
		return nil
	}

	cols := make([]string, 0, len(partitions))
	for _, p := range partitions {
		if id := p.Identifier(); id != nil {
			// Plain column: PARTITION BY RANGE(col)
			cols = append(cols, SqlIdentifier(id.GetText()))
		} else if fn := p.FunctionCallExpression(); fn != nil {
			// Function-based: AUTO PARTITION BY RANGE(date_trunc(col, 'day'))
			// Extract column name from the first argument
			args := fn.GetArguments()
			for _, arg := range args {
				text := SqlIdentifier(arg.GetText())
				// Skip string literals (like 'day', 'month') - they start with quotes
				if !strings.HasPrefix(arg.GetText(), "'") && !strings.HasPrefix(arg.GetText(), "\"") {
					cols = append(cols, text)
					break
				}
			}
		}
	}

	return cols
}

// GetUniqueKeyColumns extracts key column names from a CreateTableContext
// if the table uses UNIQUE KEY model. Returns nil for AGGREGATE/DUPLICATE KEY models
// or tables without key specification.
func GetUniqueKeyColumns(c *CreateTableContext) []string {
	if c.UNIQUE() == nil || c.GetKeys() == nil {
		return nil
	}

	idents := c.GetKeys().IdentifierSeq().GetIdent()
	cols := make([]string, 0, len(idents))
	for _, id := range idents {
		cols = append(cols, SqlIdentifier(id.GetText()))
	}
	return cols
}

// GetAllStructuralColumns returns a case-insensitive set (lowercase keys) of
// all columns that appear in structural DDL clauses: KEY (UNIQUE/AGGREGATE/
// DUPLICATE), CLUSTER KEY, PARTITION BY, and DISTRIBUTED BY HASH.
// These columns are always considered "important" regardless of query usage.
func GetAllStructuralColumns(c *CreateTableContext) map[string]bool {
	cols := make(map[string]bool)

	// KEY columns (UNIQUE/AGGREGATE/DUPLICATE)
	if c.GetKeys() != nil {
		for _, id := range c.GetKeys().IdentifierSeq().GetIdent() {
			cols[strings.ToLower(SqlIdentifier(id.GetText()))] = true
		}
	}

	// CLUSTER KEY columns
	if c.GetClusterKeys() != nil {
		for _, id := range c.GetClusterKeys().IdentifierSeq().GetIdent() {
			cols[strings.ToLower(SqlIdentifier(id.GetText()))] = true
		}
	}

	// PARTITION BY columns
	for _, col := range GetPartitionColumns(c) {
		cols[strings.ToLower(col)] = true
	}

	// DISTRIBUTED BY HASH columns
	if c.GetHashKeys() != nil {
		for _, id := range c.GetHashKeys().IdentifierSeq().GetIdent() {
			cols[strings.ToLower(SqlIdentifier(id.GetText()))] = true
		}
	}

	return cols
}

// columnExtractListener walks a parse tree and collects column reference identifiers.
type columnExtractListener struct {
	*BaseDorisParserListener
	columns map[string]bool
}

// ExitColumnReference captures bare column references (e.g. `status`, `price`).
func (l *columnExtractListener) ExitColumnReference(ctx *ColumnReferenceContext) {
	if id := ctx.Identifier(); id != nil {
		col := SqlIdentifier(id.GetText())
		l.columns[strings.ToLower(col)] = true
	}
}

// ExitDereference captures the field part of qualified column references (e.g. `t.col` → `col`).
func (l *columnExtractListener) ExitDereference(ctx *DereferenceContext) {
	if fieldName := ctx.GetFieldName(); fieldName != nil {
		col := SqlIdentifier(fieldName.GetText())
		l.columns[strings.ToLower(col)] = true
	}
}

// ExtractColumnReferences parses SQL statement(s) and extracts all column
// reference identifiers from expressions. This includes bare column names
// (from ColumnReference) and qualified column names (field part of Dereference).
// Returns a case-insensitive set (all keys are lowercase).
// Returns nil if parsing fails, meaning "all columns should be considered used".
func ExtractColumnReferences(sqlId string, sqls string) map[string]bool {
	p := NewParser(sqlId, sqls)
	tree, err := p.Parse()
	if err != nil {
		logrus.Warnf("failed to parse SQL %s for column extraction: %v", sqlId, err)
		return nil
	}

	listener := &columnExtractListener{columns: make(map[string]bool)}
	antlr.ParseTreeWalkerDefault.Walk(listener, tree)
	return listener.columns
}

// FilterDDLColumnsTopN rewrites a CREATE TABLE DDL to keep only the first
// topN columns plus all structural columns (KEY, PARTITION, DISTRIBUTED BY
// HASH, CLUSTER KEY). This is useful when no SQL queries are provided and we
// want to reduce DDL size for LLM token limits.
func FilterDDLColumnsTopN(sqlId, ddl string, topN int) string {
	if topN <= 0 {
		topN = 10
	}

	p := NewParser(sqlId, ddl)
	c, ok := p.SupportedCreateStatement().(*CreateTableContext)
	if !ok || p.ErrListener.LastErr != nil {
		return ddl
	}

	colDefs := c.ColumnDefs()
	if colDefs == nil {
		return ddl
	}
	allCols := colDefs.GetCols()
	if len(allCols) <= topN {
		return ddl
	}

	structural := GetAllStructuralColumns(c)

	// Build keepCols from first topN columns + structural columns
	keepCols := make(map[string]bool, topN+len(structural))
	for i, col := range allCols {
		if i < topN {
			keepCols[strings.ToLower(SqlIdentifier(col.GetColName().GetText()))] = true
		}
	}
	for col := range structural {
		keepCols[col] = true
	}

	return FilterDDLColumns(sqlId, ddl, keepCols)
}

// FilterDDLColumns rewrites a CREATE TABLE DDL to keep only the columns in
// keepCols (case-insensitive) plus all structural columns (KEY, PARTITION,
// DISTRIBUTED BY HASH, CLUSTER KEY). If keepCols is nil (meaning we couldn't
// parse the SQL), the DDL is returned unmodified. Omitted columns are replaced
// by a single summary comment line.
func FilterDDLColumns(sqlId, ddl string, keepCols map[string]bool) string {
	if keepCols == nil {
		return ddl
	}

	p := NewParser(sqlId, ddl)
	c, ok := p.SupportedCreateStatement().(*CreateTableContext)
	if !ok || p.ErrListener.LastErr != nil {
		return ddl // can't parse, return as-is
	}

	colDefs := c.ColumnDefs()
	if colDefs == nil {
		return ddl
	}
	allCols := colDefs.GetCols()
	if len(allCols) == 0 {
		return ddl
	}

	// Merge structural columns (always kept)
	structural := GetAllStructuralColumns(c)

	// Determine which column defs to keep
	type colRange struct {
		keep bool
	}
	colRanges := make([]colRange, len(allCols))
	var omitted int
	for i, col := range allCols {
		name := strings.ToLower(SqlIdentifier(col.GetColName().GetText()))
		keep := structural[name] || keepCols[name]
		colRanges[i] = colRange{keep: keep}
		if !keep {
			omitted++
		}
	}

	if omitted == 0 {
		return ddl
	}

	// ANTLR token positions (GetStart/GetStop) are rune indices because
	// InputStream internally stores []rune.  Go string slicing is byte-based,
	// so we must convert to []rune for correct slicing when multi-byte
	// characters (e.g. Chinese in COMMENTs) are present.
	runes := []rune(ddl)

	// prefix = everything up to the first column's first character
	firstCharIdx := allCols[0].GetStart().GetStart()
	prefix := string(runes[:firstCharIdx])

	// suffix = everything after the last column's last character
	lastCharIdx := allCols[len(allCols)-1].GetStop().GetStop()
	suffix := string(runes[lastCharIdx+1:])

	// Detect column indentation from whitespace after last newline in prefix
	indent := "  "
	if idx := strings.LastIndex(prefix, "\n"); idx >= 0 {
		indent = prefix[idx+1:]
	}

	// Extract text for each kept column from the original DDL
	keptTexts := make([]string, 0, len(allCols)-omitted+1)
	for i, col := range allCols {
		if !colRanges[i].keep {
			continue
		}
		start := col.GetStart().GetStart()
		stop := col.GetStop().GetStop() + 1
		keptTexts = append(keptTexts, strings.TrimSpace(string(runes[start:stop])))
	}

	// Append omitted summary as a comment
	keptTexts = append(keptTexts, fmt.Sprintf("-- ... %d more columns omitted ...", omitted))

	// Join columns with comma+newline+indent, then reconstruct
	colSection := strings.Join(keptTexts, ",\n"+indent)

	// Clean up suffix: trim leading commas/whitespace before closing paren
	trimmed := strings.TrimLeft(suffix, ", \t\n\r")
	return prefix + colSection + "\n" + trimmed
}
