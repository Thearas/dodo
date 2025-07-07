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
	logrus.Errorf("sql %s parse error at line %d:%d %s\n", l.sqlId, line, column, msg)
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
		return
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

	newText := fmt.Sprintf(`'%s'`, strings.Repeat("*", len(text)))
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

func GetTableCols(sqlId, createTableStmt string) ([]string, error) {
	p := NewParser(sqlId, createTableStmt)
	c, ok := p.SupportedCreateStatement().(*CreateTableContext)
	if !ok {
		logrus.Fatalln("SQL parser error")
	} else if p.ErrListener.LastErr != nil {
		return nil, p.ErrListener.LastErr
	}

	return lo.Map(c.ColumnDefs().GetCols(), func(col IColumnDefContext, _ int) string { return strings.Trim(col.GetColName().GetText(), "`") }), nil
}
