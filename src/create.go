package src

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/antlr4-go/antlr/v4"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/Thearas/dodo/src/parser"
)

//nolint:revive
func RunCreateSQL(ctx context.Context, conn *sqlx.DB, db string, sqlFile string, beCount int, dryrun bool) (needDependence string, err error) {
	ddl, err := os.ReadFile(sqlFile)
	if err != nil {
		return "", fmt.Errorf("failed to read SQL file '%s': %v", sqlFile, err)
	}

	p := parser.NewParser(sqlFile, string(ddl), newCreateParserListener(sqlFile, beCount))
	multiStmts, err := p.Parse()
	if err != nil {
		return "", err
	}

	for _, s_ := range multiStmts.AllStatement() {
		// only run create table/view sql
		s, ok := s_.(*parser.StatementBaseAliasContext)
		if !ok {
			continue
		}

		var (
			name       string
			schemaType SchemaType
		)
		if create, ok := s.StatementBase().(*parser.SupportedCreateStatementAliasContext); ok {
			// 1. table or view
			var (
				createTable *parser.CreateTableContext
				createView  *parser.CreateViewContext
			)
			createTable, ok = create.SupportedCreateStatement().(*parser.CreateTableContext)
			if !ok {
				createView, ok = create.SupportedCreateStatement().(*parser.CreateViewContext)
				if !ok {
					continue
				}
			}
			if createTable != nil {
				name = strings.ReplaceAll(createTable.GetName().GetText(), "`", "")
				schemaType = SchemaTypeTable
			} else {
				name = strings.ReplaceAll(createView.GetName().GetText(), "`", "")
				schemaType = SchemaTypeView
			}
		} else if create, ok := s.StatementBase().(*parser.MaterializedViewStatementAliasContext); ok {
			// 2. materialized view
			createMTMV, ok := create.MaterializedViewStatement().(*parser.CreateMTMVContext)
			if !ok {
				continue
			}
			name = strings.ReplaceAll(createMTMV.GetMvName().GetText(), "`", "")
			schemaType = SchemaTypeMaterializedView
		} else {
			continue
		}

		interval := antlr.NewInterval(s.GetStart().GetTokenIndex(), s.GetStop().GetTokenIndex())
		stmt := p.GetTokenStream().GetTextFromInterval(interval)

		logrus.Tracef("creating schema in db %s, sql: %s", db, stmt)
		if dryrun {
			return "", nil
		}
		c, err := conn.Connx(ctx)
		if err != nil {
			return "", err
		}
		defer c.Close()

		// create db if not exists and use it
		if _, err := c.ExecContext(ctx, InternalSqlComment+fmt.Sprintf("USE `%s`", db)); err != nil {
			if !strings.Contains(err.Error(), " Unknown database ") {
				return "", fmt.Errorf("use db '%s' failed: %v", db, err)
			}

			// create db
			logrus.Infof("Create database '%s'", db)
			if _, err := c.ExecContext(ctx, InternalSqlComment+fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db)); err != nil {
				return "", fmt.Errorf("create database '%s' failed: %v", db, err)
			}
			// use again
			if _, err := c.ExecContext(ctx, InternalSqlComment+fmt.Sprintf("USE `%s`", db)); err != nil {
				return "", fmt.Errorf("use db '%s' failed: %v", db, err)
			}
		}

		startedAt := time.Now()
		_, err = c.ExecContext(ctx, InternalSqlComment+stmt)
		duration := time.Since(startedAt)

		if err != nil {
			if strings.Contains(err.Error(), " already exists") {
				logrus.Infof("skip creating %s '%s.%s', already exists", schemaType.Lower(), db, name)
				continue
			} else if strings.Contains(err.Error(), " does not exist") {
				// may deppends on other table/view
				return err.Error(), nil
			}
			return "", fmt.Errorf("create %s '%s.%s' failed: %v", schemaType.Lower(), db, name, err)
		}

		logrus.Infof("%s '%s.%s' created, cost %.2fs", schemaType.Lower(), db, name, duration.Seconds())
	}

	return "", nil
}

// ignoredCreateTableProperties is a set of property keys to remove from CREATE TABLE statements.
var ignoredCreateTableProperties = map[string]struct{}{
	"storage_vault_id": {},
}

type CreateParserListener struct {
	*parser.BaseDorisParserListener

	sqlId             string
	beCount           int
	hasReplicationNum bool
}

func newCreateParserListener(sqlId string, beCount int) parser.DorisParserListener {
	return &CreateParserListener{sqlId: sqlId, beCount: beCount}
}

func (l *CreateParserListener) ExitPropertyItemList(ctx *parser.PropertyItemListContext) {
	if !isCreateTablePropertiesList(ctx) {
		return
	}

	items := ctx.AllPropertyItem()
	commas := ctx.AllCOMMA()

	// Remove ignored property items by blanking their tokens.
	// Note: SetText("") is a no-op in ANTLR (falls back to input stream),
	// so we use a single space " " to effectively blank tokens.
	for i, item := range items {
		key := strings.Trim(item.GetKey().GetText(), `'"`)
		if _, ok := ignoredCreateTableProperties[key]; !ok {
			continue
		}

		// Blank key, '=' and value tokens
		item.GetStart().SetText(" ")
		item.EQ().GetSymbol().SetText(" ")
		item.GetStop().SetText(" ")

		// Blank the adjacent comma
		if i < len(commas) {
			commas[i].GetSymbol().SetText(" ")
		} else if i > 0 {
			commas[i-1].GetSymbol().SetText(" ")
		}
	}

	if l.hasReplicationNum {
		return
	}

	// To make the new property appear in the SQL output via GetTextFromInterval,
	// we need to modify an existing token's text (not just add to parse tree).
	// Append the new property to the last property item's value token.
	if len(items) == 0 {
		return
	}
	lastItem := items[len(items)-1]
	lastToken := lastItem.GetStop()

	replicationNum := max(l.beCount, 1)
	newText := fmt.Sprintf(`%s, "replication_num" = "%d"`, lastToken.GetText(), replicationNum)
	lastToken.SetText(newText)
}

// Modify property value
func (l *CreateParserListener) ExitPropertyItem(ctx *parser.PropertyItemContext) {
	if !l.isCreateTablePropertyItem(ctx) {
		return
	}

	if ctx.GetKey().Constant() == nil {
		return
	}
	key := strings.Trim(ctx.GetKey().GetText(), `'"`)
	if !slices.Contains([]string{"replication_allocation", "replication_num"}, key) {
		return
	}
	l.hasReplicationNum = true
	key = `"replication_num"`
	symbol := ctx.GetKey().Constant().GetChild(0).(*antlr.TerminalNodeImpl).GetSymbol()
	symbol.SetText(key)

	pvalue := ctx.PropertyValue()
	if pvalue.Constant() != nil {
		constant := pvalue.Constant()
		rawText := constant.GetText()

		digit := NumberRe.FindString(rawText)
		if digit == "" {
			return
		}
		replicationNum := min(cast.ToInt(digit), l.beCount)

		symbol := constant.GetChild(0).(*antlr.TerminalNodeImpl).GetSymbol()
		symbol.SetText(fmt.Sprintf(`"%d"`, replicationNum))
	}
}

func (*CreateParserListener) isCreateTablePropertyItem(ctx *parser.PropertyItemContext) bool {
	list, ok := ctx.GetParent().(*parser.PropertyItemListContext)
	if !ok {
		return false
	}

	return isCreateTablePropertiesList(list)
}

func isCreateTablePropertiesList(ctx *parser.PropertyItemListContext) bool {
	clause, ok := ctx.GetParent().(*parser.PropertyClauseContext)
	if !ok {
		return false
	}

	return isCreateTableMainPropertyClause(clause)
}

func isCreateTableMainPropertyClause(clause *parser.PropertyClauseContext) bool {
	createTable, ok := clause.GetParent().(*parser.CreateTableContext)
	if !ok {
		return false
	}

	children := createTable.GetChildren()
	for i, child := range children {
		if child != clause {
			continue
		}

		if i == 0 {
			return true
		}

		term, ok := children[i-1].(antlr.TerminalNode)
		return !ok || !strings.EqualFold(term.GetText(), "BROKER")
	}

	return false
}
