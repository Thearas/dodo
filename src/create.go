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

	p := parser.NewParser(sqlFile, string(ddl), newCreateParserListener(beCount))
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

		if db != "" {
			if _, err := c.ExecContext(ctx, InternalSqlComment+fmt.Sprintf("USE `%s`", db)); err != nil {
				c.Close()
				return "", fmt.Errorf("use db '%s' failed: %v", db, err)
			}
		}
		startedAt := time.Now()
		_, err = c.ExecContext(ctx, InternalSqlComment+stmt)
		duration := time.Since(startedAt)
		c.Close()

		if err != nil {
			if strings.Contains(err.Error(), " already exists") {
				logrus.Infof("skip creating %s '%s.%s', already exists", schemaType.Lower(), db, name)
				continue
			} else if strings.Contains(err.Error(), " does not exist") {
				// may deppends on other table/view
				return err.Error(), nil
			}
			return "", err
		}

		logrus.Infof("%s '%s.%s' created, cost %.2fs", schemaType.Lower(), db, name, duration.Seconds())
	}

	return "", nil
}

type CreateParserListener struct {
	*parser.BaseDorisParserListener

	beCount int
}

func newCreateParserListener(beCount int) parser.DorisParserListener {
	return &CreateParserListener{beCount: beCount}
}

// Modify property value
func (l *CreateParserListener) ExitPropertyItem(ctx *parser.PropertyItemContext) {
	if ctx.GetKey().Constant() == nil {
		return
	}
	key := strings.Trim(ctx.GetKey().GetText(), `'"`)
	if !slices.Contains([]string{"replication_allocation", "replication_num"}, key) {
		return
	}
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
		replicationNum := cast.ToInt(digit)
		if replicationNum > l.beCount {
			replicationNum = l.beCount
		}

		symbol := constant.GetChild(0).(*antlr.TerminalNodeImpl).GetSymbol()
		symbol.SetText(fmt.Sprintf(`"%d"`, replicationNum))
	}
}
