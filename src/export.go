package src

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	gen "github.com/Thearas/dodo/src/generator"
)

const (
	ExportLabelPrefix       = "dodo_export_"
	DefaultExportTimeoutSec = 7200
)

func Export(
	ctx context.Context,
	conn *sqlx.DB,
	dbname, table, target, toURL string,
	with, props map[string]string,
) error {
	label := props["label"]
	if label == "" {
		// label format: dodo_export_<dbname>_<table>_<random(3)>
		label = fmt.Sprintf("%s%s_%s_%s", ExportLabelPrefix, dbname, table, gen.RandomStr(3, 3))
	}

	timeout := cast.ToInt(props["timeout"])
	if timeout == 0 {
		timeout = DefaultExportTimeoutSec
	}

	colSep := props["column_separator"]
	if colSep == "" {
		colSep = string(ColumnSeparator)
	}

	// set default properties
	props["label"] = label
	props["timeout"] = strconv.Itoa(timeout)
	props["column_separator"] = colSep

	// execute EXPORT statement
	if err := exportTable(ctx, conn, dbname, table, target, toURL, with, props); err != nil {
		return err
	}

	// wait for export to complete
	var (
		now     = time.Now()
		waitSec = 5
	)
	for int(time.Since(now).Seconds()) <= timeout+waitSec {
		select {
		case <-ctx.Done():
			// cancel export job
			err := cancelExportTable(ctx, conn, dbname, label)
			return errors.Join(ctx.Err(), err)
		case <-time.After(time.Duration(waitSec) * time.Second):
			// continue
		}

		completed, progress, err := showExportTable(ctx, conn, dbname, label)
		if err != nil {
			return fmt.Errorf("show export failed: %w", err)
		}
		if completed {
			return nil
		}
		logrus.Debugf("Exporting table '%s.%s', progress: %s\n", dbname, table, progress)
	}

	return fmt.Errorf("export table '%s.%s' timed out after %d seconds", dbname, table, timeout)
}
