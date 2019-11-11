package reporter

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// TODO:: think about data struct that returns Converter()
type ReporterTemplate struct {
	Query     string
	Converter func(rows *sqlx.Rows) (*[][]interface{}, error)
}

var Templates = map[string]ReporterTemplate{
	"_invoices.purpose": ReporterTemplate{
		Query: `SELECT DISTINCT
				(i."payDate" AT TIME ZONE '+3') AS "payDate",
				COALESCE(i."costWithDiscount", i."cost") AS "cost",
				purpose."name" AS "fund"
			FROM invoice AS i
			LEFT JOIN "invoiceTemplate" AS it ON i."template" = it.id
			LEFT JOIN purpose ON it.purpose = purpose.id
			WHERE
				i."payDate" IS NOT NULL
				AND (i."payDate" AT TIME ZONE '+3' >= '%begin%')
				AND (i."payDate" AT TIME ZONE '+3' < '%end%')
			ORDER BY (i."payDate" AT TIME ZONE '+3') DESC`,
		Converter: func(rows *sqlx.Rows) (*[][]interface{}, error) {
			var err error

			convRows := make([][]interface{}, 1)
			convRows[0] = []interface{}{"Дата оплаты"}
			// temporary map for saving indexes of column headers
			cols := make(map[string]int)

			for rows.Next() {
				sqlRow := make(map[string]interface{})
				err = rows.MapScan(sqlRow)
				if err != nil {
					return nil, err
				}

				row := make([]interface{}, len(convRows[0]))
				row[0] = sqlRow["payDate"].(time.Time)
				if col, ok := cols[sqlRow["fund"].(string)]; ok {
					row[col] = sqlRow["cost"].([]byte)
				} else {
					// if column doesn't exists then adding it to table & tmp map
					convRows[0] = append(convRows[0], sqlRow["fund"].(string))
					i := len(convRows[0]) - 1
					cols[sqlRow["fund"].(string)] = i

					// memory allocation for new column
					row1 := make([]interface{}, i+1)
					copy(row1, row)
					row = row1

					row[i] = sqlRow["cost"].([]byte)
				}

				convRows = append(convRows, row)
			}

			return &convRows, nil
		},
	},
}
