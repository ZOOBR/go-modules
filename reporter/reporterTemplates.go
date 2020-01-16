package reporter

import (
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	dbc "gitlab.com/battler/modules/sql"
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
				if sqlRow["fund"] == nil {
					continue
				}
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
	"_rent.errors.forced": ReporterTemplate{
		Query: `select r2.time as "Дата", r2.errors as "Коды ошибок", object."regNumber" as "Рег.номер", "user".username as "Инициатор"
		from "rentEventError" r2
		left join rent on r2.rent = rent.id
		left join object on rent."object" = object.id
		left join "rentEvent" re on rent.id = re.rent and re.state = r2.state
		left join "user" on "user".id = re.initiator
		where re.initiator is not null AND r2."time" >= '%begin%' AND r2."time" <= '%end%'
		order by r2."time" desc`,
		Converter: func(rows *sqlx.Rows) (*[][]interface{}, error) {
			errors := []struct {
				Code        string
				Description string
			}{}

			err := dbc.DB.Select(&errors, "select * FROM error")
			if err != nil {
				return nil, err
			}
			errorsMap := make(map[string]string)
			for _, e := range errors {
				errorsMap[e.Code] = e.Description
			}

			convRows := make([][]interface{}, 1)
			convRows[0] = []interface{}{"Дата", "Коды ошибок", "Описание", "Рег.номер", "Инициатор"}
			for rows.Next() {
				row := make(map[string]interface{})
				err = rows.MapScan(row)
				if err != nil {
					return nil, err
				}
				errorCodes := row["Коды ошибок"].(string)
				errorCodesArr := strings.Split(errorCodes, ",")
				description := ""
				for _, c := range errorCodesArr {
					d, ok := errorsMap[c]
					if ok {
						if description != "" {
							description += ","
						}
						description += d
					}
				}
				newRow := []interface{}{row["Дата"], row["Коды ошибок"], description, row["Рег.номер"], row["Инициатор"]}
				convRows = append(convRows, newRow)
			}

			return &convRows, nil
		},
	},
}
