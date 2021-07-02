package reporter

import (
	"bytes"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/tealeg/xlsx"
)

func GenerateXLSXFromRows(rows *sqlx.Rows, buf *bytes.Buffer, args ...[]string) error {
	var err error

	// Get column names from query result
	colNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error fetching column names, %s\n", err)
	}

	// CRUTCH:: Temporary solution, need rework
	if len(args) > 0 {
		colNames = args[0]
	}

	// Create output xlsx workbook
	xfile := xlsx.NewFile()
	xsheet, err := xfile.AddSheet("Sheet1")
	if err != nil {
		return fmt.Errorf("error adding sheet to xlsx file, %s\n", err)
	}

	// Write Headers to 1st row
	xrow := xsheet.AddRow()
	xrow.WriteSlice(&colNames, -1)

	// Process sql rows
	for rows.Next() {
		// Scan the sql rows into the interface{} slice
		container, err := rows.SliceScan()
		if err != nil {
			return fmt.Errorf("error scanning sql row, %s\n", err)
		}

		xrow = xsheet.AddRow()

		// Here we range over our container and look at each column
		// and set some different options depending on the column type.
		for _, v := range container {
			xcell := xrow.AddCell()
			switch v := v.(type) {
			case string:
				xcell.SetString(v)
			case []byte:
				xcell.SetString(string(v))
			case int64:
				xcell.SetInt64(v)
			case float64:
				xcell.SetFloat(v)
			case bool:
				xcell.SetBool(v)
			case time.Time:
				xcell.SetDateTime(v)
			default:
				xcell.SetValue(v)
			}
		}
	}

	// Save the excel file to the provided output file
	err = xfile.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

// GenerateXLSXFromTable creates XLSX file from two-dimensional array
// zero row of array (`table[0]`) contains column headings,
// the remaining rows contains data
func GenerateXLSXFromTable(table *[][]interface{}, buf *bytes.Buffer) error {
	var err error

	// Create output xlsx workbook
	xfile := xlsx.NewFile()
	xsheet, err := xfile.AddSheet("Sheet1")
	if err != nil {
		return fmt.Errorf("error adding sheet to xlsx file, %s\n", err)
	}

	for _, r := range *table {
		xrow := xsheet.AddRow()
		for _, c := range r {
			xcell := xrow.AddCell()
			switch v := c.(type) {
			case string:
				xcell.SetString(v)
			case []byte:
				xcell.SetString(string(v))
			case int64:
				xcell.SetInt64(v)
			case float64:
				xcell.SetFloat(v)
			case bool:
				xcell.SetBool(v)
			case time.Time:
				xcell.SetDateTime(v)
			default:
				xcell.SetValue(v)
			}
		}
	}

	// Save the excel file to the provided output file
	err = xfile.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

// GenerateXLSXFromMap
func GenerateXLSXFromMap(table []map[string]interface{}, buf *bytes.Buffer) error {
	var err error

	// Create output xlsx workbook
	xfile := xlsx.NewFile()
	xsheet, err := xfile.AddSheet("Sheet1")
	if err != nil {
		return fmt.Errorf("error adding sheet to xlsx file, %s\n", err)
	}

	var keys []string

	//generate header from keys and save keys
	xrow := xsheet.AddRow()
	for k, _ := range table[0] {
		xcell := xrow.AddCell()
		xcell.SetString(k)
		keys = append(keys, k)
	}

	// add rows
	for i := 0; i < len(table); i++ {
		xrow := xsheet.AddRow()
		for _, k := range keys {
			xcell := xrow.AddCell()
			switch v := table[i][k].(type) {
			case string:
				xcell.SetString(v)
			case []byte:
				xcell.SetString(string(v))
			case int64:
				xcell.SetInt64(v)
			case float64:
				xcell.SetFloat(v)
			case bool:
				xcell.SetBool(v)
			case time.Time:
				xcell.SetDateTime(v)
			case *string:
				if v != nil {
					xcell.SetString(*v)
				} else {
					xcell.SetString("")
				}
			case *float64:
				if v != nil {
					xcell.SetFloat(*v)
				} else {
					xcell.SetString("")
				}
			case *time.Time:
				if v != nil {
					xcell.SetDateTime(*v)
				} else {
					xcell.SetString("")
				}
			case *int:
				if v != nil {
					xcell.SetInt(*v)
				} else {
					xcell.SetString("")
				}
			default:
				// todo: mybe add more types ?
				xcell.SetString(fmt.Sprintf("%v", v))
			}
		}
	}

	// Save the excel file to the provided output file
	err = xfile.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
