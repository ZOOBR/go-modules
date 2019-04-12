package sql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/golog"
	_ "github.com/lib/pq"
)

const (
	//DefaultURI default SQL connection string
	DefaultURI = "user=postgres password=postgres dbname=csx sslmode=disable"
)

//DB is pointer to DB connection
var (
	DB *sqlx.DB
	Q  Query
)

type QueryParams struct {
	BaseTable string
	Select    *[]string
	From      *string
	Where     *[]string
	Order     *[]string
}

type QueryStringParams struct {
	Select *string
	From   *string
	Where  *string
	Order  *string
}

type QueryResult struct {
	Result []map[string]interface{}
	Error  error
}

type Query struct {
	tx *sqlx.Tx
	db *sqlx.DB
}

func NewQuery(tx bool) (q Query, err error) {
	q = Query{db: DB}
	if tx {
		q.tx, err = DB.Beginx()
	}
	return q, err
}

func (this *Query) Commit() (err error) {
	return this.tx.Commit()
}

func (this *Query) Rollback() (err error) {
	return this.tx.Rollback()
}

var (
	baseQuery = `SELECT {{.Select}} FROM {{.From}} {{.Where}} {{.Order}}`
)

// func (s *QueryResult) MarshalJSON() ([]byte, error) {

// }

func prepareFields(fields *[]string) string {
	resStr := strings.Join(*fields, ",")
	return resStr
}

func prepareBaseFields(baseTable string, fields *[]string) string {
	var res string
	for index := 0; index < len(*fields); index++ {
		res += `"` + baseTable + `".` + (*fields)[index]
		if index != len(*fields)-1 {
			res += ","
		}
	}
	return res
}

func MakeQuery(params *QueryParams) (*string, error) {
	query := baseQuery
	fields := "*"
	var from, where, order string
	if params.BaseTable != "" {
		fields = prepareBaseFields(params.BaseTable, params.Select)
	} else if params.Select != nil {
		fields = prepareFields(params.Select)
	}
	if params.From != nil {
		from = *params.From
	}
	if params.Where != nil {
		where = " WHERE " + prepareFields(params.Where)
	}
	if params.Order != nil {
		order = " ORDER BY " + prepareFields(params.Order)
	}
	pStruct := QueryStringParams{&fields, &from, &where, &order}
	template := template.Must(template.New("").Parse(query))
	var tpl bytes.Buffer
	err := template.Execute(&tpl, pStruct)
	if err != nil {
		return nil, err
	}
	query = tpl.String()
	return &query, nil
}

func execQuery(q *string) QueryResult {
	results := QueryResult{}
	rows, err := DB.Queryx(*q)
	if err != nil {
		return QueryResult{Error: err}
	}
	for rows.Next() {
		row := make(map[string]interface{})
		err = rows.MapScan(row)
		for k, v := range row {
			switch v.(type) {
			case []byte:
				var jsonMap map[string]*json.RawMessage
				err := json.Unmarshal(v.([]byte), &jsonMap)
				if err != nil {
					row[k] = string(v.([]byte))
				} else {
					row[k] = jsonMap
				}
			}
		}
		results.Result = append(results.Result, row)
	}
	return results
}

func Find(params *QueryParams) QueryResult {
	query, err := MakeQuery(params)
	// golog.Debug(*query)
	if err != nil {
		return QueryResult{Error: err}
	}
	return execQuery(query)
}

func FindOne(params *QueryParams) (*map[string]interface{}, error) {
	query, err := MakeQuery(params)
	if err != nil {
		return nil, err
	}
	data := execQuery(query)
	if data.Error != nil {
		return nil, data.Error
	}
	if data.Result != nil && len(data.Result) > 0 {
		return &data.Result[0], nil
	}
	return nil, nil
}

func GetCollection(tableName *string, params ...[]string) QueryResult {

	qparams := &QueryParams{
		From: tableName}

	if len(params) > 0 {
		qparams.Where = &params[0]
	}
	if len(params) > 1 {
		qparams.Order = &params[1]
	}
	if len(params) > 2 {
		qparams.Select = &params[2]
	}
	return Find(qparams)
}

// func GetItem(tableName *string, id *string, params ...string) (*map[string]interface{}, error) {
// 	if tableName == nil || id == nil {
// 		return nil, errors.New("id and tableName arguments required")
// 	}
// 	qparams := &QueryParams{
// 		From:  tableName,
// 		Where: &[]string{"id='" + *id + "'"}}

// 	if len(fields) > 0 {
// 		qparams.Select = &fields
// 	}

// 	data, err := FindOne(qparams)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return data, nil
// }

//SetValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func SetValues(query *string, values *map[string]interface{}) error {
	prepText := " "
	for key := range *values {
		prepText += key + "=:" + key
	}
	prepText += " "
	strings.Replace(*query, "?", prepText, -1)
	_, err := DB.NamedExec(*query, *values)
	if err != nil {
		return err
	}
	return nil
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (this *Query) SetStructValues(query string, structVal interface{}, isUpdate ...bool) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)

	if len(isUpdate) > 0 && isUpdate[0] {
		iVal := reflect.ValueOf(structVal).Elem()
		var oldModel interface{}
		for i := 0; i < iVal.NumField(); i++ {
			if iVal.Type().Field(i).Name == "OldModel" {
				oldModel = iVal.Field(i)
				break
			}
		}
		if oldModel != nil {
			iVal := oldModel.(reflect.Value).Elem()
			typ := iVal.Type()
			for i := 0; i < iVal.NumField(); i++ {
				f := iVal.Field(i)
				if f.Kind() == reflect.Ptr {
					f = reflect.Indirect(f)
				}
				if !f.IsValid() {
					continue
				}
				tag := typ.Field(i).Tag.Get("db")
				switch val := f.Interface().(type) {
				case int, int8, int16, int32, int64:
					oldMap[tag] = f.Int()
				case uint, uint8, uint16, uint32, uint64:
					oldMap[tag] = f.Uint()
				case float32, float64:
					oldMap[tag] = f.Float()
				case []byte:
					v := string(f.Bytes())
					oldMap[tag] = v
				case string:
					oldMap[tag] = f.String()
				case time.Time:
					oldMap[tag] = val.Format(time.RFC3339)
				default:
					continue
				}
			}
		}
	}
	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		if !f.IsValid() {
			continue
		}
		tag := typ.Field(i).Tag.Get("db")
		var updV string
		switch val := f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case float32, float64:
			resultMap[tag] = f.Float()
			updV = fmt.Sprintf("%f", resultMap[tag])
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
			updV = resultMap[tag].(string)
		case string:
			resultMap[tag] = f.String()
			updV = resultMap[tag].(string)
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339)
			updV = resultMap[tag].(string)
		default:
			continue
		}

		if len(isUpdate) > 0 && isUpdate[0] {
			if oldMap[tag] != resultMap[tag] {
				prepFields = append(prepFields, `"`+tag+`"`)
				prepValues = append(prepValues, "'"+updV+"'")
			}
		} else {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, ":"+tag)
		}
	}
	var prepText string
	if len(isUpdate) > 0 && isUpdate[0] {
		if len(prepFields) == 0 {
			return errors.New("no fields to update")
		} else if len(prepFields) == 1 {
			prepText = " " + strings.Join(prepFields, ",") + " = " + strings.Join(prepValues, ",") + " "
		} else {
			prepText = " (" + strings.Join(prepFields, ",") + ") = (" + strings.Join(prepValues, ",") + ") "
		}
	} else {
		prepText = " (" + strings.Join(prepFields, ",") + ") VALUES (" + strings.Join(prepValues, ",") + ") "
	}

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if this.tx != nil {
		if len(isUpdate) > 0 && isUpdate[0] {
			_, err = this.tx.Exec(query)
		} else {
			_, err = this.tx.NamedExec(query, resultMap)
		}
	} else {
		if len(isUpdate) > 0 && isUpdate[0] {
			_, err = this.db.Exec(query)
		} else {
			_, err = this.db.NamedExec(query, resultMap)
		}
	}
	if err != nil {
		golog.Error(query)
		golog.Error(err)
		return err
	}
	return nil
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (this *Query) UpdateStructValues(query string, structVal interface{}) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)

	iValOld := reflect.ValueOf(structVal).Elem()
	var oldModel interface{}
	for i := 0; i < iValOld.NumField(); i++ {
		if iValOld.Type().Field(i).Name == "OldModel" {
			oldModel = iValOld.Field(i)
			break
		}
	}
	if oldModel != nil {
		iVal := oldModel.(reflect.Value).Elem()
		typ := iVal.Type()
		for i := 0; i < iVal.NumField(); i++ {
			f := iVal.Field(i)
			if f.Kind() == reflect.Ptr {
				f = reflect.Indirect(f)
			}
			if !f.IsValid() {
				continue
			}
			tag := typ.Field(i).Tag.Get("db")
			switch val := f.Interface().(type) {
			case int, int8, int16, int32, int64:
				oldMap[tag] = f.Int()
			case uint, uint8, uint16, uint32, uint64:
				oldMap[tag] = f.Uint()
			case float32, float64:
				oldMap[tag] = f.Float()
			case []byte:
				v := string(f.Bytes())
				oldMap[tag] = v
			case string:
				oldMap[tag] = f.String()
			case time.Time:
				oldMap[tag] = val.Format(time.RFC3339)
			default:
				continue
			}
		}
	}

	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		if !f.IsValid() {
			continue
		}
		tag := typ.Field(i).Tag.Get("db")
		var updV string
		switch val := f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case float32, float64:
			resultMap[tag] = f.Float()
			updV = fmt.Sprintf("%f", resultMap[tag])
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
			updV = resultMap[tag].(string)
		case string:
			resultMap[tag] = f.String()
			updV = resultMap[tag].(string)
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339)
			updV = resultMap[tag].(string)
		default:
			continue
		}

		if oldMap[tag] != resultMap[tag] {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, "'"+updV+"'")
		}

	}
	var prepText string

	if len(prepFields) == 0 {
		return errors.New("no fields to update")
	} else if len(prepFields) == 1 {
		prepText = " " + strings.Join(prepFields, ",") + " = " + strings.Join(prepValues, ",") + " "
	} else {
		prepText = " (" + strings.Join(prepFields, ",") + ") = (" + strings.Join(prepValues, ",") + ") "
	}

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if this.tx != nil {
		_, err = this.tx.Exec(query)

	} else {
		_, err = this.db.Exec(query)
	}
	if err != nil {
		golog.Error(query)
		golog.Error(err)
		return err
	}
	return nil
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (this *Query) InsertStructValues(query string, structVal interface{}) error {
	resultMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)

	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		if !f.IsValid() {
			continue
		}
		tag := typ.Field(i).Tag.Get("db")
		switch val := f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
		case float32, float64:
			resultMap[tag] = f.Float()
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
		case string:
			resultMap[tag] = f.String()
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339)
		default:
			continue
		}
		prepFields = append(prepFields, `"`+tag+`"`)
		prepValues = append(prepValues, ":"+tag)
	}

	prepText := " (" + strings.Join(prepFields, ",") + ") VALUES (" + strings.Join(prepValues, ",") + ") "

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if this.tx != nil {
		_, err = this.tx.NamedExec(query, resultMap)

	} else {
		_, err = this.db.NamedExec(query, resultMap)
	}
	if err != nil {
		golog.Error(query)
		golog.Error(err)
		return err
	}
	return nil
}

func GetStructValues(structVal interface{}, fields *[]string) map[string]interface{} {
	resultMap := make(map[string]interface{})
	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		tag := typ.Field(i).Tag.Get("db")
		isFound := false
		for _, field := range *fields {
			if field == tag {
				isFound = true
			}
		}
		if !isFound {
			continue
		}
		switch f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
		case float32, float64:
			resultMap[tag] = f.Float()
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
		case string:
			resultMap[tag] = f.String()
		default:
			continue
		}
	}
	return resultMap
}

func MakeQueryFromReq(req map[string]string) string {
	limit := req["limit"]
	offset := req["offset"]
	if limit == "" {
		limit = "1000"
	}
	if offset == "" {
		offset = "0"
	}
	where := ""
	orderby := ""
	q := "LIMIT " + limit + " OFFSET " + offset
	for p, v := range req {
		if p == "limit" || p == "offset" || p == "sort" {
			continue
		}
		f := strings.Split(p, "-")
		if where != "" {
			where += " AND "
		}
		switch f[1] {
		case "text":
			where += `"` + f[0] + `" ILIKE '%` + v + "%'"
		case "date":
			where += `"` + f[0] + `" >= '` + v + "'"
		}

	}
	if where != "" {
		where = "WHERE " + where
	}
	if val, ok := req["sort"]; ok {
		sortParams := strings.Split(val, "-")
		orderby += `ORDER BY "` + sortParams[0] + `" ` + sortParams[1]
	}
	return where + " " + orderby + " " + q
}

//Init open connection to database
func Init() {
	var err error
	if err != nil {
		golog.Error(err)
	}
	envURI := os.Getenv("SQL_URI")
	if envURI == "" {
		envURI = DefaultURI
	}
	db, err := sqlx.Connect("postgres", envURI)
	if err != nil {
		golog.Error("failed connect to database:", envURI, " ", err)
	} else {
		golog.Info("success connect to database:", envURI)
	}
	DB = db
	Q, err = NewQuery(false)
}
