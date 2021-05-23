package sql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/buger/jsonparser"
	"gitlab.com/battler/modules/csxamqp"
	"gitlab.com/battler/modules/csxjson"
	csxstrings "gitlab.com/battler/modules/csxstrings"
	"gitlab.com/battler/modules/csxutils"
)

const DUPLICATE_KEY_ERROR = "23505"
const TABLE_NOT_EXISTS = "42P01"

var (
	//DefaultURI default SQL connection string
	amqpURI = os.Getenv("AMQP_URI")
	// DEPRECATED:: Need only for compatible
	sqlURI       = os.Getenv("SQL_URI")
	mainDatabase = os.Getenv("MAIN_SQL_DATABASE")
)

//DB is pointer to DB connection
var (
	DB              *sqlx.DB
	Q               Query
	initedDatabases map[string]int
)

func initialized() bool {
	return DB != nil
}

// DatabaseParams struct with params required to create database connect
type DatabaseParams struct {
	DriverName       string `json:"driverName"`
	ConnectionString string `json:"connectionString"`
}

// QueryParams Structure for transmitting SQL request parameters
// All parameters except for "from" are passed in arrays
type QueryParams struct {
	BaseTable string
	Select    *[]string
	From      *string
	Where     *[]string
	Order     *[]string
	Group     *[]string
}

// QueryStringParams Structure for working with query parameters as strings
type QueryStringParams struct {
	Select *string
	From   *string
	Where  *string
	Order  *string
	Group  *string
}

// QueryResult The structure into which the result of the SQL request is parsed
type QueryResult struct {
	Result []map[string]interface{}
	Error  error
	Query  string
	Xlsx   []byte
}

// Query struct for run SQL queries in transaction
// with commit or rollback methods
type Query struct {
	Tx                *sqlx.Tx // need for outer modules
	tx                *sqlx.Tx
	db                *sqlx.DB
	txCommitCallbacks []func()
}
type AccessFieldsMap struct {
	Access map[string]bool
	Deny   map[string]bool
}

//  check manadat access
func (accessFieldsMap *AccessFieldsMap) Check(subject string) bool {
	if len(accessFieldsMap.Access) > 0 {
		if _, ok := accessFieldsMap.Access[subject]; !ok {
			return false
		}
		if _, ok := accessFieldsMap.Deny[subject]; ok {
			return false
		}
	} else {
		if _, ok := accessFieldsMap.Deny[subject]; ok {
			return false
		}
	}
	return true
}

//  check manadat access
func (accessFieldsMap *AccessFieldsMap) Load(access, deny []string) {
	accessFieldsMap.Access = map[string]bool{}
	accessFieldsMap.Deny = map[string]bool{}
	for i := 0; i < len(access); i++ {
		accessFieldsMap.Access[access[i]] = true
	}
	for i := 0; i < len(deny); i++ {
		accessFieldsMap.Deny[deny[i]] = true
	}
}

// NewQuery Constructor for creating a pointer to work with the main database
func NewQuery(useTransaction bool) (q Query, err error) {
	q = Query{db: DB}
	if useTransaction {
		q.tx, err = DB.Beginx()
		q.Tx = q.tx
	}
	return q, err
}

// NewQuery Constructor for creating a pointer to work with the main database
func NewDBQuery(db *sqlx.DB, useTransaction bool) (q *Query, err error) {
	q = &Query{db: db}
	if useTransaction {
		q.tx, err = DB.Beginx()
		q.Tx = q.tx
	}
	return q, err
}

// BeginTransaction Constructor for creating a pointer to work with the base and begin new transaction
func BeginTransaction() (q *Query, err error) {
	q = &Query{db: DB}
	q.tx, err = DB.Beginx()
	q.Tx = q.tx
	return q, err
}

func (queryObj *Query) IsTransact() bool {
	return queryObj.tx != nil
}

// Commit commit transaction
func (queryObj *Query) Commit() (err error) {
	err = queryObj.tx.Commit()
	if err == nil && len(queryObj.txCommitCallbacks) > 0 {
		for _, cb := range queryObj.txCommitCallbacks {
			cb()
		}
	}
	return err
}

// Rollback rollback transaction
func (queryObj *Query) Rollback() (err error) {
	return queryObj.tx.Rollback()
}

// BindTxCommitCallback Sets the callback that is executed when a transaction is confirmed
func (queryObj *Query) BindTxCommitCallback(cb func()) {
	queryObj.txCommitCallbacks = append(queryObj.txCommitCallbacks, cb)
}

// GetWithArg run get SQL query and write result to first argument
func (queryObj *Query) GetWithArg(data interface{}, query string) (err error) {
	if queryObj.tx != nil {
		return queryObj.tx.Get(data, query)
	}
	return queryObj.db.Get(data, query)
}

// Exec exec simple query with optional args
func (queryObj *Query) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if queryObj.tx != nil {
		return queryObj.tx.Exec(query, args...)
	}
	return queryObj.db.Exec(query, args...)
}

// Select select data from SQL database with optional args
func (queryObj *Query) Select(dest interface{}, query string, args ...interface{}) error {
	if queryObj.tx != nil {
		return queryObj.tx.Select(dest, query, args...)
	}
	return queryObj.db.Select(dest, query, args...)
}

// In wrapper for sqlx.In
func (queryObj *Query) In(query string, args ...interface{}) (string, []interface{}, error) {
	return sqlx.In(query, args...)
}

// JsonB struct for work with jsonb database fields
type JsonB map[string]interface{}

// Scan interface for read jsonb content
func (js *JsonB) Scan(src interface{}) error {
	val, ok := src.([]byte)
	if !ok {
		return errors.New("unable scan jsonb")
	}
	return json.Unmarshal(val, js)
}

// Decode correct decoding `map[string]interface{}` for `csxhttp.Context`
func (js *JsonB) Decode(body []byte) error {
	return json.Unmarshal(body, &js)
}

var (
	baseQuery    = `SELECT {{.Select}} FROM {{.From}} {{.Where}} {{.Group}} {{.Order}}`
	baseTemplate = template.Must(template.New("").Parse(baseQuery))
)

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

// TODO:: Move save log to modelLog
func (queryObj *Query) SaveLog(table string, item string, user string, diff map[string]interface{}) {
	if len(diff) > 0 {
		if user == "" {
			logrus.Error("save log tbl:" + table + " item:" + item + " err: current user is not defined")
			return
		}
		keys := []string{}
		values := []string{}
		for key := range diff {
			keys = append(keys, key)
			values = append(values, ":"+key)
		}
		id := csxstrings.NewId()
		query := `INSERT INTO "modelLog" ("id","table","item","user","diff","time") VALUES (:id,:table,:item,:user,:diff,:time)`
		diffByte, err := json.Marshal(diff)
		if err != nil {
			logrus.Error("save log tbl:"+table+" item:"+item+" err:", err)
			return
		}
		now := time.Now().UTC()
		logItem := map[string]interface{}{
			"id":    id,
			"table": table,
			"item":  item,
			"user":  user,
			"diff":  string(diffByte),
			"time":  now,
		}

		if queryObj.tx != nil {
			_, err = queryObj.tx.NamedExec(query, logItem)
		} else {
			_, err = queryObj.db.NamedExec(query, logItem)
		}
		if err != nil {
			logrus.Error("save log tbl:"+table+" item:"+item+" err:", err)
		}
	}
}

func GetStrictFields(fields []string, accessFieldsMap *AccessFieldsMap, prepareFieldName bool) []string {
	result := []string{}
	for i := 0; i < len(fields); i++ {
		fieldName := fields[i]
		var preparedFieldName string
		if prepareFieldName {
			preparedFieldName = strings.ReplaceAll(strings.ReplaceAll(fieldName, `"`, ``), " ", "")
		} else {
			preparedFieldName = fieldName
		}
		if strings.Contains(preparedFieldName, " as ") || strings.Contains(preparedFieldName, " AS ") {
			parts := strings.Split(preparedFieldName, " ")
			aliasName := strings.ReplaceAll(strings.ReplaceAll(parts[len(parts)-1], `"`, ``), " ", "")
			preparedFieldName = aliasName
		}
		isAccess := true
		if len(accessFieldsMap.Access) > 0 && !accessFieldsMap.Access[preparedFieldName] {
			isAccess = false
		}
		if len(accessFieldsMap.Deny) > 0 && accessFieldsMap.Deny[preparedFieldName] {
			isAccess = false
		}
		if isAccess {
			result = append(result, fieldName)
		}
	}
	return result
}

// MakeQuery make sql string expression from params
func MakeQuery(params *QueryParams, options ...map[string]interface{}) (*string, error) {
	var accessFieldsMap *AccessFieldsMap
	if len(options) > 0 {
		opts := options[0]
		accessFieldsMapInt, ok := opts["accessFieldsMap"]
		if ok {
			accessFieldsMap, _ = accessFieldsMapInt.(*AccessFieldsMap)
		}
	}
	fields := "*"
	var from, where, group, order string
	if params.BaseTable != "" {
		fields = prepareBaseFields(params.BaseTable, params.Select)
	} else if params.Select != nil {
		fields = prepareFields(params.Select)
	}

	if accessFieldsMap != nil {
		fieldsArr := strings.Split(fields, ",")
		restrictFieldsArr := GetStrictFields(fieldsArr, accessFieldsMap, true)
		fields = strings.Join(restrictFieldsArr, ",")
	}
	if params.From != nil {
		from = *params.From
		if from[0] != '"' && !strings.Contains(from, " ") {
			from = `"` + from + `"`
		}
	}
	if params.Where != nil {
		where = " WHERE " + prepareFields(params.Where)
	}
	if params.Order != nil && len(*params.Order) > 0 {
		order = " ORDER BY " + prepareFields(params.Order)
	}
	if params.Group != nil {
		group = " GROUP BY " + prepareFields(params.Group)
	}
	pStruct := QueryStringParams{&fields, &from, &where, &order, &group}
	var tpl bytes.Buffer
	err := baseTemplate.Execute(&tpl, pStruct)
	if err != nil {
		return nil, err
	}
	query := tpl.String()
	return &query, nil
}

// execQuery exec query and run callback with query result
func execQuery(db *sqlx.DB, q *string, fieldsMap *AccessFieldsMap, cb ...func(rows *sqlx.Rows) bool) *QueryResult {
	rows, err := db.Queryx(*q)
	if err != nil {
		return &QueryResult{Error: err}
	}
	defer rows.Close()
	// Launching the transferred callbacks with the transfer of the strings received from the base.
	// The resulting lines can be used, for example, to pre-filter them, or to customize the algorithm for their processing
	lenCb := len(cb)
	parseRows := true
	if lenCb > 0 {
		for i := 0; i < lenCb; i++ {
			breakProcessing := cb[i](rows)
			if breakProcessing {
				// The case when the callback serves for custom processing of records
				// and after bypassing the callbacks there is no need to prepare additional records.
				parseRows = false
			}
		}
	}
	if !parseRows {
		return nil
	}
	results := QueryResult{Query: *q}
	var parsingTime int64 = 0
	checkFields := fieldsMap != nil
	for rows.Next() {
		row := map[string]interface{}{}
		start := time.Now()
		// TODO:: MapScan works quite slowly. You need to figure out how to optimize, if possible.
		err = rows.MapScan(row)
		duration := time.Since(start)
		parsingTime += duration.Nanoseconds()

		for k, v := range row {
			if checkFields && !fieldsMap.Check(k) {
				delete(row, k)
				continue
			}
			//TODO:: You need to think about how to get rid of the extra cycle
			switch v.(type) {
			case []byte:
				// CRUTCH:: The execQuery function needs to be reworked.
				// It should work through a receiver, which will contain a parameter map for typing fields,
				// in most cases based on the table schema.
				valueForParse := v.([]byte)
				lenValue := len(valueForParse)
				if lenValue == 0 {
					continue
				}
				// try check for uuid value
				if lenValue == 36 {

					// check method is crutch-like,
					// but nevertheless the fastest and most efficient.
					// The parse method in the uid library from Google works several times slower.

					// 	 --- Typically this method is used in this scenario ---:
					// query: = dbc.MakeQueryFromReq (params, whereExt)
					// data: = dbc.ExecQuery (& query)
					//
					// In fact, you can create a function that will replace this construction with the following:
					// data: = dbc.ExecQueryFromReq(params, &query)
					// There is a table in the parameters, the field types of which can be obtained through the table schema

					stringValue := string(valueForParse)
					if stringValue[8] == '-' && stringValue[13] == '-' && stringValue[18] == '-' && stringValue[23] == '-' {
						row[k] = string(valueForParse)
						continue
					}
				}
				// parse json value
				data, dataType, _, _ := jsonparser.Get(valueForParse)
				if isPgArray(data) {
					dataType = csxjson.PgArrayType
				}
				val, _ := csxjson.GetParsedValue(data, dataType)
				row[k] = val
			}
		}
		results.Result = append(results.Result, row)
	}
	return &results
}

func isPgArray(data []byte) bool {
	return len(data) > 1 && data[0] == '{' && data[1] != '"'
}

// ExecQuery exec query in main database and run callback with query result
func ExecQuery(queryString *string, cb ...func(rows *sqlx.Rows) bool) *QueryResult {
	return execQuery(DB, queryString, nil, cb...)
}

// ExecRestrictQuery exec query in main database and run callback with query result
func ExecRestrictQuery(queryString *string, fieldsMap *AccessFieldsMap, cb ...func(rows *sqlx.Rows) bool) *QueryResult {
	return execQuery(DB, queryString, fieldsMap, cb...)
}

// Find find records from database
func Find(params *QueryParams) *QueryResult {
	query, err := MakeQuery(params)
	if err != nil {
		return &QueryResult{Error: err}
	}
	return ExecQuery(query)
}

// FindOne find record from database
func FindOne(params *QueryParams) (*map[string]interface{}, error) {
	query, err := MakeQuery(params)
	if err != nil {
		return nil, err
	}
	data := ExecQuery(query)
	if data.Error != nil {
		return nil, data.Error
	}
	if data.Result != nil && len(data.Result) > 0 {
		return &data.Result[0], nil
	}
	return nil, nil
}

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

// Delete run delete query in transaction
func (queryObj *Query) Delete(query string) (err error) {
	if queryObj.tx != nil {
		_, err = queryObj.tx.Exec(query)
	} else {
		_, err = queryObj.db.Exec(query)
	}
	return err
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
//! DEPRECATED. Use InsertStructValues() for inserting or UpdateStructValues() for updating
func (queryObj *Query) SetStructValues(query string, structVal interface{}, isUpdate ...bool) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)
	cntAuto := 0

	checkOldModel := len(isUpdate) > 0 && isUpdate[0]
	if checkOldModel {
		iVal := reflect.ValueOf(structVal).Elem()
		oldModel := iVal.FieldByName("OldModel")
		if oldModel.IsValid() {
			iVal := oldModel.Elem()
			if iVal.IsValid() {
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
					tagWrite := typ.Field(i).Tag.Get("dbField")
					if tagWrite == "-" || tag == "-" ||
						(tag == "" && tagWrite == "") {
						continue
					}
					if tagWrite != "" && tagWrite[0] != '-' {
						tag = tagWrite
					}
					switch val := f.Interface().(type) {
					case bool:
						oldMap[tag] = f.Bool()
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
						oldMap[tag] = val.Format(time.RFC3339Nano)
					case []string:
						jsonArray, _ := json.Marshal(val)
						oldMap[tag] = jsonArray
					default:
						continue
					}
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
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		auto := false
		if tagWrite != "" {
			if tagWrite[0] != '-' {
				tag = tagWrite
			} else if tagWrite == "-auto" {
				auto = true
			}

		}
		var updV string

		switch val := f.Interface().(type) {
		case bool:
			resultMap[tag] = f.Bool()
			updV = strconv.FormatBool(resultMap[tag].(bool))
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
			resultMap[tag] = val.Format(time.RFC3339Nano)
			updV = resultMap[tag].(string)
		case []string:
			jsonArray, _ := json.Marshal(val)
			resultMap[tag] = string(jsonArray)
			updV = string(jsonArray)
		default:
			continue
		}

		if checkOldModel {
			if oldMap[tag] != resultMap[tag] {
				prepFields = append(prepFields, `"`+tag+`"`)
				prepValues = append(prepValues, "'"+updV+"'")
				if auto {
					cntAuto++
				}
			}
		} else {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, ":"+tag)
		}
	}

	var prepText string
	if checkOldModel {
		if len(prepFields) <= cntAuto {
			return nil //errors.New("no fields to update")
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
	if queryObj.tx != nil {
		if len(isUpdate) > 0 && isUpdate[0] {
			_, err = queryObj.tx.Exec(query)
		} else {
			_, err = queryObj.tx.NamedExec(query, resultMap)
		}
	} else {
		if len(isUpdate) > 0 && isUpdate[0] {
			_, err = queryObj.db.Exec(query)
		} else {
			_, err = queryObj.db.NamedExec(query, resultMap)
		}
	}
	if err != nil {
		logrus.Error(query)
		logrus.Error(err)
		return err
	}

	return nil
}

//UpdateStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (queryObj *Query) UpdateStructValues(query string, structVal interface{}, options ...interface{}) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)
	diff := make(map[string]interface{})
	diffPub := make(map[string]interface{})
	cntAuto := 0

	iValOld := reflect.ValueOf(structVal).Elem()
	oldModel := iValOld.FieldByName("OldModel")
	if oldModel.IsValid() {
		iVal := oldModel.Elem()
		if iVal.IsValid() {
			typ := iVal.Type()
			for i := 0; i < typ.NumField(); i++ {
				f := iVal.Field(i)
				if f.Kind() == reflect.Ptr {
					f = reflect.Indirect(f)
				}
				if !f.IsValid() {
					continue
				}
				ft := typ.Field(i)
				tag := ft.Tag.Get("db")
				tagWrite := ft.Tag.Get("dbField")
				if tagWrite == "-" || tag == "-" ||
					(tag == "" && tagWrite == "") {
					continue
				}
				if tagWrite != "" && tagWrite[0] != '-' {
					tag = tagWrite
				}
				switch val := f.Interface().(type) {
				case bool:
					oldMap[tag] = f.Bool()
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
					oldMap[tag] = val.Format(time.RFC3339Nano)
				case pq.StringArray:
					arr := f.Interface().(pq.StringArray)
					oldMap[tag] = "{" + strings.Join(arr, ",") + "}"
				default:
					valJSON, _ := json.Marshal(val)
					oldMap[tag] = string(valJSON)
				}
			}
		}
	}

	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		isPointer := false
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
			isPointer = true
		}
		ft := typ.Field(i)
		tag := ft.Tag.Get("db")
		tagWrite := ft.Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		auto := false
		log := true
		if tagWrite != "" {
			if tagWrite[0] != '-' {
				tag = tagWrite
			} else if tagWrite == "-auto" {
				auto = true
			} else if tagWrite == "-nolog" {
				log = false
			}
		}
		if !f.IsValid() {
			if isPointer && oldMap[tag] != nil {
				resultMap[tag] = nil
				diff[tag] = nil
				diffPub[tag] = nil
				prepFields = append(prepFields, `"`+tag+`"`)
				prepValues = append(prepValues, "NULL")
			}
			continue
		}

		var updV string
		var rawValue interface{}

		switch val := f.Interface().(type) {
		case bool:
			resultMap[tag] = f.Bool()
			updV = strconv.FormatBool(resultMap[tag].(bool))
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
			resultMap[tag] = val.Format(time.RFC3339Nano)
			updV = resultMap[tag].(string)
		case pq.StringArray:
			arr := f.Interface().(pq.StringArray)
			updVal := "{" + strings.Join(arr, ",") + "}"
			resultMap[tag] = updVal
			updV = updVal
		default:
			rawValue = val
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
			updV = string(valJSON)
		}

		if oldMap[tag] != resultMap[tag] {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, "'"+updV+"'")
			if log {
				tagVal := resultMap[tag]
				diffVal := []interface{}{tagVal}
				if oldMap[tag] != nil {
					diffVal = append(diffVal, oldMap[tag])
				}
				diff[tag] = diffVal
				if rawValue != nil {
					diffPub[tag] = rawValue
				} else {
					diffPub[tag] = tagVal
				}
			}
			if auto {
				cntAuto++
			}
		}
	}
	var prepText string

	if len(prepFields) <= cntAuto {
		return nil
	} else if len(prepFields) == 1 {
		prepText = " " + strings.Join(prepFields, ",") + " = " + strings.Join(prepValues, ",") + " "
	} else {
		prepText = " (" + strings.Join(prepFields, ",") + ") = (" + strings.Join(prepValues, ",") + ") "
	}

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if queryObj.tx != nil {
		_, err = queryObj.tx.Exec(query)

	} else {
		_, err = queryObj.db.Exec(query)
	}
	if err != nil {
		logrus.Error(query)
		logrus.Error(err)
		return err
	}

	if len(options) > 0 {
		var id, user string
		withLog := true
		table := csxutils.LowerFirst(typ.Name())
		opts, ok := options[0].([]string)
		if !ok {
			optsMap, okMap := options[0].(map[string]string)
			if okMap {
				id = optsMap["id"]
				user = optsMap["user"]
				if optsMap["table"] != "" {
					table = optsMap["table"]
				}
				if optsMap["withLog"] == "false" || optsMap["withLog"] == "0" {
					withLog = false
				}
			} else {
				logrus.Error("err get info for log model info:", options)
			}
		} else if len(opts) > 1 {
			// 0 - id
			// 1 - user
			// 2 - table name
			if len(opts) > 2 {
				table = opts[2]
			}
			id = opts[0]
			user = opts[1]
		}

		if table != "" && id != "" {
			go csxamqp.SendUpdate(amqpURI, table, id, "update", diffPub)
			if withLog {
				queryObj.SaveLog(table, id, user, diff)
			}
		}
	}
	return nil
}

//InsertStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (queryObj *Query) InsertStructValues(query string, structVal interface{}, options ...interface{}) error {
	resultMap := make(map[string]interface{})
	diffPub := make(map[string]interface{})
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
		if tag == "-" {
			continue
		}
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		if tagWrite != "" && tagWrite[0] != '-' {
			tag = tagWrite
		}
		switch val := f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
			diffPub[tag] = resultMap[tag]
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
			diffPub[tag] = resultMap[tag]
		case float32, float64:
			resultMap[tag] = f.Float()
			diffPub[tag] = resultMap[tag]
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
			diffPub[tag] = resultMap[tag]
		case string:
			resultMap[tag] = f.String()
			diffPub[tag] = resultMap[tag]
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339Nano)
			diffPub[tag] = resultMap[tag]
		default:
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
			diffPub[tag] = val
		}
		prepFields = append(prepFields, `"`+tag+`"`)
		prepValues = append(prepValues, ":"+tag)
	}

	prepText := " (" + strings.Join(prepFields, ",") + ") VALUES (" + strings.Join(prepValues, ",") + ") "

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if queryObj.tx != nil {
		_, err = queryObj.tx.NamedExec(query, resultMap)

	} else {
		_, err = queryObj.db.NamedExec(query, resultMap)
	}
	if err != nil {
		logrus.Error(query)
		logrus.Error(err)
		return err
	}
	if len(options) > 0 {
		settings, ok := options[0].(map[string]string)
		if !ok {
			logrus.Error("err parse opt:", options)
			return nil
		}
		if len(settings) < 2 {
			logrus.Error("amqp updates, invalid args:", settings)
			return nil
		}
		table := settings["table"]
		id := settings["id"]
		go csxamqp.SendUpdate(amqpURI, table, id, "create", diffPub)
	}
	return nil
}

// GetMapFromStruct return map from struct
func GetMapFromStruct(structVal interface{}, options ...map[string]interface{}) map[string]interface{} {
	iVal := reflect.Indirect(reflect.ValueOf(structVal))
	typ := iVal.Type()
	var checkTags *bool
	var tagsForCheck map[string]bool
	mapToJson := true
	if len(options) > 0 {
		opts := options[0]
		if val, ok := opts["checkTags"]; ok {
			v := val.(bool)
			checkTags = &v
		}
		if val, ok := opts["mapToJson"]; ok {
			mapToJson = val.(bool)
		}
		if val, ok := opts["roles"]; ok {
			tagsForCheck = map[string]bool{}
			roles := val.(map[string]interface{})
			for _, roleInt := range roles {
				role := roleInt.(map[string]interface{})
				rightsInt := role["rights"]
				if rightsInt == nil {
					continue
				}
				allRights := JsonB(rightsInt.(map[string]interface{}))
				if val, ok := opts["collection"]; ok {
					rightsInt := allRights[val.(string)]
					rights, ok := rightsInt.(map[string]interface{})
					if ok {
						for key := range rights {
							tagsForCheck[key] = true
						}
					}
				}
			}
		}
	}
	res := make(map[string]interface{})
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		ft := typ.Field(i)
		tag := ft.Tag.Get("db")

		if checkTags != nil {
			if *checkTags {
				if tag == "" || tag == "-" {
					continue
				}
			} else {
				tag = strings.ToLower(ft.Name)
			}
		} else {
			if len(tagsForCheck) > 0 {
				tag = strings.ToLower(ft.Name)
				if !tagsForCheck[tag] {
					continue
				}
			} else if tag == "" || tag == "-" {
				continue
			}
		}

		if f.IsValid() {
			if f.Kind() == reflect.Map && !mapToJson {
				mapInt := map[string]interface{}{}
				for _, key := range f.MapKeys() {
					var keyStr string
					switch key.Interface().(type) {
					case bool:
						keyStr = fmt.Sprintf("%t", key.Bool())
					case int, int8, int16, int32, int64:
						keyStr = fmt.Sprintf("%d", key.Int())
					case uint, uint8, uint16, uint32, uint64:
						keyStr = fmt.Sprintf("%d", key.Uint())
					case float32, float64:
						keyStr = fmt.Sprintf("%f", key.Float())
					case []byte:
						v := string(key.Bytes())
						keyStr = v
					case string:
						keyStr = key.String()
					}
					mapInt[keyStr] = f.MapIndex(key).Interface()
				}
				res[tag] = mapInt
				continue
			}
			switch val := f.Interface().(type) {
			case bool:
				res[tag] = f.Bool()
			case int, int8, int16, int32, int64:
				res[tag] = f.Int()
			case uint, uint8, uint16, uint32, uint64:
				res[tag] = f.Uint()
			case float32, float64:
				res[tag] = f.Float()
			case []byte:
				v := string(f.Bytes())
				res[tag] = v
			case string:
				res[tag] = f.String()
			case time.Time:
				res[tag] = val.Format(time.RFC3339Nano)
			case pq.StringArray:
				arr := f.Interface().(pq.StringArray)
				res[tag] = "{" + strings.Join(arr, ",") + "}"
			default:
				valJSON, _ := json.Marshal(val)
				res[tag] = string(valJSON)
			}
		} else {
			res[tag] = nil
		}

	}
	return res
}

// GetStructValues create map from strunt
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

// MakeQueryFromReq create sql query exspression from string map
func MakeQueryFromReq(req map[string]string, extConditions ...string) string {
	r := strings.NewReplacer("create ", "", "insert ", "", " set ", "", "drop ", "", "alter ", "", "update ", "", "delete ", "", "CREATE ", "", "INSERT ", "", " SET ", "", "DROP ", "", "ALTER ", "", "UPDATE ", "", "DELETE ", "")
	limit := req["limit"]
	offset := req["offset"]
	join := req["join"]
	table := req["table"]
	isCount := req["count"] == "1"
	startDatePagination := req["startDatePagination"]
	endDatePagination := req["endDatePagination"]
	prevStartDatePagination := req["prevStartDatePagination"]
	prevEndDatePagination := req["prevEndDatePagination"]
	paginationField := req["paginationField"]
	if limit == "" {
		limit = "1000"
	}
	if offset == "" {
		offset = "0"
	}

	newQ := ""
	if join != "" || table != "" {
		newQ = `SELECT `
		if isCount {
			newQ += `COUNT(*) `
		} else {
			fields := req["fields"]
			if fields == "" {
				fields = "*"
			}
			newQ += fields
		}
		if join != "" {
			newQ += ` FROM ` + join + ` `
		} else {
			newQ += ` FROM "` + table + `" `
		}
	}

	where := ""
	if len(extConditions) > 0 {
		where += extConditions[0]
	}
	if filtered, ok := req["filter"]; ok {
		filters := strings.Split(filtered, "$")
		for _, kv := range filters {
			if kv == "" {
				continue
			}
			keyValue := strings.Split(kv, "->")
			if len(keyValue) < 2 {
				continue
			}
			v := keyValue[1]
			f := strings.Split(v, "~")
			if len(f) > 1 {
				v := f[1]
				if where != "" {
					where += " AND "
				}
				multiFields := strings.Split(f[0], ",")
				lenMultiFields := len(multiFields)
				if lenMultiFields > 1 {
					where += " ("
					for i := 0; i < lenMultiFields; i++ {
						mField := multiFields[i]
						fieldParts := strings.Split(mField, ".")
						var field string
						if len(fieldParts) < 2 {
							field = `"` + mField + `"`
						} else {
							field = mField
						}
						addCondition(keyValue[0], field, v, &where)
						if i != lenMultiFields-1 {
							where += " OR "
						}
					}
					where += ") "
				} else {
					fieldParts := strings.Split(f[0], ".")
					var field string
					if len(fieldParts) < 2 {
						field = `"` + f[0] + `"`
					} else {
						field = f[0]
					}
					addCondition(keyValue[0], field, v, &where)
				}
			}
		}
	}
	if where != "" {
		where = "WHERE " + where
	}
	groupby := ""
	if val, ok := req["group"]; ok && val != "" {
		groupby += "GROUP BY " + val
	}
	orderby := ""
	if val, ok := req["sort"]; ok && val != "" && !isCount {
		orderby += "ORDER BY "
		sortFields := strings.Split(val, ",")
		for iSort := 0; iSort < len(sortFields); iSort++ {
			if iSort > 0 {
				orderby += ","
			}
			sortParams := strings.Split(sortFields[iSort], "-")
			if _, err := strconv.ParseInt(sortParams[0], 10, 16); err == nil {
				orderby += sortParams[0]
			} else {
				sortFieldParts := strings.Split(sortParams[0], ".")
				if len(sortFieldParts) > 1 { // 2 parts: <table name>.<field name>
					orderby += `"` + sortFieldParts[0] + `"."` + sortFieldParts[1] + `"`
				} else {
					orderby += `"` + sortParams[0] + `"`
				}

			}
			if len(sortParams) > 1 {
				orderby += " " + sortParams[1]
			}
			if v, o := req["nulls"]; o && v != "" {
				orderby += ` NULLS ` + v
			}
		}

	}
	paginationQuery := ""
	prevPaginationQuery := ""
	if paginationField != "" {
		paginationQuery = `"` + paginationField + `" >= '` + startDatePagination + `' AND "` + paginationField + `" < '` + endDatePagination + `'`
		if prevStartDatePagination != "" {
			prevPaginationQuery = `"` + paginationField + `" >= '` + prevStartDatePagination + `' AND "` + paginationField + `" < '` + prevEndDatePagination + `'`
		}
	}
	fullReq := r.Replace(newQ + " " + where + " " + groupby + " " + orderby)
	if !isCount {
		fullReq += " LIMIT " + limit + " OFFSET " + offset
	}
	if paginationQuery != "" {
		pagRepl := strings.NewReplacer("{{pagination}}", paginationQuery)
		prevPagRepl := strings.NewReplacer("{{prevPagination}}", prevPaginationQuery)
		fullReq = pagRepl.Replace(fullReq)
		fullReq = prevPagRepl.Replace(fullReq)
	}

	// fmt.Println(fullReq)
	return fullReq
}

func addCondition(fieldType, field, v string, where *string) {
	switch fieldType {
	case "similar":
		*where += field + ` SIMILAR TO '%` + v + "%'"
	case "notsimilar":
		*where += field + ` NOT SIMILAR TO '%` + v + "%'"
	case "text":
		// CRUTCH:: This is necessary for working with numeric fields.
		// It is necessary to add normal filtering by number fields with comparison on > and <
		*where += field + `::varchar ILIKE '%` + v + "%'"
	case "ilike":
		// CRUTCH:: This is necessary for working with numeric fields.
		// It is necessary to add normal filtering by number fields with comparison on > and <
		*where += field + `::varchar ILIKE '%` + v + "%'"
	case "notilike":
		*where += field + ` NOT ILIKE '%` + v + "%'"
	case "date":
		rangeDates := strings.Split(v, "_")
		beginDate, err := strconv.ParseInt(rangeDates[0], 10, 64)
		if err != nil {
			return
		}
		tmBegin := time.Unix(beginDate/1000, 0).UTC().Format("2006-01-02 15:04:05")
		*where += field + ` >= '` + tmBegin + "'"
		if len(rangeDates) > 1 {
			endDate, err := strconv.ParseInt(rangeDates[1], 10, 64)
			if err != nil {
				return
			}
			tmEnd := time.Unix(endDate/1000, 0).UTC().Format("2006-01-02 15:04:05")
			*where += ` AND ` + field + ` <= '` + tmEnd + "'"
			// startDatePagination = tmBegin
			// endDatePagination = tmEnd
		}
	case "select":
		*where += field + ` = '` + v + "'"
	case "mask":
		*where += field + ` & ` + v + " > 0"
	case "notMask":
		*where += field + ` & ` + v + " = 0"
	case "lte":
		*where += field + ` <= '` + v + "'"
	case "lten":
		*where += `(` + field + ` IS NULL OR ` + field + ` <= '` + v + "')"
	case "gte":
		*where += field + ` >= '` + v + "'"
	case "gten":
		*where += `(` + field + ` IS NULL OR ` + field + ` >= '` + v + "')"
	case "lt":
		*where += field + ` < '` + v + "'"
	case "gt":
		*where += field + ` > '` + v + "'"
	case "is":
		*where += field + ` IS ` + v
	case "in":
		*where += field + ` IN (` + v + `)`
	case "notin":
		*where += field + ` NOT IN(` + v + `)`
	}
}

// PreparePaySystemQuery creates query for payment system identifying
func PreparePaySystemQuery(field string, alias ...string) string {
	if field == "" {
		return ""
	}

	query := `(CASE `
	for digits, system := range csxutils.PaySystems {
		query += " WHEN " + field + " ILIKE " + "'" + digits + "%' THEN '" + system + "'"
	}
	query += " ELSE 'Unknown' END)"
	if len(alias) > 0 {
		query += ` AS "` + alias[0] + `"`
	}

	return query
}

// Init open connection to database
func Init() {
	if initialized() {
		return
	}
	// << DEPRECATED:: Need for compatible with old connection logic
	if mainDatabase == "" {
		mainDatabase = "main"
	}

	// TODO:: Need to deal with the default parameters
	//db.DB.SetMaxIdleConns(10)  // The default is defaultMaxIdleConns (= 2)
	//db.DB.SetMaxOpenConns(100)  // The default is 0 (unlimited)
	//db.DB.SetConnMaxLifetime(3600 * time.Second)  // The default is 0 (connections reused forever)

	var err error
	DB, err = sqlx.Connect("postgres", sqlURI)
	if err != nil {
		logrus.Error("failed connect to main database:", sqlURI, " ", err)
		time.Sleep(20 * time.Second)
		Init()
	}
	// << DEPRECATED::
	Q, _ = NewQuery(false)
	// DEPRECATED:: >>
	logrus.Info("success connect to main database:", sqlURI)
}

//---------------------------------------------------------------------------

func GetDBErrorCode(err error) pq.ErrorCode {
	pqErr := err.(*pq.Error)
	return pqErr.Code
}

func (queryObj *Query) NamedExec(query string, resultMap map[string]interface{}) (*sql.Result, error) {
	var err error
	var result sql.Result
	if queryObj.IsTransact() {
		result, err = queryObj.tx.NamedExec(query, resultMap)
	} else {
		result, err = queryObj.db.NamedExec(query, resultMap)
	}
	return &result, err
}

//---------------------------------------------------------------------------
