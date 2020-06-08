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
	"sync"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/golog"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	deepcopier "github.com/mohae/deepcopy"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"

	amqp "gitlab.com/battler/modules/amqpconnector"
	strUtil "gitlab.com/battler/modules/strings"
)

var (
	//DefaultURI default SQL connection string
	DefaultURI = "host=localhost user=postgres password=FHJdg876h&*^6fd dbname=csx sslmode=disable"
	amqpURI    = os.Getenv("AMQP_URI")
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
	Group     *[]string
}

type QueryStringParams struct {
	Select *string
	From   *string
	Where  *string
	Order  *string
	Group  *string
}

type QueryResult struct {
	Result []map[string]interface{}
	Error  error
	Query  string
	Xlsx   []byte
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

func (queryObj *Query) Commit() (err error) {
	return queryObj.tx.Commit()
}

func (queryObj *Query) Rollback() (err error) {
	return queryObj.tx.Rollback()
}

func (queryObj *Query) ExecWithArg(arg interface{}, query string) (err error) {
	if queryObj.tx != nil {
		return queryObj.tx.Get(arg, query)
	}
	return queryObj.db.Get(arg, query)
}

func (queryObj *Query) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if queryObj.tx != nil {
		return queryObj.tx.Exec(query, args...)
	}
	return queryObj.db.Exec(query, args...)
}

func (queryObj *Query) Select(dest interface{}, query string) error {
	return queryObj.tx.Select(dest, query)
}

var (
	baseQuery    = `SELECT {{.Select}} FROM {{.From}} {{.Where}} {{.Group}} {{.Order}}`
	baseTemplate = template.Must(template.New("").Parse(baseQuery))
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

// TODO:: Move save log to modelLog
func (queryObj *Query) saveLog(table string, item string, user string, diff map[string]interface{}) {
	if len(diff) > 0 {
		if user == "" {
			log.Error("save log tbl:" + table + " item:" + item + " err: current user is not defined")
			return
		}
		keys := []string{}
		values := []string{}
		for key := range diff {
			keys = append(keys, key)
			values = append(values, ":"+key)
		}
		id := strUtil.NewId()
		query := `INSERT INTO "modelLog" ("id","table","item","user","diff","time") VALUES (:id,:table,:item,:user,:diff,:time)`
		diffByte, err := json.Marshal(diff)
		if err != nil {
			log.Error("save log tbl:"+table+" item:"+item+" err:", err)
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
			log.Error("save log tbl:"+table+" item:"+item+" err:", err)
		}
	}
}

func lowerFirst(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}

func MakeQuery(params *QueryParams) (*string, error) {
	fields := "*"
	var from, where, group, order string
	if params.BaseTable != "" {
		fields = prepareBaseFields(params.BaseTable, params.Select)
	} else if params.Select != nil {
		fields = prepareFields(params.Select)
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

func ExecQuery(q *string, cb ...func(rows *sqlx.Rows)) QueryResult {
	results := QueryResult{Query: *q}
	rows, err := DB.Queryx(*q)
	if err != nil {
		return QueryResult{Error: err}
	}
	defer rows.Close()
	if len(cb) > 0 {
		cb[0](rows)
	}
	for rows.Next() {
		row := make(map[string]interface{})
		err = rows.MapScan(row)
		for k, v := range row {
			//CRUTCH:: Type values get here uuid and arrays (return as string)
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
	//golog.Info(*query)
	if err != nil {
		return QueryResult{Error: err}
	}
	return ExecQuery(query)
}

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
		golog.Error(query)
		golog.Error(err)
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
		default:
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
				diffPub[tag] = tagVal
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
	if len(options) > 0 {
		var id, user string
		withLog := true
		table := lowerFirst(typ.Name())
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
				log.Error("err get info for log model info:", options)
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

		if withLog {
			if table != "" && id != "" {
				go amqp.SendUpdate(amqpURI, table, id, "update", diffPub)
				queryObj.saveLog(table, id, user, diff)
			} else {
				log.Error("missing table or id for save log", options)
			}
		}
	}

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	if queryObj.tx != nil {
		_, err = queryObj.tx.Exec(query)

	} else {
		_, err = queryObj.db.Exec(query)
	}
	if err != nil {
		golog.Error(query)
		golog.Error(err)
		return err
	}
	return nil
}

//InsertStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (queryObj *Query) InsertStructValues(query string, structVal interface{}, options ...interface{}) error {
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
			resultMap[tag] = val.Format(time.RFC3339Nano)
		default:
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
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
		golog.Error(query)
		golog.Error(err)
		return err
	}
	if len(options) > 0 {
		settings, ok := options[0].(map[string]string)
		if !ok {
			golog.Error("err parse opt:", options)
			return nil
		}
		if len(settings) < 2 {
			golog.Error("amqp updates, invalid args:", settings)
			return nil
		}
		go amqp.SendUpdate(amqpURI, settings["table"], settings["id"], "create", resultMap)
	}
	return nil
}

// GetMapFromStruct return map from struct
func GetMapFromStruct(structVal interface{}) map[string]interface{} {
	iVal := reflect.Indirect(reflect.ValueOf(structVal))
	typ := iVal.Type()
	res := make(map[string]interface{})
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		ft := typ.Field(i)
		tag := ft.Tag.Get("db")

		if tag == "" || tag == "-" {
			continue
		}

		if f.IsValid() {
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

// Init open connection to database
func Init() {
	if DB != nil {
		// Exit on dublicate initialization
		return
	}
	var err error
	var db *sqlx.DB
	connected := false
	envURI := os.Getenv("SQL_URI")
	if envURI == "" {
		envURI = DefaultURI
	}
	for !connected {
		db, err = sqlx.Connect("postgres", envURI)
		if err != nil {
			golog.Error("failed connect to database:", envURI, " ", err)
			time.Sleep(20 * time.Second)
			continue
		} else {
			connected = true
		}
	}

	//db.DB.SetMaxIdleConns(10)  // The default is defaultMaxIdleConns (= 2)
	//db.DB.SetMaxOpenConns(100)  // The default is 0 (unlimited)
	//db.DB.SetConnMaxLifetime(3600 * time.Second)  // The default is 0 (connections reused forever)

	golog.Info("success connect to database:", envURI)
	DB = db
	Q, err = NewQuery(false)
	registerSchemaPrepare()
}

//---------------------------------------------------------------------------
// Database schemas defenitions

// SchemaField is definition of sql field
type SchemaField struct {
	Name      string
	Type      string
	IndexType string
	Length    int
	IsNull    bool
	IsUUID    bool
	Default   string
	Key       int
	Auto      int
	checked   bool
}

// SchemaTable is definition of sql table
type SchemaTable struct {
	Name        string
	Fields      []*SchemaField
	initalized  bool
	IDFieldName string
	sqlSelect   string
	SQLFields   []string
	onUpdate    schemaTableUpdateCallback
	LogChanges  bool
	Struct      interface{}
}

type schemaTablePrepareCallback func(table *SchemaTable, event string)
type schemaTableUpdateCallback func(table *SchemaTable, msg interface{})
type schemaTableReg struct {
	table       *SchemaTable
	callbacks   []schemaTableUpdateCallback
	isConnector bool
}
type schemaTableMap map[string]*schemaTableReg

var registerSchema struct {
	sync.RWMutex
	tables schemaTableMap
}

func schemaLogSQL(sql string, err error) {
	if err != nil {
		golog.Error(sql, ": ", err)
	} else {
		golog.Info(sql)
	}
}

func registerSchemaPrepare() {
	registerSchema.Lock()
	defer registerSchema.Unlock()
	if registerSchema.tables == nil {
		return
	}
	for _, reg := range registerSchema.tables {
		err := reg.table.prepare()
		if err != nil {
			golog.Error(`Table "` + reg.table.Name + `" schema check failed: ` + err.Error())
		} else {
			golog.Debug(`Table "` + reg.table.Name + `" schema check successfully`)
		}
	}
	logrus.Info(`Database schema check successfully`)
}

func registerSchemaSetUpdateCallback(tableName string, cb schemaTableUpdateCallback, internal bool) error {
	if !internal {
		registerSchema.Lock()
		defer registerSchema.Unlock()
	}
	if registerSchema.tables == nil {
		return errors.New("Table not registered")
	}
	reg, ok := registerSchema.tables[tableName]
	if !ok {
		return errors.New("Table not registered")
	}
	reg.callbacks = append(reg.callbacks, cb)
	if !reg.isConnector {
		reg.isConnector = true
		queueName := "csx.sql." + tableName
		envName := os.Getenv("CSX_ENV")
		consumerTag := os.Getenv("SERVICE_NAME")
		if envName != "" {
			queueName += "." + envName
		}
		if consumerTag != "" {
			queueName += "." + consumerTag
		} else {
			queueName += "." + *strUtil.NewId()
		}
		go amqp.OnUpdates(registerSchemaOnUpdate, []string{tableName})
	}
	return nil
}

func registerSchemaOnUpdate(d *amqp.Delivery) {
	msg := amqp.Update{}
	err := json.Unmarshal(d.Body, &msg)
	if err != nil {
		log.Error("HandleUpdates", "Error parse json: ", err)
		return
	}
	registerSchema.RLock()
	name := d.RoutingKey
	reg, ok := registerSchema.tables[name]
	if ok {
		for _, cb := range reg.callbacks {
			cb(reg.table, msg)
		}
	}
	registerSchema.RUnlock()
}

// GetSchemaTable get schema table by table name
func GetSchemaTable(table string) (*SchemaTable, bool) {
	registerSchema.RLock()
	schema, ok := registerSchema.tables[table]
	registerSchema.RUnlock()
	if !ok {
		return nil, ok
	}
	return schema.table, ok
}

// NewSchemaField create SchemaTable definition
func NewSchemaField(name, typ string, args ...interface{}) *SchemaField {
	field := new(SchemaField)
	field.Name = name
	field.Type = typ
	ofs := 2
	obj := reflect.ValueOf(field).Elem()
	for index, arg := range args {
		f := obj.Field(index + ofs)
		v := reflect.ValueOf(arg)
		f.Set(v)
	}
	return field
}

// NewSchemaTableFields create SchemaTable definition
func NewSchemaTableFields(name string, fieldsInfo ...*SchemaField) *SchemaTable {
	fields := make([]*SchemaField, len(fieldsInfo))
	for index, field := range fieldsInfo {
		fields[index] = field
	}
	newSchemaTable := SchemaTable{}
	newSchemaTable.Name = name
	newSchemaTable.Fields = fields
	newSchemaTable.register()
	return &newSchemaTable
}

// NewSchemaTable create SchemaTable definition from record structure
func NewSchemaTable(name string, info interface{}, options map[string]interface{}) *SchemaTable {
	var recType reflect.Type
	infoType := reflect.TypeOf(info)
	switch infoType.Kind() {
	case reflect.Slice:
		recType = infoType.Elem()
	case reflect.Array:
		recType = infoType.Elem()
	default:
		recType = infoType
	}

	idFieldName := ""
	fields := make([]*SchemaField, 0)
	if recType.Kind() == reflect.Struct {
		fcnt := recType.NumField()
		for i := 0; i < fcnt; i++ {
			f := recType.Field(i)
			name := f.Tag.Get("db")
			dbFieldTag := f.Tag.Get("dbField")
			if len(name) == 0 || name == "-" || dbFieldTag == "-" {
				continue
			}
			field := new(SchemaField)
			field.Name = name
			field.Type = f.Tag.Get("type")
			field.IndexType = f.Tag.Get("index")
			if len(field.Type) == 0 {
				field.Type = f.Type.Name()
				if len(field.Type) == 0 || field.Type == "string" {
					field.Type = "varchar"
				}
			}
			field.Length, _ = strconv.Atoi(f.Tag.Get("len"))
			field.Key, _ = strconv.Atoi(f.Tag.Get("key"))
			field.Default = f.Tag.Get("def")
			field.Auto, _ = strconv.Atoi(f.Tag.Get("auto"))
			if f.Type.Kind() == reflect.Ptr {
				field.IsNull = true
			}
			field.IsUUID = field.Type == "uuid"
			if (field.Key == 1 || name == "id") && field.IsUUID {
				idFieldName = name
			}
			fields = append(fields, field)
		}
	}

	var onUpdate schemaTableUpdateCallback
	var logChanges bool
	for key, val := range options {
		switch key {
		case "onUpdate":
			if reflect.TypeOf(val).Kind() == reflect.Func {
				onUpdate = val.(func(table *SchemaTable, msg interface{}))
			}
		case "logChanges":
			logChanges = val.(bool)
		}
	}
	newSchemaTable := SchemaTable{}
	newSchemaTable.Name = name
	newSchemaTable.Fields = fields
	newSchemaTable.IDFieldName = idFieldName
	newSchemaTable.onUpdate = onUpdate
	newSchemaTable.LogChanges = logChanges
	newSchemaTable.Struct = info
	newSchemaTable.register()
	return &newSchemaTable
}

func (table *SchemaTable) register() {
	registerSchema.Lock()
	if registerSchema.tables == nil {
		registerSchema.tables = make(schemaTableMap)
	}
	reg, ok := registerSchema.tables[table.Name]
	if !ok {
		reg = &schemaTableReg{table, []schemaTableUpdateCallback{}, false}
		registerSchema.tables[table.Name] = reg
	}
	registerSchema.Unlock()
}

func (field *SchemaField) sql() string {
	sql := `"` + field.Name + `" ` + field.Type
	if field.Length > 0 {
		sql += "(" + strconv.Itoa(field.Length) + ")"
	}
	if !field.IsNull {
		sql += " NOT NULL"
	}
	if len(field.Default) > 0 {
		sql += " DEFAULT " + field.Default
	}
	return sql
}

func (table *SchemaTable) create() error {
	var keys string
	skeys := []*SchemaField{}
	sql := `CREATE TABLE "` + table.Name + `"(`
	for index, field := range table.Fields {
		if index > 0 {
			sql += ", "
		}
		sql += field.sql()
		if (field.Key & 1) != 0 {
			if len(keys) > 0 {
				keys += ","
			}
			keys += field.Name
		}
		if (field.Key&2) != 0 || field.IndexType != "" {
			skeys = append(skeys, field)
		}
	}
	sql += `)`
	_, err := DB.Exec(sql)
	schemaLogSQL(sql, err)
	if err == nil && len(keys) > 0 {
		sql := `ALTER TABLE "` + table.Name + `" ADD PRIMARY KEY (` + keys + `)`
		_, err = DB.Exec(sql)
		schemaLogSQL(sql, err)
	}
	if err == nil && len(skeys) > 0 {
		for _, field := range skeys {
			table.createIndex(field)
		}
	}
	return err
}

func (table *SchemaTable) createIndex(field *SchemaField) error {
	indexType := field.IndexType
	if indexType == "" {
		if field.Type == "json" || field.Type == "jsonb" {
			indexType = "GIN"
		} else {
			indexType = "btree"
		}
	}
	sql := `CREATE INDEX "` + table.Name + "_" + field.Name + `_idx" ON "` + table.Name + `" USING ` + indexType + ` ("` + field.Name + `")`
	_, err := DB.Exec(sql)
	schemaLogSQL(sql, err)
	return err
}

func (table *SchemaTable) alter(cols []string) error {
	for _, name := range cols {
		_, field := table.FindField(name)
		if field != nil {
			field.checked = true
		}
	}
	var err error
	for _, field := range table.Fields {
		if !field.checked {
			sql := `ALTER TABLE "` + table.Name + `" ADD COLUMN ` + field.sql()
			_, err = DB.Exec(sql)
			schemaLogSQL(sql, err)
			if err != nil {
				break
			}
			if (field.Key&2) != 0 || field.IndexType != "" {
				table.createIndex(field)
			}
			field.checked = true
		}
	}
	return err
}

// Prepare check scheme initializing and and create or alter table
func (table *SchemaTable) prepare() error {
	table.initalized = true
	table.SQLFields = []string{}
	table.sqlSelect = "SELECT "
	for index, field := range table.Fields {
		if index > 0 {
			table.sqlSelect += ", "
		}
		fieldName := ""
		if field.Type == "geometry" {
			fieldName = "st_asgeojson(" + field.Name + `) as "` + field.Name + `"`
		} else {
			fieldName = `"` + field.Name + `" `
		}
		table.sqlSelect += fieldName
		table.SQLFields = append(table.SQLFields, fieldName)
	}
	table.sqlSelect += ` FROM "` + table.Name + `"`

	rows, err := DB.Query(`SELECT * FROM "` + table.Name + `" limit 1`)
	if err == nil {
		defer rows.Close()
		var cols []string
		cols, err = rows.Columns()
		if err == nil {
			err = table.alter(cols)
		}
	} else {
		pqErr := err.(*pq.Error)
		code := pqErr.Code
		if code == "42P01" { // not exists
			err = table.create()
		}
	}
	if err == nil && table.onUpdate != nil {
		registerSchemaSetUpdateCallback(table.Name, table.onUpdate, true)
	}
	return err
}

// FindField search field by name
func (table *SchemaTable) FindField(name string) (int, *SchemaField) {
	for index, field := range table.Fields {
		if field.Name == name {
			return index, field
		}
	}
	return -1, nil
}

// OnUpdate init and set callback on table external update event
func (table *SchemaTable) OnUpdate(cb schemaTableUpdateCallback) {
	registerSchemaSetUpdateCallback(table.Name, cb, false)
}

// QueryParams execute  sql query with params
func (table *SchemaTable) QueryParams(recs interface{}, params ...[]string) error {
	var fields, where, order, groupby *[]string

	if len(params) > 0 {
		where = &params[0]
	}
	if len(params) > 1 {
		order = &params[1]
	}
	if len(params) > 2 {
		fields = &params[2]
	} else {
		fields = &table.SQLFields
	}
	if len(params) > 3 && params[3] != nil {
		join := &params[3]
		return table.QueryJoin(recs, fields, where, order, join)
	}
	if len(params) > 4 {
		groupby = &params[4]
	}

	return table.Query(recs, fields, where, order, groupby)
}

// Query execute sql query with params
func (table *SchemaTable) Query(recs interface{}, fields, where, order, group *[]string, args ...interface{}) error {
	qparams := &QueryParams{
		Select: fields,
		From:   &table.Name,
		Where:  where,
		Order:  order,
		Group:  group,
	}

	query, err := MakeQuery(qparams)
	if err = DB.Select(recs, *query, args...); err != nil && err != sql.ErrNoRows {
		log.Error("err: ", err, " query:", *query)
		return err
	}
	return nil
}

// QueryJoin execute sql query with params
func (table *SchemaTable) QueryJoin(recs interface{}, fields, where, order, join *[]string, args ...interface{}) error {
	if join == nil || len(*join) == 0 {
		return errors.New("join arg is empty")
	}
	qparams := &QueryParams{
		Select: fields,
		From:   &(*join)[0],
		Where:  where,
		Order:  order,
	}

	query, err := MakeQuery(qparams)
	if err = DB.Select(recs, *query, args...); err != nil && err != sql.ErrNoRows {
		log.Error(*query)
		fmt.Println(err)
		return err
	}
	return nil
}

// Select execute select sql string
func (table *SchemaTable) Select(recs interface{}, where string, args ...interface{}) error {
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return DB.Select(recs, sql, args...)
}

// Get execute select sql string and return first record
func (table *SchemaTable) Get(rec interface{}, where string, args ...interface{}) error {
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return DB.Get(rec, sql, args...)
}

// Count records with where sql string
func (table *SchemaTable) Count(where string, args ...interface{}) (int, error) {
	sql := `SELECT COUNT(*) FROM "` + table.Name + `"`
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	rec := struct{ Count int }{}
	err := DB.Get(&rec, sql, args...)
	if err == nil {
		return rec.Count, err
	}
	return -1, err
}

// Exists test records exists with where sql string
func (table *SchemaTable) Exists(where string, args ...interface{}) (bool, error) {
	cnt, err := table.Count(where, args...)
	return cnt > 0, err
}

// Insert execute insert sql string
func (table *SchemaTable) Insert(data interface{}, options ...map[string]interface{}) error {
	_, err := table.CheckInsert(data, nil, options...)
	return err
}

func (table *SchemaTable) getIDField(id string, options []map[string]interface{}) (idField, newID string, err error) {
	idField = table.IDFieldName
	newID = id
	if len(options) > 0 {
		option := options[0]
		if option != nil && option["idField"] != nil {
			idField = option["idField"].(string)
		}

		index, field := table.FindField(idField)
		if index == -1 {
			return "", "", errors.New("invalid id field: " + idField)
		}
		if field.Type == "uuid" && id == "" {
			newID = *strUtil.NewId()
		}
	}
	if idField == "" {
		idField = "id"
	}
	return idField, newID, nil
}

// UpsertMultiple execute insert or update sql string
func (table *SchemaTable) UpsertMultiple(data interface{}, where string, options ...map[string]interface{}) (count int64, err error) {
	result, err := table.CheckInsert(data, &where, options...)
	if err != nil {
		return count, err
	}
	count, err = result.RowsAffected()
	if err != nil {
		_, _, err = table.UpdateMultiple(nil, data, where, options...)
	}
	return count, err
}

// Upsert execute insert or update sql string
func (table *SchemaTable) Upsert(id string, data interface{}, options ...map[string]interface{}) error {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return err
	}
	where := `"` + idField + `"='` + id + "'"
	result, err := table.CheckInsert(data, &where, options...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		err = table.Update(id, data, options...)
	}
	return err
}

func (table *SchemaTable) prepareArgsStruct(rec reflect.Value, oldData interface{}, idField string, options ...map[string]interface{}) (args []interface{}, values, fields, itemID string, diff, diffPub map[string]interface{}) {
	diff = make(map[string]interface{})
	diffPub = make(map[string]interface{})
	args = []interface{}{}
	cnt := 0
	recType := rec.Type()
	fcnt := recType.NumField()
	oldRec := reflect.ValueOf(oldData)
	compareWithOldRec := oldRec.IsValid()
	for i := 0; i < fcnt; i++ {
		f := recType.Field(i)
		name := f.Tag.Get("db")
		fType := f.Tag.Get("type")
		if len(name) == 0 || name == "-" {
			continue
		}
		newFld := rec.FieldByName(f.Name)
		var oldFldInt interface{}
		if compareWithOldRec {
			oldFld := oldRec.FieldByName(f.Name)
			if oldFld.IsValid() {
				oldFldInt = oldFld.Interface()
				if oldFldInt == newFld.Interface() {
					continue
				}
			}
		}
		if checkExcludeFields(name, options...) {
			continue
		}
		if cnt > 0 {
			fields += ","
			values += ","
		}
		cnt++
		fields += `"` + name + `"`
		fldInt := newFld.Interface()
		if newFld.IsValid() {
			if fType == "geometry" {
				values += "ST_GeomFromGeoJSON($" + strconv.Itoa(cnt) + ")"
			} else {
				values += "$" + strconv.Itoa(cnt)
			}
			if name == idField {
				itemID = fmt.Sprintf("%v", fldInt)
			}
			if name != idField || oldFldInt == nil {
				diffVal := []interface{}{fldInt}
				if oldFldInt != nil {
					diffVal = append(diffVal, oldFldInt)
				}
				diff[name] = diffVal
				diffPub[name] = fldInt
			}
			args = append(args, fldInt)
		} else {
			values += "NULL"
		}
	}
	excludeFields(diff, diffPub, options...)
	return args, values, fields, itemID, diff, diffPub
}

func (table *SchemaTable) prepareArgsMap(data, oldData map[string]interface{}, idField string, options ...map[string]interface{}) (args []interface{}, values, fields, itemID string, diff, diffPub map[string]interface{}) {
	diff = make(map[string]interface{})
	diffPub = make(map[string]interface{})
	args = []interface{}{}
	cnt := 0
	cntField := 0
	compareWithOldRec := oldData != nil
	for name := range data {
		_, f := table.FindField(name)
		if f == nil {
			logrus.WithFields(logrus.Fields{"field": name}).Warn("invalid schema field")
			continue
		}
		val := data[name]
		oldVal := oldData[name]
		if f.IsUUID {
			if str, ok := val.(string); ok && str == "" {
				val = nil
			}
		}
		if compareWithOldRec && val == oldVal {
			continue
		}
		if checkExcludeFields(name, options...) {
			continue
		}
		if cntField > 0 {
			fields += ","
			values += ","
		}
		fields += `"` + name + `"`
		if name == idField {
			itemID = fmt.Sprintf("%v", val)
		}
		if name != idField || oldVal == nil {
			diffVal := []interface{}{val}
			if oldVal != nil {
				diffVal = append(diffVal, oldVal)
			}
			diff[name] = diffVal
			diffPub[name] = val
		}
		if val != nil {
			cnt++
			argNum := "$" + strconv.Itoa(cnt)
			if f.Type == "geometry" {
				values += "ST_GeomFromGeoJSON(" + argNum + ")"
			} else if f.Type == "jsonb" {
				valJSON, err := json.Marshal(val)
				if err != nil {
					logrus.Error("invalid jsonb value: ", val, " of field: ", name)
				}
				val = valJSON
				values += argNum
			} else {
				values += argNum
			}
			args = append(args, val)
		} else {
			values += "NULL"
		}
		cntField++
	}
	excludeFields(diff, diffPub, options...)
	return args, values, fields, itemID, diff, diffPub
}

func excludeFields(diff map[string]interface{}, diffPub map[string]interface{}, options ...map[string]interface{}) {
	ignoreDiff := []string{}
	if len(options) > 0 {
		option := options[0]
		if option["ignoreDiff"] != nil {
			ignoreDiff = option["ignoreDiff"].([]string)
		}
	}
	for i := 0; i < len(ignoreDiff); i++ {
		field := ignoreDiff[i]
		delete(diff, field)
		delete(diffPub, field)
	}
}

func checkExcludeFields(fieldName string, options ...map[string]interface{}) bool {
	ignoreDiff := []string{}
	if len(options) > 0 {
		option := options[0]
		if option["ignoreDiff"] != nil {
			ignoreDiff = option["ignoreDiff"].([]string)
		}
	}
	for i := 0; i < len(ignoreDiff); i++ {
		f := ignoreDiff[i]
		if f == fieldName {
			return true
		}
	}
	return false
}

// SelectMap select multiple items from db to []map[string]interfaces
func (table *SchemaTable) SelectMap(where string) ([]map[string]interface{}, error) {
	q := table.sqlSelect
	if len(where) > 0 {
		q += " WHERE " + where
	}
	results := make([]map[string]interface{}, 0)
	rows, err := DB.Queryx(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
		results = append(results, row)
	}
	return results, nil
}

// GetMap get one item form db and return as map[string]interfaces
func (table *SchemaTable) GetMap(q string) (map[string]interface{}, error) {
	results, err := table.SelectMap(q)
	if err != nil {
		return nil, err
	}
	lenResults := len(results)
	if lenResults > 1 {
		return nil, errors.New("multiple results for GetMap query not allowed")
	} else if lenResults == 0 {
		return nil, nil
	} else {
		return results[0], nil
	}
}

// CheckInsert execute insert sql string if not exist where expression
func (table *SchemaTable) CheckInsert(data interface{}, where *string, options ...map[string]interface{}) (sql.Result, error) {
	q := Query{db: DB}
	if len(options) > 0 {
		option := options[0]
		if _, ok := option["queryObj"]; ok {
			q = option["queryObj"].(Query)
		}
	}

	var diff, diffPub map[string]interface{}
	idField, _, err := table.getIDField("", options)
	if err != nil {
		return nil, err
	}
	rec := reflect.ValueOf(data)
	recType := rec.Type()

	if recType.Kind() == reflect.Ptr {
		rec = reflect.Indirect(rec)
		recType = rec.Type()
	}
	var fields, values, itemID string
	var args []interface{}
	if recType.Kind() == reflect.Map {
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, errors.New("element must be a map[string]interface")
		}
		args, values, fields, itemID, diff, diffPub = table.prepareArgsMap(dataMap, nil, idField)
	} else if recType.Kind() == reflect.Struct {
		args, values, fields, itemID, diff, diffPub = table.prepareArgsStruct(rec, nil, idField)
	} else {
		return nil, errors.New("element must be struct or map[string]interface")
	}

	sql := `INSERT INTO "` + table.Name + `" (` + fields + `)`
	if where == nil {
		sql += ` VALUES (` + values + `)`
	} else {
		sql += ` SELECT ` + values + ` WHERE NOT EXISTS(SELECT * FROM "` + table.Name + `" WHERE ` + *where + `)`
	}

	res, err := q.Exec(sql, args...)
	if err == nil {
		go amqp.SendUpdate(amqpURI, table.Name, itemID, "create", diffPub)
		table.SaveLog(itemID, diff, options)
	}
	return res, err
}

// SaveLog save model logs to database
func (table *SchemaTable) SaveLog(itemID string, diff map[string]interface{}, options []map[string]interface{}) error {
	withLog := table.LogChanges
	if len(options) > 0 {
		option := options[0]
		if option["withLog"] != nil {
			withLog = option["withLog"].(bool)
		}
		if withLog {
			id := itemID
			user := ""
			if option["idField"] != nil {
				id = option["idField"].(string)
			}
			if option["user"] != nil {
				user = option["user"].(string)
			}
			query, err := NewQuery(false)
			if err != nil {
				return err
			}
			query.saveLog(table.Name, id, user, diff)
		}
	}
	return nil
}

// UpdateMultiple execute update sql string
func (table *SchemaTable) UpdateMultiple(oldData, data interface{}, where string, options ...map[string]interface{}) (diff, diffPub map[string]interface{}, err error) {
	q := Query{db: DB}
	if len(options) > 0 {
		option := options[0]
		if _, ok := option["queryObj"]; ok {
			q = option["queryObj"].(Query)
		}
	}

	rec := reflect.ValueOf(data)
	recType := rec.Type()

	if recType.Kind() == reflect.Ptr {
		rec = reflect.Indirect(rec)
		recType = rec.Type()
	}
	var fields, values string
	var args []interface{}
	if recType.Kind() == reflect.Map {
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, nil, errors.New("data must be a map[string]interface")
		}
		oldDataMap, ok := oldData.(map[string]interface{})
		if !ok {
			return nil, nil, errors.New("oldData must be a map[string]interface")
		}
		args, values, fields, _, diff, diffPub = table.prepareArgsMap(dataMap, oldDataMap, "", options...)
	} else if recType.Kind() == reflect.Struct {
		oldRec := reflect.ValueOf(oldData)
		oldRecType := oldRec.Type()

		if oldRecType.Kind() == reflect.Ptr {
			oldRec = reflect.Indirect(oldRec)
			oldRecType = oldRec.Type()
		}
		//if oldData is map and rec is struct - convert rec to map
		if oldRecType.Kind() == reflect.Map {
			str := GetMapFromStruct(data)
			args, values, fields, _, diff, diffPub = table.prepareArgsMap(str, oldData.(map[string]interface{}), "", options...)
		} else {
			args, values, fields, _, diff, diffPub = table.prepareArgsStruct(rec, oldData, "", options...)
		}
	} else {
		return nil, nil, errors.New("element must be struct or map[string]interface")
	}

	var sql string
	lenDiff := len(diff)
	if lenDiff == 1 {
		sql = `UPDATE "` + table.Name + `" SET ` + fields + ` = ` + values + ` WHERE ` + where
	} else if lenDiff > 0 {
		sql = `UPDATE "` + table.Name + `" SET (` + fields + `) = (` + values + `) WHERE ` + where
	}
	if lenDiff > 0 {
		_, err = q.Exec(sql, args...)
	}
	return diff, diffPub, err
}

// Update update one item by id
func (table *SchemaTable) Update(id string, data interface{}, options ...map[string]interface{}) error {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return err
	}
	var oldData map[string]interface{}
	if len(options) > 0 {
		if _, ok := options[0]["oldData"]; ok {
			oldData = options[0]["oldData"].(map[string]interface{})
		}
	}
	where := `"` + idField + `"='` + id + `'`
	if oldData == nil {
		oldData, err = table.GetMap(where)
		if err != nil {
			return err
		}
	}
	diff, diffPub, err := table.UpdateMultiple(oldData, data, where, options...)
	if err == nil && len(diff) > 0 {
		go amqp.SendUpdate(amqpURI, table.Name, id, "update", diffPub)
		table.SaveLog(id, diff, options)
	}
	return err
}

// DeleteMultiple  delete all records with where sql string
func (table *SchemaTable) DeleteMultiple(where string, options ...map[string]interface{}) (int, error) {
	q := Query{db: DB}
	if len(options) > 0 {
		option := options[0]
		if _, ok := option["queryObj"]; ok {
			q = option["queryObj"].(Query)
		}
	}

	sql := `DELETE FROM "` + table.Name + `"`
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	var args []interface{}
	if len(options) > 0 {
		option := options[0]
		if option["args"] != nil {
			args = option["args"].([]interface{})
		}
	}
	ret, err := q.Exec(sql, args...)
	if err == nil {
		rows, err := ret.RowsAffected()
		return int(rows), err
	}
	return -1, err
}

// Delete delete one record by id
func (table *SchemaTable) Delete(id string, options ...map[string]interface{}) (int, error) {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return 0, err
	}
	count, err := table.DeleteMultiple(idField+`='`+id+`'`, options...)
	if count != 1 {
		err = errors.New("record not found")
	}
	if err == nil {
		var data interface{}
		if len(options) > 0 {
			data = options[0]["data"]
		}
		if data == nil {
			data = struct{}{}
		}
		go amqp.SendUpdate(amqpURI, table.Name, id, "delete", data)
		//table.SaveLog(id, diff, options)
	}
	return count, err
}

//---------------------------------------------------------------------------

// DataStore store local data
type DataStore struct {
	sync.RWMutex
	name        string
	propID      string
	propIndexes []string
	load        DataStoreLoadProc
	items       map[string]interface{}
	indexes     map[string]map[interface{}]interface{}
}

// DataStoreLoadProc load store items function
type DataStoreLoadProc func(id *string) (interface{}, error)

// NewDataStore create DataStore
func NewDataStore(storeName, propID string, indexes []string, load DataStoreLoadProc) *DataStore {
	return &DataStore{
		name:        storeName,
		propID:      propID,
		propIndexes: indexes,
		load:        load,
	}
}

// Find data store search element by id
func (store *DataStore) Find(args ...interface{}) (result interface{}, ok bool) {
	var propID string
	var propVal interface{}
	argcnt := len(args)
	if argcnt > 1 {
		propVal = args[0]
		propIDValue := reflect.Indirect(reflect.ValueOf(args[1]))
		if propIDValue.IsValid() {
			propID = propIDValue.String()
		}
		if propID == "" {
			logrus.Error("store '" + store.name + "' invalid index id")
			return nil, false
		}
	} else if argcnt > 0 {
		propVal = args[0]
	}
	if propVal == nil {
		return nil, false
	}

	store.RLock()
	if propID == "" {
		val := reflect.Indirect(reflect.ValueOf(propVal))
		if val.IsValid() {
			id := val.String()
			result, ok = store.items[id]
			if !ok {
				logrus.Warn("store '"+store.name+"' not found: ", id)
			}
		}
	} else {
		index, isIndex := store.indexes[propID]
		if isIndex {
			result, ok = index[propVal]
		} else {
			logrus.Error("store '"+store.name+"' not found index: ", propID)
		}
	}
	store.RUnlock()
	return result, ok
}

// Load data store from source
func (store *DataStore) Load() {
	items, err := store.load(nil)
	if err != nil {
		panic("store '" + store.name + "' load " + err.Error())
	}
	itemsVal := reflect.ValueOf(items)
	cnt := itemsVal.Len()
	store.Lock()
	cntIndexes := len(store.propIndexes)
	store.items = make(map[string]interface{})
	store.indexes = make(map[string]map[interface{}]interface{})
	for i := 0; i < cntIndexes; i++ {
		store.indexes[store.propIndexes[i]] = make(map[interface{}]interface{})
	}
	for i := 0; i < cnt; i++ {
		itemVal := itemsVal.Index(i)
		itemPtr := itemVal.Addr().Interface()
		itemID := itemVal.FieldByName(store.propID).String()
		store.items[itemID] = itemPtr
		if cntIndexes > 0 {
			store.updateIndexies(itemVal, itemPtr, nil)
		}
	}
	store.Unlock()
}

// Range iterate datastore map and run callback
func (store *DataStore) Range(cb func(key, val interface{}) bool) {
	for key, val := range store.items {
		res := cb(key, val)
		if !res {
			break
		}
	}
}

// Update data store by data
func (store *DataStore) Update(cmd, id, data string) {
	var itemPtr interface{}
	var itemVal reflect.Value
	var oldValues []interface{}
	cntIndexes := len(store.propIndexes)
	isUpdate := cmd == "update"
	if isUpdate {
		var ok bool
		store.RLock()
		itemPtr, ok = store.items[id]
		store.RUnlock()
		if !ok || itemPtr == nil {
			logrus.Error("store '"+store.name+"' not found: ", id)
			return
		}
	}
	if itemPtr == nil {
		items, err := store.load(&id)
		if err != nil {
			logrus.Error("store '"+store.name+"' load: ", id, " ", err)
		} else {
			itemsVal := reflect.ValueOf(items)
			if itemsVal.Len() == 0 {
				logrus.Error("store '"+store.name+"' not found: ", id)
			} else {
				itemVal = itemsVal.Index(0)
				itemPtr = itemVal.Addr().Interface()
			}
		}
	} else {
		var err error
		if cntIndexes > 0 {
			itemVal = reflect.ValueOf(itemPtr)
			if itemVal.Kind() == reflect.Ptr {
				itemVal = reflect.Indirect(itemVal)
			}
			oldValues = store.readIndexies(itemVal, itemPtr)
		}
		writeItem(itemPtr, func(locked bool) {
			if !locked {
				// Делаем полную копию, если у элемента нет методов блокировки (можно использовать только для чтения)
				itemPtr = deepcopier.Copy(itemPtr)
				itemVal = reflect.Indirect(reflect.ValueOf(itemPtr))
			}
			err = json.Unmarshal([]byte(data), itemPtr)
		})
		if err != nil {
			logrus.Error("store '"+store.name+"' unmarshal: ", id, " ", err)
			itemPtr = nil
		}
	}
	if itemPtr != nil {
		store.Lock()
		store.items[id] = itemPtr
		if cntIndexes > 0 {
			store.updateIndexies(itemVal, itemPtr, oldValues)
		}
		store.Unlock()
		logrus.Warn("store '"+store.name+"' update: ", id, " ", data)
	}
}

func (store *DataStore) readIndexies(itemVal reflect.Value, itemPtr interface{}) []interface{} {
	cnt := len(store.propIndexes)
	result := make([]interface{}, cnt)
	readItem(itemPtr, func() {
		for i := 0; i < cnt; i++ {
			propID := store.propIndexes[i]
			prop := reflect.Indirect(itemVal.FieldByName(propID))
			if !prop.IsValid() {
				continue
			}
			result[i] = prop.Interface()
		}
	})
	return result
}

func (store *DataStore) updateIndexies(itemVal reflect.Value, itemPtr interface{}, oldValues []interface{}) {
	cnt := len(store.propIndexes)
	for i := 0; i < cnt; i++ {
		propID := store.propIndexes[i]
		prop := reflect.Indirect(itemVal.FieldByName(propID))
		index, isIndex := store.indexes[propID]
		if !isIndex || !prop.IsValid() {
			continue
		}
		propVal := prop.Interface()
		if oldValues != nil {
			oldVal := oldValues[i]
			if oldVal != propVal && oldVal != nil {
				delete(index, oldVal)
			}
		}
		if propVal != nil {
			index[propVal] = itemPtr
		}
	}
}

func writeItem(itemPtr interface{}, cb func(locked bool)) {
	itemVal := reflect.ValueOf(itemPtr)
	lock := itemVal.MethodByName("Lock")
	locked := lock.IsValid()
	if locked {
		lock.Call([]reflect.Value{})
	}
	cb(locked)
	if locked {
		unlock := itemVal.MethodByName("Unlock")
		unlock.Call([]reflect.Value{})
	}
}

func readItem(itemPtr interface{}, cb func()) {
	itemVal := reflect.ValueOf(itemPtr)
	lock := itemVal.MethodByName("RLock")
	if lock.IsValid() {
		lock.Call([]reflect.Value{})
	}
	cb()
	if lock.IsValid() {
		unlock := itemVal.MethodByName("RUnlock")
		unlock.Call([]reflect.Value{})
	}
}

//---------------------------------------------------------------------------
