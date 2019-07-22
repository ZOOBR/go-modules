package sql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/golog"
	_ "github.com/lib/pq"
	"github.com/prometheus/common/log"
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

func (this *Query) saveLog(table string, item string, user string, data interface{}, diff map[string]interface{}) {
	if len(diff) > 0 {
		keys := []string{}
		values := []string{}
		for key := range diff {
			keys = append(keys, key)
			values = append(values, ":"+key)
		}
		id := strUtil.NewId()
		query := `INSERT INTO "modelLog" ("id","table","item","user","diff","data","time") VALUES (:id,:table,:item,:user,:diff,:data,:time)`
		diffByte, err := json.Marshal(diff)
		if err != nil {
			log.Error("save log tbl:"+table+" item:"+item+" err:", err)
			return
		}
		dataByte, err := json.Marshal(data)
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
			"data":  string(dataByte),
			"time":  now,
		}

		if this.tx != nil {
			_, err = this.tx.NamedExec(query, logItem)
		} else {
			_, err = this.db.NamedExec(query, logItem)
		}
		if err != nil {
			log.Error("save log tbl:"+table+" item:"+item+" err:", err)
		}
		msg := map[string]interface{}{
			"id":   item,
			"cmd":  "update",
			"data": string(diffByte),
		}
		msgJSON, err := json.Marshal(msg)
		if err == nil {
			amqp.Publish(amqpURI, "csx.updates", "direct", table, string(msgJSON), false)
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

func ExecQuery(q *string) QueryResult {
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

func (this *Query) Delete(query string) (err error) {
	if this.tx != nil {
		_, err = this.tx.Exec(query)
	} else {
		_, err = this.db.Exec(query)
	}
	return err
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (this *Query) SetStructValues(query string, structVal interface{}, isUpdate ...bool) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)
	diff := make(map[string]interface{})

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
				tagWrite := typ.Field(i).Tag.Get("dbField")
				if tagWrite == "-" {
					continue
				} else if tagWrite != "" {
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
					oldMap[tag] = val.Format(time.RFC3339)
				case []string:
					jsonArray, _ := json.Marshal(val)
					oldMap[tag] = jsonArray
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
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" {
			continue
		} else if tagWrite != "" {
			tag = tagWrite
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
			resultMap[tag] = val.Format(time.RFC3339)
			updV = resultMap[tag].(string)
		case []string:
			jsonArray, _ := json.Marshal(val)
			resultMap[tag] = string(jsonArray)
			updV = string(jsonArray)
		default:
			continue
		}

		if len(isUpdate) > 0 && isUpdate[0] {
			if oldMap[tag] != resultMap[tag] {
				prepFields = append(prepFields, `"`+tag+`"`)
				prepValues = append(prepValues, "'"+updV+"'")
				diff[tag] = f.Interface()
			}
		} else {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, ":"+tag)
		}
	}

	var prepText string
	if len(isUpdate) > 0 && isUpdate[0] {
		//fmt.Fprintln(os.Stdout, diff)
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
func (this *Query) UpdateStructValues(query string, structVal interface{}, options ...interface{}) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)
	diff := make(map[string]interface{})

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
			tagWrite := typ.Field(i).Tag.Get("dbField")
			if tagWrite == "-" || tag == "-" {
				continue
			} else if tagWrite != "" {
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
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" {
			continue
		} else if tagWrite != "" {
			tag = tagWrite
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
			resultMap[tag] = val.Format(time.RFC3339)
			updV = resultMap[tag].(string)
		default:
			continue
		}

		if oldMap[tag] != resultMap[tag] {
			diff[tag] = f.Interface()
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
	if len(options) > 0 {
		opts, ok := options[0].([]string)
		if !ok {
			log.Error("err get info for log model info:", options)
		} else {
			// 0 - id
			// 1 - user
			// 2 - table name
			table := lowerFirst(typ.Name())
			if len(opts) > 2 {
				table = opts[2]
			}
			id := opts[0]
			user := opts[1]
			this.saveLog(table, id, user, oldMap, diff)
		}
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
		if tag == "-" {
			continue
		}
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" {
			continue
		} else if tagWrite != "" {
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

func MakeQueryFromReq(req map[string]string, extConditions ...string) string {
	r := strings.NewReplacer("create ", "", "insert ", "", " set ", "", "drop ", "", "alter ", "", "update ", "", "delete ", "", "CREATE ", "", "INSERT ", "", " SET ", "", "DROP ", "", "ALTER ", "", "UPDATE ", "", "DELETE ", "")
	limit := req["limit"]
	offset := req["offset"]
	if limit == "" {
		limit = "1000"
	}
	if offset == "" {
		offset = "0"
	}
	newQ := ""
	fields, okF := req["fields"]
	join, okJ := req["join"]
	table, okT := req["table"]
	if okF && okT {
		newQ += `SELECT ` + fields + ` FROM "` + table + `" `
	} else if okF && okJ {
		newQ += `SELECT ` + fields + ` FROM ` + join + ` `
	}
	where := ""
	if len(extConditions) > 0 {
		where += extConditions[0]
	}
	orderby := ""
	q := "LIMIT " + limit + " OFFSET " + offset
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
				fieldParts := strings.Split(f[0], ".")
				var field string
				if len(fieldParts) < 2 {
					field = `"` + f[0] + `"`
				} else {
					field = f[0]
				}
				switch keyValue[0] {
				case "text":
					where += field + ` ILIKE '%` + v + "%'"
				case "date":
					rangeDates := strings.Split(v, "_")
					beginDate, err := strconv.ParseInt(rangeDates[0], 10, 64)
					if err != nil {
						continue
					}
					tmBegin := time.Unix(beginDate/1000, 0).UTC().Format("2006-01-02 15:04:05")
					where += field + ` >= '` + tmBegin + "'"
					if len(rangeDates) > 1 {
						endDate, err := strconv.ParseInt(rangeDates[1], 10, 64)
						if err != nil {
							continue
						}
						tmEnd := time.Unix(endDate/1000, 0).UTC().Format("2006-01-02 15:04:05")
						where += ` AND ` + field + ` <= '` + tmEnd + "'"
					}
				case "select":
					where += field + ` = '` + v + "'"
				case "is":
					where += field + ` IS ` + v
				}
			}
		}
	}
	if where != "" {
		where = "WHERE " + where
	}
	if val, ok := req["sort"]; ok && val != "" {
		sortParams := strings.Split(val, "-")
		orderby += `ORDER BY "` + sortParams[0] + `" ` + sortParams[1]
		if v, o := req["nulls"]; o && v != "" {
			orderby += ` NULLS ` + v
		}
	}
	fullReq := r.Replace(newQ + " " + where + " " + orderby + " " + q)
	// fmt.Println(fullReq)
	return fullReq
}

//Init open connection to database
func Init() {
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
	golog.Info("success connect to database:", envURI)
	DB = db
	Q, err = NewQuery(false)
}
