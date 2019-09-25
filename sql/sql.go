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
	"github.com/lib/pq"
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
		go amqp.SendUpdate(amqpURI, table, item, "update", diff)
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
		tag := typ.Field(i).Tag.Get("db")
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		} else if tagWrite != "" {
			tag = tagWrite
		}
		if !f.IsValid() {
			if isPointer && oldMap[tag] != nil {
				resultMap[tag] = nil
				diff[tag] = nil
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
			resultMap[tag] = val.Format(time.RFC3339)
			updV = resultMap[tag].(string)
		default:
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
			updV = string(valJSON)
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
				this.saveLog(table, id, user, oldMap, diff)
			} else {
				log.Error("missing table or id for save log", options)
			}
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
func (this *Query) InsertStructValues(query string, structVal interface{}, options ...interface{}) error {
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
				case "lte":
					where += field + ` <= '` + v + "'"
				case "lten":
					where += `(` + field + ` IS NULL OR ` + field + ` <= '` + v + "')"
				case "gte":
					where += field + ` >= '` + v + "'"
				case "gten":
					where += `(` + field + ` IS NULL OR ` + field + ` >= '` + v + "')"
				case "lt":
					where += field + ` < '` + v + "'"
				case "gt":
					where += field + ` > '` + v + "'"
				case "is":
					where += field + ` IS ` + v
				case "in":
					where += field + ` IN (` + v + `)`
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
	if val, ok := req["sort"]; ok && val != "" {
		sortParams := strings.Split(val, "-")
		orderby += "ORDER BY "
		if _, err := strconv.ParseInt(sortParams[0], 10, 16); err == nil {
			orderby += sortParams[0]
		} else {
			orderby += `"` + sortParams[0] + `"`
		}
		if len(sortParams) > 1 {
			orderby += " " + sortParams[1]
		}
		if v, o := req["nulls"]; o && v != "" {
			orderby += ` NULLS ` + v
		}
	}
	fullReq := r.Replace(newQ + " " + where + " " + groupby + " " + orderby + " " + q)
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

//---------------------------------------------------------------------------
// Database schemas defenitions

// SchemaField is definition of sql field
type SchemaField struct {
	Name    string
	Type    string
	Length  int
	Key     bool
	checked bool
}

// SchemaTable is definition of sql table
type SchemaTable struct {
	Name       string
	Fields     []*SchemaField
	initalized bool
	err        error
	sqlSelect  string
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

// NewSchemaTable create SchemaTable definition
func NewSchemaTable(name string, fieldsInfo ...*SchemaField) SchemaTable {
	fields := make([]*SchemaField, len(fieldsInfo))
	for index, field := range fieldsInfo {
		fields[index] = field
	}
	return SchemaTable{name, fields, false, nil, ""}
}

func (table *SchemaTable) create() error {
	var keys string
	sql := `CREATE TABLE "` + table.Name + `"(`
	for index, field := range table.Fields {
		if index > 0 {
			sql += ", "
		}
		sql += `"` + field.Name + `" ` + field.Type
		if field.Length > 0 {
			sql += "(" + strconv.Itoa(field.Length) + ")"
		}
		if field.Key {
			if len(keys) > 0 {
				keys += ","
			}
			keys += field.Name
		}
	}
	sql += `)`
	_, err := DB.Exec(sql)
	if err == nil && len(keys) > 0 {
		_, err = DB.Exec(`ALTER TABLE "` + table.Name + `" ADD PRIMARY KEY (` + keys + `)`)
	}
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
			sql := `ALTER TABLE "` + table.Name + `" ADD COLUMN "` + field.Name + `" ` + field.Type
			if field.Length > 0 {
				sql += "(" + strconv.Itoa(field.Length) + ")"
			}
			_, err = DB.Exec(sql)
			if err != nil {
				break
			}
			field.checked = true
		}
	}
	return err
}

// Prepare check scheme initializing and and create or alter table
func (table *SchemaTable) Prepare() error {
	if table.initalized {
		return table.err
	}
	table.initalized = true

	table.sqlSelect = "SELECT "
	for index, field := range table.Fields {
		if index > 0 {
			table.sqlSelect += ", "
		}
		table.sqlSelect += `"` + field.Name + `" `
	}
	table.sqlSelect += ` FROM "` + table.Name + `"`

	rows, err := DB.Query(`SELECT * FROM "` + table.Name + `" limit 1`)
	if err == nil {
		cols, err := rows.Columns()
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
	table.err = err
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

// Select execute select sql string
func (table *SchemaTable) Select(recs interface{}, where string, args ...interface{}) error {
	err := table.Prepare()
	if err != nil {
		return err
	}
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return DB.Select(recs, sql, args...)
}

// Get execute select sql string and return first record
func (table *SchemaTable) Get(rec interface{}, where string, args ...interface{}) error {
	err := table.Prepare()
	if err != nil {
		return err
	}
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return DB.Get(rec, sql, args...)
}

// Insert execute insert sql string (not worked)
func (table *SchemaTable) Insert(rec interface{}) error {
	err := table.Prepare()
	if err != nil {
		return err
	}
	var args = map[string]interface{}{}
	var fields, values string
	cnt := 0
	info := reflect.ValueOf(rec)
	typ := info.Type()
	for i := 0; i < info.NumField(); i++ {
		f := typ.Field(i)
		name := f.Tag.Get("db")
		if len(name) == 0 {
			continue
		}
		/*
			_, field := table.FindField(name)
			if field == nil {
				continue
			}
		*/
		if cnt > 0 {
			fields += ","
			values += ","
		}
		cnt++
		fields += `"` + name + `"`
		// values += "$" + strconv.Itoa(cnt)
		// args[name] = reflect.ValueOf(f)
		values += ":" + name
		args[name] = info.Index(i).Interface()
	}

	sql := `INSERT INTO "` + table.Name + `" (` + fields + `) VALUES (` + values + `)`
	_, err = DB.NamedExec(sql, args)
	return err
}

//---------------------------------------------------------------------------
