package sql

import (
	"os"
	"reflect"
	"strings"

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
var DB *sqlx.DB

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
func SetStructValues(query string, structVal interface{}) error {
	resultMap := make(map[string]interface{})
	prepText := " "
	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		tag := typ.Field(i).Tag.Get("db")
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
		prepText += tag + "=:" + tag
		if i+1 != iVal.NumField() {
			prepText += ", "
		}
	}

	prepText += " "
	query = strings.Replace(query, "?", prepText, -1)
	_, err := DB.NamedExec(query, resultMap)
	if err != nil {
		return err
	}
	return nil
}

//Init open connection to database
func Init() {

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
}
