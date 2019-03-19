package sql

import (
	"fmt"
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
	_, err := DB.NamedExec(*query, values)
	if err != nil {
		return err
	}
	return nil
}

//SetStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func SetStructValues(query *string, structVal interface{}) error {

	val := reflect.ValueOf(structVal) // could be any underlying type

	// if its a pointer, resolve its value
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}

	fields := reflect.TypeOf(val)
	values := reflect.ValueOf(val)

	num := fields.NumField()
	resultMap := make(map[string]interface{})

	prepText := " "
	for i := 0; i < num; i++ {
		field := fields.Field(i)
		value := values.Field(i)
		fmt.Print("Type:", field.Type, ",", field.Name, "=", value, "\n")

		switch value.Kind() {
		case reflect.String:
			v := value.String()
			resultMap[field.Name] = v
		//int
		case reflect.Int:
			resultMap[field.Name] = value.Int()
		case reflect.Int8:
			resultMap[field.Name] = value.Int()
		case reflect.Int16:
			resultMap[field.Name] = value.Int()
		case reflect.Int32:
			resultMap[field.Name] = value.Int()
		case reflect.Int64:
			resultMap[field.Name] = value.Int()
		//uint
		case reflect.Uint8:
			resultMap[field.Name] = value.Uint()
		case reflect.Uint16:
			resultMap[field.Name] = value.Uint()
		case reflect.Uint32:
			resultMap[field.Name] = value.Uint()
		case reflect.Uint64:
			resultMap[field.Name] = value.Uint()
		//float
		case reflect.Float32:
			resultMap[field.Name] = value.Float()
		case reflect.Float64:
			resultMap[field.Name] = value.Float()
		default:
			fmt.Print("Not support type of struct")
			continue
		}
		prepText += field.Name + "=:" + field.Name
	}

	prepText += " "
	strings.Replace(*query, "?", prepText, -1)
	_, err := DB.NamedExec(*query, &resultMap)
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
