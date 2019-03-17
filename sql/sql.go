package sql

import (
	"os"
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
