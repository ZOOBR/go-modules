package dbconnector

import (
	"flag"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/golog"
	"github.com/kataras/iris"
	"github.com/kataras/iris/sessions"
	"github.com/kataras/iris/sessions/sessiondb/redis"
	"github.com/kataras/iris/sessions/sessiondb/redis/service"
)

var (
	// sqlURI            = flag.String("sql-uri", "root:@/project?parseTime=true", "SQL connection string")
	sqlURI            = flag.String("sql-uri", "user=postgres dbname=csx sslmode=disable", "SQL connection string")
	redisURI          = flag.String("redis-uri", "127.0.0.1:6379", "Redis URI")
	redisDB           = flag.String("redis-db", "2", "Redis DB number")
	reconnectInterval = flag.Duration("reconnect-interval", 20000, "Reconnect interval")
)

//DB is pointer to DB connection
var DB *sqlx.DB

//RedisDB is pointer to RedisDB connection
var RedisDB *redis.Database

//Sess is pointer to redis session
var Sess *sessions.Sessions

//Init open connection to database
func Init() {
	// db, err := sqlx.Connect("mysql", *sqlURI)
	db, err := sqlx.Connect("postgres", *sqlURI)
	if err != nil {
		golog.Error("failed connect to database:", *sqlURI, " ", err)
	} else {
		golog.Info("success connect to database:", *sqlURI)
	}
	DB = db

	// for {
	redisDb := redis.New(service.Config{
		Network:     "tcp",
		Addr:        *redisURI,
		Password:    "",
		Database:    *redisDB,
		MaxIdle:     0,
		MaxActive:   0,
		IdleTimeout: time.Duration(5) * time.Minute,
		Prefix:      ""}) // optionally configure the bridge between your redis server

	if redisDb == nil {
		golog.Error("error connect to redis:" + *redisURI)
		time.Sleep(*reconnectInterval)
		// continue
	} else {
		golog.Info("success connect to redis:" + *redisURI)
	}

	// close connection when control+C/cmd+C
	iris.RegisterOnInterrupt(func() {
		redisDb.Close()
	})

	// defer redisDb.Close() // close the database connection if application errored.

	sess := sessions.New(sessions.Config{
		Cookie:       "2yPuQFFzc4lF1UUK9NQzwsl3PVbslQcf",
		Expires:      1440 * time.Minute, // <=0 means unlimited life. Defaults to 0.
		AllowReclaim: true,
	})

	if sess == nil {
		golog.Print("failed use session")
	}

	sess.UseDatabase(redisDb)

	Sess = sess
	RedisDB = redisDb
	// }
}
