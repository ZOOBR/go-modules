package session

import (
	"os"
	"strconv"
	"time"

	"github.com/kataras/golog"
	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/sessions"
	"github.com/kataras/iris/v12/sessions/sessiondb/redis"
)

const (
	//DefaultURI default SQL connection string
	DefaultURI = "127.0.0.1:6379"
	//DefaultDB is default Redis DB
	DefaultDB = "10"
	//DefaultPass is default Redis password
	DefaultPassword = ""
	//ReconnectInterval is default reconnect interval
	ReconnectInterval = 20000
	//SessionDefaultDuration - default duration of session in minutes
	SessionDefaultDuration = 1440 //in minutes
)

//RedisDB is pointer to RedisDB connection
var RedisDB *redis.Database

//Sess is pointer to redis session
var Sess *sessions.Sessions

var EnvSessDuration int

//Init open connection to database
func Init() {

	envURI := os.Getenv("REDIS_URI")
	if envURI == "" {
		envURI = DefaultURI
	}
	envDB := os.Getenv("REDIS_DB")
	if envDB == "" {
		envDB = DefaultDB
	}
	envPass := os.Getenv("REDIS_PASSWORD")
	if envPass == "" {
		envPass = DefaultPassword
	}

	envDuration := os.Getenv("REDIS_SESS_DURATION")
	if envDuration == "" {
		EnvSessDuration = SessionDefaultDuration
	} else {
		duration, err := strconv.ParseInt(envDuration, 10, 32)
		if err != nil {
			EnvSessDuration = SessionDefaultDuration
		} else {
			EnvSessDuration = int(duration)
		}
	}

	redisDb := redis.New(redis.Config{
		Network:  "tcp",
		Addr:     envURI,
		Password: envPass,
		Database: envDB,
		// MaxIdle:     0,
		MaxActive: 0,
		Timeout:   time.Duration(5) * time.Minute,
		Prefix:    "",
		Delim:     "_"}) // optionally configure the bridge between your redis server

	if redisDb == nil {
		golog.Error("error connect to redis:" + envURI)
		time.Sleep(ReconnectInterval)
		// continue
	} else {
		golog.Info("success connect to redis:" + envURI)
	}

	// close connection when control+C/cmd+C
	iris.RegisterOnInterrupt(func() {
		redisDb.Close()
	})

	// defer redisDb.Close() // close the database connection if application errored.

	sess := sessions.New(sessions.Config{
		Cookie:       "2yPuQFFzc4lF1UUK9NQzwsl3PVbslQcf",
		Expires:      time.Duration(EnvSessDuration) * time.Minute, // <=0 means unlimited life. Defaults to 0.
		AllowReclaim: true,
	})

	if sess == nil {
		golog.Print("failed use session")
	}

	sess.UseDatabase(redisDb)

	Sess = sess
	RedisDB = redisDb
}
