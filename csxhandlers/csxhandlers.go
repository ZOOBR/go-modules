package csxhandlers

import (
	"net/http"
	"os"
	"reflect"
	"strings"

	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/csxhttp"
	"gitlab.com/battler/modules/csxsession"
	"gitlab.com/battler/modules/csxstrings"
)

var (
	sessionsStore *csxsession.CsxStore
	SessionKey    string
)

type route struct {
	method     string
	controller string
	name       string
	cb         func(ctx *csxhttp.Context) error
}

var (
	App           *echo.Echo
	controllers   = []interface{}{}
	initCallbacks = []func(){}
	routes        = []route{}
)

func initRoutes() {
	for i := 0; i < len(routes); i++ {
		route := routes[i]
		var routeName string
		if route.controller != "" {
			if routeName != "" {
				routeName = "/" + route.controller + "/" + route.name
			} else {
				routeName = "/" + route.controller
			}
		} else {
			routeName = "/" + route.name
		}

		logrus.Debug("init method: " + routeName)
		var handle func(string, echo.HandlerFunc, ...echo.MiddlewareFunc) *echo.Route
		switch route.method {
		case "GET":
			handle = App.GET
		case "POST":
			handle = App.POST
		case "PUT":
			handle = App.PUT
		case "DELETE":
			handle = App.DELETE
		case "OPTIONS":
			handle = App.OPTIONS
		case "HEAD":
			handle = App.HEAD
		}
		handle(routeName, func(c echo.Context) error {
			return route.cb(c.(*csxhttp.Context))
		})
	}
}

func handleRoute(method, controller, name string, cb func(*csxhttp.Context) error) {
	routes = append(routes, route{
		method:     method,
		controller: controller,
		name:       name,
		cb:         cb,
	})
}

// GET Wrapper for GET query
func GET(controller, name string, cb func(ctx *csxhttp.Context) error) {
	handleRoute("GET", controller, name, cb)
}

// POST Wrapper for POST query
func POST(controller, name string, cb func(ctx *csxhttp.Context) error) {
	handleRoute("POST", controller, name, cb)
}

// PUT Wrapper for PUT query
func PUT(controller, name string, cb func(ctx *csxhttp.Context) error) {
	handleRoute("PUT", controller, name, cb)
}

// DELETE Wrapper for DELETE query
func DELETE(controller, name string, cb func(ctx *csxhttp.Context) error) {
	handleRoute("DELETE", controller, name, cb)
}

func prepareMethodName(prefix, method string) string {
	if method != "" {
		prefix += "/"
	}
	routeName := prefix + csxstrings.SplitPascal(method, "/")
	return "/" + strings.ToLower(routeName)
}

// HandleRoute handle route to app by name with HTTP method prefix
func HandleRoute(app *echo.Echo, prefix, methodName string, cb func(ctx echo.Context) error) {
	if strings.HasPrefix(methodName, "Post") {
		app.POST(prepareMethodName(prefix, methodName[4:]), cb)
	} else if strings.HasPrefix(methodName, "Delete") {
		app.DELETE(prepareMethodName(prefix, methodName[6:]), cb)
	} else if strings.HasPrefix(methodName, "Put") {
		app.PUT(prepareMethodName(prefix, methodName[3:]), cb)
	} else {
		app.GET(prepareMethodName(prefix, methodName[3:]), cb)
	}
}

// HandleController Bypasses all methods of the structure and registers them as api handlers.
// The name of the handler is formed by converting pascal case to url.
// For example PostSendInfo will be converted to send/info. The function prefix defines the http method.
func HandleController(app *echo.Echo, controller interface{}, ctrlFields map[string]interface{}) {
	structType := reflect.TypeOf(controller)
	structName := structType.Elem().Name()
	ctrlNameParts := strings.Split(structName, "Controller")
	if len(ctrlNameParts) != 2 {
		logrus.Error("incompatible controller struct name: ", structName)
		return
	}
	ctrlName := strings.ToLower(csxstrings.SplitPascal(ctrlNameParts[0], "-"))
	logrus.Info(" >>> init controller: " + ctrlName)
	for i := 0; i < structType.NumMethod(); i++ {
		method := structType.Method(i)
		routeName := method.Name
		logrus.Debug("	init controller method: " + routeName)
		HandleRoute(app, ctrlName, routeName, func(c echo.Context) error {
			ctx := c.(*csxhttp.Context)
			args := []reflect.Value{reflect.ValueOf(controller), reflect.ValueOf(ctx)}
			res := method.Func.Call(args)
			if len(res) == 0 {
				logrus.Warn("controller: " + ctrlName + " method: " + routeName + " returns no value")
				return nil
			}
			resInt := res[0]
			if !resInt.IsNil() {
				err, ok := resInt.Interface().(error)
				if ok {
					return err
				}
				logrus.Warn("controller: " + ctrlName + " method: " + routeName + " returns non-error value")
				return nil
			}
			return nil
		})
	}

	// allows to transfer data to the controller
	if ctrlFields != nil {
		structElem := structType.Elem()
		structValue := reflect.ValueOf(controller).Elem()
		for i := 0; i < structValue.NumField(); i++ {
			ctrlField := structValue.Field(i)
			field, ok := ctrlFields[structElem.Field(i).Name]
			if !ok {
				continue
			}
			ctrlField.Set(reflect.ValueOf(field))
		}
	}
}

func RegisterController(ctrl interface{}) {
	// prepare logger
	if len(controllers) == 0 {
		currLev := os.Getenv("LOG_LEVEL")
		logLevel, err := logrus.ParseLevel(currLev)
		if err != nil {
			logrus.Error(err)
		}
		logrus.SetLevel(logLevel)
		logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true, FullTimestamp: true})
	}
	structTypeNew := reflect.TypeOf(ctrl)
	if reflect.ValueOf(ctrl).Kind() != reflect.Ptr {
		nameOfStruct := structTypeNew.Name()
		logrus.Fatal("incorrect controller struct '" + nameOfStruct + "' (not pointer). Please use as follow: err := csxhandlers.RegisterController(&SampleController{})")
	}
	structName := structTypeNew.Elem().Name()
	for i := 0; i < len(controllers); i++ {
		structType := reflect.TypeOf(controllers[i])
		if structTypeNew == structType {
			logrus.Fatal("duplicate initialize controller: "+structName, ". Make sure the following code is not repeated: csxhandlers.RegisterController(&"+structName+"{})")
		}
	}
	controllers = append(controllers, ctrl)
}

func InitController(controller interface{}) {
	initMethod, ok := reflect.TypeOf(controller).MethodByName("Init")
	if ok {
		args := []reflect.Value{reflect.ValueOf(controller)}
		initMethod.Func.Call(args)
	}
}

func Start(app *echo.Echo, sessStore *csxsession.CsxStore, sessionKey string) {
	App = app
	for i := 0; i < len(controllers); i++ {
		ctrl := controllers[i]
		HandleController(app, ctrl, nil)
		InitController(ctrl)
	}
	initRoutes()
	for i := 0; i < len(initCallbacks); i++ {
		cb := initCallbacks[i]
		go cb()
	}
	sessionsStore = sessStore
	SessionKey = sessionKey
}

func InitHandler(cb func()) {
	initCallbacks = append(initCallbacks, cb)
}

// SetSessionStore set session store
func SetSessionStore(store *csxsession.CsxStore, key string) {
	sessionsStore = store
	SessionKey = key
}

func NewSession(ctx *csxhttp.Context) (*sessions.Session, error) {
	return sessionsStore.New(ctx.Request(), SessionKey)
}

func SaveSession(ctx *csxhttp.Context, session *sessions.Session) error {
	return sessionsStore.Save(ctx.Request(), ctx.Response(), session)
}

func SaveSessionWithoutCtx(session *sessions.Session) error {
	return sessionsStore.SaveWithoutCtx(session)
}

func DeleteSession(ctx *csxhttp.Context, session *sessions.Session) error {
	return sessionsStore.Delete(ctx.Request(), ctx.Response(), session)
}

func GetSession(ctx *csxhttp.Context) (*sessions.Session, error) {
	return sessionsStore.Get(ctx.Request(), SessionKey)
}

func GetSessionByRequest(req *http.Request) (*sessions.Session, error) {
	return sessionsStore.Get(req, SessionKey)
}

func DeleteSessionByID(id string) error {
	return sessionsStore.DeleteByID(id)
}

func GetSessionByID(id string) (*sessions.Session, error) {
	return sessionsStore.GetByID(id, SessionKey)
}
