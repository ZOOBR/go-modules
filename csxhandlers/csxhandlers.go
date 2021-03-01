package csxhandlers

import (
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

// HandlerManager wrapper for work with custom context and other functional
type HandlerManager struct {
	echo *echo.Echo
}

// GET Wrapper for GET query
func (manager *HandlerManager) GET(route string, cb func(ctx *csxhttp.Context) error) {
	manager.echo.GET(route, func(c echo.Context) error {
		return cb(c.(*csxhttp.Context))
	})
}

// POST Wrapper for POST query
func (manager *HandlerManager) POST(route string, cb func(ctx *csxhttp.Context) error) {
	manager.echo.POST(route, func(c echo.Context) error {
		return cb(c.(*csxhttp.Context))
	})
}

// PUT Wrapper for PUT query
func (manager *HandlerManager) PUT(route string, cb func(ctx *csxhttp.Context) error) {
	manager.echo.PUT(route, func(c echo.Context) error {
		return cb(c.(*csxhttp.Context))
	})
}

// DELETE Wrapper for DELETE query
func (manager *HandlerManager) DELETE(route string, cb func(ctx *csxhttp.Context) error) {
	manager.echo.DELETE(route, func(c echo.Context) error {
		return cb(c.(*csxhttp.Context))
	})
}

var (
	App         *echo.Echo
	Manager     = HandlerManager{}
	controllers = []interface{}{}
)

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
func HandleController(app *echo.Echo, controller interface{}) {
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
		prefix := "api/" + ctrlName
		logrus.Debug("	init controller method: " + routeName)
		HandleRoute(app, prefix, routeName, func(c echo.Context) error {
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
}

func NewHandlerManager(app *echo.Echo) *HandlerManager {
	return &HandlerManager{echo: app}
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
		HandleController(app, ctrl)
		InitController(ctrl)
	}
	sessionsStore = sessStore
	SessionKey = sessionKey
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

func DeleteSession(ctx *csxhttp.Context, session *sessions.Session) error {
	return sessionsStore.Delete(ctx.Request(), ctx.Response(), session)
}

func GetSession(ctx *csxhttp.Context) (*sessions.Session, error) {
	return sessionsStore.Get(ctx.Request(), SessionKey)
}

func DeleteSessionByID(id string) error {
	return sessionsStore.DeleteByID(id)
}
