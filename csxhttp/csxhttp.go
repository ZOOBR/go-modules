package csxhttp

import (
	"bytes"
	"net/http"
	"strconv"
	"strings"
	"text/template"

	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/csxaccess"
)

// Context Custom context
type Context struct {
	echo.Context
	store         echo.Map
	mandatSession *MandatSession
}

// ErrorItem struct for send errors
type ErrorItem struct {
	statusCode int
	messages   map[string]string
}

var (
	errorsItems = map[string]ErrorItem{}
)

// Lang struct for load langs
type Lang struct {
	Key    string `db:"key" json:"key" key:"1"`
	Status int    `db:"status" json:"status" type:"int4"`
	Ru     string `db:"ru" json:"ru"`
	En     string `db:"en" json:"en"`
}

// SubjectInfo struct for create error messages with templates
type SubjectInfo struct {
	Subject string
}

// NewContext return context based on echo.Context
func NewContext(ctx echo.Context) *Context {
	return &Context{
		ctx,
		make(echo.Map),
		nil,
	}
}

// Values return stre with all values
func (ctx *Context) Values() map[string]interface{} {
	return ctx.store
}

// GetString return string value by key
func (ctx *Context) GetString(key string) (result string, ok bool) {
	val := ctx.Get(key)
	if val != nil {
		strVal, ok := ctx.Get(key).(string)
		if ok {
			result = strVal
			return result, true
		}
	}
	return result, false
}

// MustGetString return string value by key
func (ctx *Context) MustGetString(key string) (result string) {
	result, _ = ctx.GetString(key)
	return result
}

// GetInt return int value by key
func (ctx *Context) GetInt(key string) (int, bool) {
	val, ok := ctx.Get(key).(int)
	return val, ok
}

// MustGetInt return int value by key
func (ctx *Context) MustGetInt(key string) int {
	result, _ := ctx.GetInt(key)
	return result
}

// GetHeader return header value by key
func (ctx *Context) GetHeader(key string) string {
	return ctx.Request().Header.Get(key)
}

// SetHeader set header value by key
func (ctx *Context) SetHeader(key, value string) {
	ctx.Request().Header.Set(key, value)
}

// QueryParamInt return string value by key
func (ctx *Context) QueryParamInt(key string) (int, error) {
	param := ctx.QueryParam(key)
	intValue, err := strconv.ParseInt(param, 10, 64)
	return int(intValue), err
}

// QueryParamBool return string value by key
func (ctx *Context) QueryParamBool(key string) (bool, error) {
	param := ctx.QueryParam(key)
	return strconv.ParseBool(param)
}

// FillQueryParams fill query params to map by keys
func (ctx *Context) FillQueryParams(params ...string) map[string]string {
	paramsMap := map[string]string{}
	for i := 0; i < len(params); i++ {
		param := params[i]
		queryParam := ctx.QueryParam(param)
		if queryParam != "" {
			paramsMap[param] = queryParam
		}
	}
	return paramsMap
}

// FillParams fill query params to map by keys
func (ctx *Context) FillParams(paramsMap map[string]string, params ...string) map[string]string {
	if paramsMap == nil {
		return nil
	}
	for i := 0; i < len(params); i++ {
		param := params[i]
		queryParam := ctx.QueryParam(param)
		if queryParam != "" {
			paramsMap[param] = queryParam
		}
	}
	return paramsMap
}

// SimpleQueryParams return map[atring]interface with params
func (ctx *Context) SimpleQueryParams() map[string]string {
	paramsMap := map[string]string{}
	params := ctx.QueryParams()
	for key, value := range params {
		paramsMap[key] = strings.Join(value, ",")
	}
	return paramsMap
}

// Success send http 200 without arguments
func (ctx *Context) Success() error {
	return ctx.NoContent(http.StatusOK)
}

// SetMandatSession sets mandat session to ctx
func (ctx *Context) SetMandatSession(mandatSession *MandatSession) {
	ctx.mandatSession = mandatSession
}

// SetMandatSession sets mandat session to ctx
func (ctx *Context) AccessManager() *csxaccess.AccessManager {
	return ctx.mandatSession.accessManager
}

// GetCtxLang return lang from request
func GetCtxLang(ctx *Context) string {
	lang := strings.Replace(ctx.Request().Header.Get("Accept-Language"), " ", "", -1)
	parts := strings.Split(lang, ",")
	if len(parts) > 1 {
		lang = parts[1]
	}
	return lang
}

func getErrorMsg(errorCode string, lang string) (msg string, statusCode int) {
	if lang == "" {
		lang = "en"
	} else {
		langs := strings.SplitAfter(lang, ",")
		if len(langs) > 1 {
			lang = strings.Trim(langs[1], " ")
		}
	}

	item, ok := errorsItems[errorCode]
	if !ok {
		return errorCode, 400
	}
	itemMsg, ok := item.messages[lang]
	if !ok {
		return errorCode, 400
	}
	return itemMsg, item.statusCode
}

// GetErrorMsg is using for returning localized error messages & status
func GetErrorMsg(errorCode, locale string) (msg string, status int) {
	return getErrorMsg(errorCode, locale)
}

// Result get request lang and return result status, status code and translated message
func Result(ctx *Context, errorID string) (bool, int, string) {
	if errorID == "" {
		return true, 200, ""
	}
	lang := GetCtxLang(ctx)
	msg, statusCode := GetErrorMsg(errorID, lang)
	subject := ctx.Path()
	if msg != "" {
		tmpl, err := template.New(errorID).Parse(msg)
		if err == nil {
			var buf bytes.Buffer
			tmpl.Execute(&buf, &SubjectInfo{subject})
			msg = buf.String()
		}
	}
	return statusCode < 400, statusCode, msg
}

//Error is using for handling error responses
func Error(ctx *Context, errorCode string, err ...interface{}) error {
	lenErr := len(err)
	if lenErr > 0 {
		newErrs := make([]interface{}, 0)
		if lenErr > 1 {
			newErrs = append(newErrs, "[", err[0], "]", err[1:])
		} else {
			newErrs = append(newErrs, "[", err[0], "]")
		}
		logrus.Error(newErrs...)
	}
	lang := strings.Replace(ctx.GetHeader("Accept-Language"), " ", "", -1)
	parts := strings.Split(lang, ",")
	if len(parts) > 1 {
		lang = parts[1]
	}
	msg, status := getErrorMsg(errorCode, lang)
	return ctx.String(status, msg)
}

//ChatError is using for handling error chat responses
func ChatError(ctx *Context, errorCode string, clientID string, regStateID *string, err ...interface{}) error {
	if len(err) > 0 {
		logrus.Error(err...)
	}
	lang := strings.Replace(ctx.GetHeader("Accept-Language"), " ", "", -1)
	parts := strings.Split(lang, ",")
	if len(parts) > 1 {
		lang = parts[1]
	}
	msg, status := getErrorMsg(errorCode, lang)
	return ctx.String(status, msg)
}

//Success is using for handling success responses
func Success(ctx *Context, messageCode string, info ...interface{}) error {
	if len(info) > 0 {
		logrus.Info(info...)
	}
	lang := strings.Replace(ctx.GetHeader("Accept-Language"), " ", "", -1)
	parts := strings.Split(lang, ",")
	if len(parts) > 1 {
		lang = parts[1]
	}
	msg, status := getErrorMsg(messageCode, lang)
	return ctx.String(status, msg)
}

// SetLangs load translates for responses
func SetLangs(langs []Lang) {
	for _, item := range langs {
		errorsItems[item.Key] = ErrorItem{
			statusCode: item.Status,
			messages: map[string]string{
				"en": item.En,
				"ru": item.Ru,
			},
		}
	}
}
