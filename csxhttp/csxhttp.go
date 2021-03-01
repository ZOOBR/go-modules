package csxhttp

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
)

// Context Custom context
type Context struct {
	echo.Context
	store echo.Map
}

// NewContext return context based on echo.Context
func NewContext(ctx echo.Context) *Context {
	return &Context{
		ctx,
		make(echo.Map),
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
