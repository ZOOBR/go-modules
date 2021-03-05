package csxerrors

import (
	"strings"

	"gitlab.com/battler/modules/csxhttp"
)

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

// GetCtxLang return lang from request
func GetCtxLang(ctx *csxhttp.Context) string {
	lang := strings.Replace(ctx.Request().Header.Get("Accept-Language"), " ", "", -1)
	parts := strings.Split(lang, ",")
	if len(parts) > 1 {
		lang = parts[1]
	}
	return lang
}

// Error return response message and status code by error ID
func Error(errorCode string, lang string) (msg string, statusCode int) {
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

// Result get request lang and return result status, status code and translated message
func Result(ctx *csxhttp.Context, errorCode string) (bool, int, string) {
	lang := GetCtxLang(ctx)
	msg, statusCode := Error(errorCode, lang)
	return statusCode < 400, statusCode, msg
}

// Init load translates for responses
func Init(langs []Lang) {
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
