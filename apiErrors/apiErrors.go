package apiErrors

import "strings"

type ErrorItem struct {
	statusCode int
	messages   map[string]string
}

var (
	errorsItems = map[string]ErrorItem{}
)

type Lang struct {
	Key    string `db:"key" json:"key" key:"1"`
	Status int    `db:"status" json:"status" type:"int4"`
	Ru     string `db:"ru" json:"ru"`
	En     string `db:"en" json:"en"`
}

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
