package apiErrors

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"gitlab.com/battler/models"
)

type ErrorItem struct {
	statusCode int
	messages   map[string]string
}

var (
	errorsItems = map[string]ErrorItem{}
)

func Init() {
	langs, err := models.GetLangs()
	if err != nil {
		log.Error("Init locales err:", err)
	}
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
