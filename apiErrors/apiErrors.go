package apiErrors

type ErrorItem struct {
	statusCode int
	messages   map[string]string
}

var (
	errorsItems = map[string]ErrorItem{
		"invalidPromocode": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid promocode",
				"ru": "Неправильный промокод",
			},
		},
	}
)

func Error(errorCode string, lang string) (msg string, statusCode int) {
	if lang == "" {
		lang = "en"
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
