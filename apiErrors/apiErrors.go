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
		"PromocodeExpired": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Promocode has expired",
				"ru": "Срок действия промокод истек",
			},
		},
		"DbError": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Oops, something going wrong...",
				"ru": "Упс, что-то пошло не так...",
			},
		},
		"Auth failed": ErrorItem{
			statusCode: 401,
			messages: map[string]string{
				"en": "You are not authorized",
				"ru": "Вы не авторизованы",
			},
		},
		"MissingReqParams": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Missing required parameters in request",
				"ru": "Отсутствуют обязательные параметры в запросе",
			},
		},
		"InvalidJSON": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid JSON format",
				"ru": "Некорректный JSON формат",
			},
		},
		"CardNotFound": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Card not found",
				"ru": "Карта не найдена",
			},
		},
		"InvalidCard": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Card is invalid",
				"ru": "Карта не действительна",
			},
		},
		"S3Error": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Image is not available",
				"ru": "Изображение недоступно",
			},
		},
		"ServiceError": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Service unavailable",
				"ru": "Сервис недоступен",
			},
		},
		"ErrorUploadFile": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Error file upload",
				"ru": "Ошибка загрузки файла",
			},
		},
		"PasswordsMatch": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Old and new password match",
				"ru": "Старый и новый пароли совпадают",
			},
		},
		"ErrorPasswordHash": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Password saving failed",
				"ru": "Не удалось сохранить пароль",
			},
		},
		"InvalidRegData": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid input data",
				"ru": "Введенные данные некорректны",
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
