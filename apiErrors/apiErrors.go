package apiErrors

type ErrorItem struct {
	statusCode int
	messages   map[string]string
}

var (
	errorsItems = map[string]ErrorItem{
		"AuthFailed": ErrorItem{
			statusCode: 401,
			messages: map[string]string{
				"en": "You are not authorized",
				"ru": "Вы не авторизованы",
			},
		},
		"CardNotFound": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Card not found",
				"ru": "Карта не найдена",
			},
		},
		"DbError": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Oops, something going wrong...",
				"ru": "Упс, что-то пошло не так...",
			},
		},
		"ErrorPasswordHash": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Password saving failed",
				"ru": "Не удалось сохранить пароль",
			},
		},
		"ErrorUploadFile": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Error file upload",
				"ru": "Ошибка загрузки файла",
			},
		},
		"InvalidRegData": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid input data",
				"ru": "Введенные данные некорректны",
			},
		},
		"InvoiceNotFound": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invoice not found",
				"ru": "Счет не найден",
			},
		},
		"InvalidCard": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Card is invalid",
				"ru": "Карта не действительна",
			},
		},
		"invalidPromocode": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid promocode",
				"ru": "Неправильный промокод",
			},
		},
		"InvalidJSON": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Invalid JSON format",
				"ru": "Некорректный JSON формат",
			},
		},
		"InvoicePayError": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Can not pay invoice",
				"ru": "Не удалось оплатить счет",
			},
		},
		"MissingReqParams": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Missing required parameters in request",
				"ru": "Отсутствуют обязательные параметры в запросе",
			},
		},
		"PasswordsMatch": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Old and new password match",
				"ru": "Старый и новый пароли совпадают",
			},
		},
		"PromocodeExpired": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Promocode was activated before or has expired",
				"ru": "Промокод был активирован ранее или истек срок действия",
			},
		},
		"RentNotFound": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Rent not found",
				"ru": "Аренда не найдена",
			},
		},
		"RentStateNotFound": ErrorItem{
			statusCode: 400,
			messages: map[string]string{
				"en": "Rent state not found",
				"ru": "Арендное состояние не найдено",
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
