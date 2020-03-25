package msgsender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/amqpconnector"
	amqp "gitlab.com/battler/modules/amqpconnector"
	"gitlab.com/battler/modules/templater"
)

const (
	// MessageModeSMS is sms mode of message
	MessageModeSMS = 1
	// MessageModePush is phone push mode of message
	MessageModePush = 2
	// MessageModeMail is e-mail mode of message
	MessageModeMail = 4
	// MessageModeWebPush is web push mode of message
	MessageModeWebPush = 8
	// MessageModeBot is telegram bot mode of message
	MessageModeBot = 16
)

var (
	amqpURI               = os.Getenv("AMQP_URI")
	mailingsExchange      = initMailingsExchange()
	publisher             *amqp.Consumer
	notificationPublisher *amqp.Consumer
	reconTime             = time.Second * 20
	botProxy              = os.Getenv("BOT_HTTP_PROXY")

	// PublisherInitWait - wait initialization of publisher
	PublisherInitWait = NewInitWait()
)

// InitWait - wait initialize
type InitWait struct {
	sync.Mutex
	cond        *sync.Cond
	initialized bool
	result      error
}

// NewInitWait - new waiter
func NewInitWait() *InitWait {
	wait := InitWait{}
	wait.cond = sync.NewCond(&wait)
	return &wait
}

// Wait wait
func (wait *InitWait) Wait() error {
	wait.Lock()
	if !wait.initialized {
		wait.cond.Wait()
	}
	result := wait.result
	wait.Unlock()
	return result
}

// Process set initialize result and signals waiters
func (wait *InitWait) Process(result error) {
	wait.Lock()
	wait.result = result
	wait.initialized = true
	wait.Unlock()
	wait.cond.Broadcast()
}

func initMailingsExchange() string {
	mExch := os.Getenv("MAILINGS_EXCHANGE")
	if mExch == "" {
		mExch = "csx.mailings"
	}
	return mExch
}

func initEventsPublisher() {
	amqpTelemetryExchange := os.Getenv("AMQP_EVENTS_EXCHANGE")
	if amqpURI != "" && amqpTelemetryExchange != "" {
		var err error
		for {
			publisher, err = amqp.NewPublisher(amqpURI, amqpTelemetryExchange, "topic", "csx.events", "csx.events")
			if err != nil {
				log.Error("init events publisher err:", err)
				log.Warn("try reconnect to rabbitmq after:", reconTime)
				time.Sleep(reconTime)
				continue
			}
			PublisherInitWait.Process(err)
			log.Info("events publisher running")
			select {
			case <-publisher.Done:
				log.Warn("try reconnect to rabbitmq after:", reconTime)
				time.Sleep(reconTime)
				continue
			}
		}
	}
}

func initNotificationsPublisher() {
	if amqpURI != "" && mailingsExchange != "" {
		var err error
		for {
			notificationPublisher, err = amqp.NewPublisher(amqpURI, mailingsExchange, "direct", "", "csx.notifications")
			if err != nil {
				log.Error("init notification publisher err:", err)
				log.Warn("try reconnect to rabbitmq after:", reconTime)
				time.Sleep(reconTime)
				continue
			}
			log.Info("notification publisher running")
			select {
			case <-notificationPublisher.Done:
				log.Warn("try reconnect to rabbitmq after:", reconTime)
				time.Sleep(reconTime)
				continue
			}
		}
	}
}

// Message is a common simple message struct
type Message struct {
	Mode    int         `json:"mode"`
	Msg     string      `json:"msg"`
	Title   string      `json:"title"`
	Lang    string      `json:"lang"`
	Phones  []string    `json:"phones"`
	Tokens  []string    `json:"tokens"`
	Addrs   []string    `json:"addrs"`
	Sender  string      `json:"sender"`
	Payload interface{} `json:"payload"`
	Trigger string      `json:"trigger"`
	BotID   string      `json:"bot"`
	ChatID  string      `json:"chat"`
	Type    int         `json:"type"`
	MsgId   *string     `json:"msgId"`
}

// SMS is a basic SMS struct
type SMS struct {
	Phone string  `json:"phone"`
	Msg   string  `json:"msg"`
	MsgID *string `json:"msgId"`
	Type  string  `json:"type"`
}

// Mail is a basic email struct
type Mail struct {
	From        string   `json:"from"`
	To          string   `json:"to"`
	Subject     string   `json:"subject"`
	Images      []string `json:"images"`
	Bucket      string   `json:"bucket"`
	Body        string   `json:"body"`
	ContentType string   `json:"contentType"`
}

// Push is a basic push struct
type Push struct {
	Msg     string      `json:"msg"`
	Data    interface{} `json:"data"`
	Title   string      `json:"title"`
	Tokens  []string    `json:"tokens"`
	IsTopic bool        `json:"isTopic"`
}

// SendEmail is using for sending email messages
// to - recepient email
// subject - email subject
// mail - email body
// contentType - email content type
// images - array of paths to images (nil if without images)
// bucket - optional for email with images
func SendEmail(to, subject, mail string, contentType string, images *[]string, bucket ...string) {
	log.Info("[msgSender-SendEmail] ", "Try send notification to: ", to)

	newMail := Mail{
		From:        os.Getenv("EMAIL_SENDER"),
		To:          to,
		Subject:     subject,
		Body:        mail,
		ContentType: contentType,
	}

	if images != nil && len(*images) > 0 {
		newMail.Images = *images
	}
	if len(bucket) > 0 {
		newMail.Bucket = bucket[0]
	}
	m, err := json.Marshal(newMail)
	if err != nil {
		log.Warn("msgSender-sendEmail error json marshal: ", err)
	}
	notificationPublisher.Publish(m, "email")
	log.Info("[msgSender-SendEmail] ", "Success sended notification to: ", to)
}

// SendSMS is using for sending SMS messages
// phone - recepient phone
// msg - message body
func SendSMS(phone, msg string, options ...string) {
	log.Info("[msgSender-SendSMS] ", "Try send SMS to: ", phone)
	newSms := SMS{Phone: phone, Msg: msg}
	if len(options) > 0 && options[0] != "" {
		newSms.MsgID = &options[0]
	}
	if len(options) > 1 {
		newSms.Type = options[1]
	}
	m, err := json.Marshal(newSms)
	if err != nil {
		log.Error("[msgSender-SendSMS] ", "Error create sms for client: "+phone, err)
		return
	}
	notificationPublisher.Publish(m, "sms")
	log.Info("[msgSender-SendSMS] ", "Success sended notification to: ", phone)
}

// SendPush is using for sending push messages
// msg - message body
// title - message title
// tokens - recipient tokens, array of deviceToken
// isTopic - is topic message
func SendPush(msg, title string, tokens []string, data interface{}, isTopic bool) {
	log.Info("[msgSender-SendPush] ", "Try send push to: ", tokens)
	newPush := Push{Msg: msg, Title: title, Tokens: tokens, Data: data}
	newPush.IsTopic = isTopic

	m, err := json.Marshal(newPush)
	if err != nil {
		log.Error("[msgSender-SendPush] ", "Error create push: ", err)
		return
	}
	notificationPublisher.Publish(m, "push")
	log.Info("[msgSender-SendPush] ", "Success sended notification to: ", tokens)
}

// SendWeb is using for sending web push messages
func SendWeb(routingKey string, payload interface{}) {
	var event amqpconnector.Update
	switch payload.(type) {
	case amqpconnector.Update:
		event = payload.(amqpconnector.Update)
		break
	case map[string]interface{}:
		payload := payload.(map[string]interface{})
		event.Cmd = "notify"
		event.ExtData = payload
		if firm, ok := payload["firm"]; ok {
			event.Groups = []string{firm.(string)}
		}
		break
	default:
		event.Cmd = "notify"
		event.ExtData = payload
	}
	msgdata, _ := json.Marshal(event)
	publisher.Publish(msgdata, routingKey)
}

// SendBot sending message to telegram chat
// msg - message body
// title - message title
// botID - bot api token
// chatID - chat identity
func SendBot(msg, title string, botID, chatID string) {
	if title != "" {
		msg = "*" + title + "*\r\n" + msg
	}
	uri := "https://api.telegram.org/bot" + botID + "/sendMessage"
	data := []byte(`{"chat_id":"` + chatID + `","text":"` + msg + `","parse_mode":"Markdown"}`)
	r := bytes.NewReader(data)
	client := &http.Client{}
	if botProxy != "" {
		proxyURL, err := url.Parse(botProxy)
		if err == nil {
			client.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		}
	}
	resp, err := client.Post(uri, "application/json", r)
	if err != nil {
		log.Error("[msgSender-SendBot] ", "Error send telegram to: ", chatID, " ", err)
		return
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	log.Info("[msgSender-SendBot] ", "Sended telegram to: ", chatID, result)
}

// Send format and send universal message by SMS, Push, Mail
func (msg *Message) Send(data interface{}) {
	var text, typ, title, info string
	if msg.Mode != MessageModeWebPush {
		text = msg.Msg
		isTemplate := false
		if len(text) > 0 && text[0] == '#' {
			text = text[1:]
			isTemplate = true
		}
		text, typ, _ = templater.Format(text, msg.Lang, data, map[string]interface{}{
			"isTemplate": isTemplate,
		})
		if len(msg.Title) > 0 && (msg.Mode&(MessageModePush|MessageModeMail|MessageModeBot)) != 0 {
			title = msg.Title
			isTemplate := false
			if title[0] == '#' {
				title = title[1:]
				isTemplate = true
			}
			title, _, _ = templater.Format(title, msg.Lang, data, map[string]interface{}{
				"isTemplate": isTemplate,
			})
		}
	}
	if (msg.Mode & MessageModeSMS) != 0 {
		for _, phone := range msg.Phones {
			if len(info) > 0 {
				info += ","
			}
			info += phone
			msgID := ""
			if msg.MsgId != nil {
				msgID = *msg.MsgId
			}
			SendSMS(phone, text, msgID, fmt.Sprintf("%d", msg.Type))
		}
	}
	if (msg.Mode&MessageModePush) != 0 && len(msg.Tokens) > 0 {
		if len(info) > 0 {
			info += ","
		}
		info += strings.Join(msg.Tokens[:], ",")
		SendPush(text, title, msg.Tokens, msg.Payload, false)
	}
	if (msg.Mode & MessageModeWebPush) != 0 {
		SendWeb(msg.Trigger, msg.Payload)
	}
	if (msg.Mode & MessageModeBot) != 0 {
		SendBot(text, title, msg.BotID, msg.ChatID)
	}
	if (msg.Mode & MessageModeMail) != 0 {
		var contentType string
		if typ == "html" {
			contentType = "text/html"
		} else {
			contentType = "text/plain"
		}
		for _, addr := range msg.Addrs {
			if len(info) > 0 {
				info += ","
			}
			info += addr
			SendEmail(addr, title, text, contentType, nil)
		}
	}
	log.Debug("Message", " [Send] ", info+": ", text)
}

// NewMessage create new message structure
func NewMessage(lang, msg, title string, phones, tokens, mails []string, msgType int, msgID *string) *Message {
	mode := MessageModeSMS | MessageModePush | MessageModeMail
	return &Message{
		Mode:   mode,
		Msg:    msg,
		Title:  title,
		Lang:   lang,
		Phones: phones,
		Tokens: tokens,
		Addrs:  mails,
		Type:   msgType,
		MsgId:  msgID,
	}
}

// SendMessage format and send universal message by SMS, Push, Mail
func SendMessage(lang, msg, title string, phones, tokens, mails []string, data interface{}, payload interface{}, msgType int, msgID *string) {
	message := NewMessage(lang, msg, title, phones, tokens, mails, msgType, msgID)
	message.Payload = payload
	message.Send(data)
}

// SendMessageSMS format and send universal message by SMS
func SendMessageSMS(lang, msg, title, phone string, data interface{}, msgType int, msgID *string) {
	NewMessage(lang, msg, title, []string{phone}, nil, nil, msgType, msgID).Send(data)
}

// SendMessagePush format and send universal message by phone push
func SendMessagePush(lang, msg, title, token string, data interface{}, payload interface{}, msgType int, msgID *string) {
	message := NewMessage(lang, msg, title, nil, []string{token}, nil, msgType, msgID)
	message.Payload = payload
	message.Send(data)
}

// SendMessageMail format and send universal message by e-mail
func SendMessageMail(lang, msg, title, addr string, data interface{}, msgType int, msgID *string) {
	NewMessage(lang, msg, title, nil, nil, []string{addr}, msgType, msgID).Send(data)
}

// Init initializes initEventsPublisher
func Init() {
	go initNotificationsPublisher()
	go initEventsPublisher()
}
