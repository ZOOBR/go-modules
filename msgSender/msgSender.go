package msgsender

import (
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
	amqp "gitlab.com/battler/modules/amqpconnector"
)

//SMS is a basic SMS struct
type SMS struct {
	Phone string  `json:"phone"`
	Msg   string  `json:"msg"`
	MsgID *string `json:"msgId"`
}

//Mail is a basic email struct
type Mail struct {
	From        string   `json:"from"`
	To          string   `json:"to"`
	Subject     string   `json:"subject"`
	Images      []string `json:"images"`
	Bucket      string   `json:"bucket"`
	Body        string   `json:"body"`
	ContentType string   `json:"contentType"`
}

//Push is a basic push struct
type Push struct {
	Msg     string   `json:"msg"`
	Title   string   `json:"title"`
	Tokens  []string `json:"tokens"`
	IsTopic bool     `json:"isTopic"`
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
	amqp.Publish(os.Getenv("AMQP_URI"), "csx.mailings", "direct", "email", string(m), false)
	log.Info("[msgSender-SendEmail] ", "Success sended notification to: ", to)
}

// SendSMS is using for sending SMS messages
// phone - recepient phone
// msg - message body
func SendSMS(phone, msg string, msgId ...string) {
	log.Info("[msgSender-SendSMS] ", "Try send SMS to: ", phone)
	newSms := SMS{Phone: phone, Msg: msg}
	if len(msgId) > 0 {
		newSms.MsgID = &msgId[0]
	}
	m, err := json.Marshal(newSms)
	if err != nil {
		log.Error("[msgSender-SendSMS] ", "Error create sms for client: "+phone, err)
		return
	}
	amqp.Publish(os.Getenv("AMQP_URI"), "csx.mailings", "direct", "sms", string(m), false)
	log.Info("[msgSender-SendSMS] ", "Success sended notification to: ", phone)
}

// SendPush is using for sending push messages
// msg - message body
// title - message title
// tokens - recipient tokens, array of deviceToken
// isTopic - is topic message
func SendPush(msg, title string, tokens []string, isTopic ...bool) {
	log.Info("[msgSender-SendPush] ", "Try send push to: ", tokens)
	newPush := Push{Msg: msg, Title: title, Tokens: tokens}
	if len(isTopic) > 0 {
		newPush.IsTopic = isTopic[0]
	}
	m, err := json.Marshal(newPush)
	if err != nil {
		log.Error("[msgSender-SendPush] ", "Error create push: ", err)
		return
	}
	amqp.Publish(os.Getenv("AMQP_URI"), "csx.mailings", "direct", "push", string(m), false)
	log.Info("[msgSender-SendSMS] ", "Success sended notification to: ", tokens)
}
