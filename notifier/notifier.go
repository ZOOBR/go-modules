package notifier

import (
	"errors"
	"sync"

	sender "gitlab.com/battler/modules/msgSender"
	dbc "gitlab.com/battler/modules/sql"
	"gitlab.com/battler/modules/timers"

	"gitlab.com/battler/models"
)

var clientNotifications sync.Map

type notificationItem struct {
	Notification string
	TimerChan    chan bool
}

// Notify sends notification by trigger
func Notify(lang, trigger string, clientID *string, phones, tokens, mails []string, data interface{}) error {
	notifications, err := models.GetNotifications([]string{`"triggerInit" = '` + trigger + `' OR "triggerClose" = '` + trigger + `'`})
	if err != nil {
		return err
	}
	for i := 0; i < len(notifications); i++ {
		n := notifications[i]
		if n.RepeatTime > 0 && clientID != nil {
			startRepeatableNotify(n, lang, clientID, phones, tokens, mails, data)
		}
	}
	return nil
}

// LoadOpenNotifications loads open notifications for clients
func LoadOpenNotifications() error {
	notifications, err := models.GetOpenNotificationsInfo()
	if err != nil {
		return err
	}
	for i := 0; i < len(notifications); i++ {
		n := notifications[i]
		if n.Count >= n.RepeatCount {
			continue
		}
		newTimerNotify := timers.SetInterval(func() {
			client, err := models.GetClient(&n.Recipient)
			if err != nil {
				return
			}
			phones, tokens, mails := []string{}, []string{}, []string{}
			if client.Phone != nil {
				phones = append(phones, *client.Phone)
			}
			if client.DeviceToken != nil {
				tokens = append(tokens, *client.DeviceToken)
			}
			if client.Email != nil {
				mails = append(mails, *client.Email)
			}
			newMessage := sender.NewMessage(n.Lang, n.RepeatTemplate, "", phones, tokens, mails)
			newMessage.Send(n.Data)
		}, int(n.RepeatTime), false)
		clientNotifications.Store(n.Recipient, newTimerNotify)
	}
	return nil
}

// startRepeatableNotify start repeatable interval notifications for client
func startRepeatableNotify(n models.Notification, lang string, clientID *string, phones, tokens, mails []string, data interface{}) {
	newTimerNotify := timers.SetInterval(func() {
		newMessage := sender.NewMessage(lang, n.RepeatTemplate, "", phones, tokens, mails)
		newMessage.Mode = n.Mask
		newMessage.Send(data)
	}, int(n.RepeatTime), false)
	clientNotifications.Store(*clientID, notificationItem{Notification: n.Id, TimerChan: newTimerNotify})
}

// stopRepeatableNotify stops repeatable interval notifications for client, remove them from DB and sync.Map
func stopRepeatableNotify(clientID *string) error {
	timerInt, ok := clientNotifications.Load(clientID)
	if !ok {
		return errors.New("Notify not found")
	}
	timer, ok := timerInt.(notificationItem)
	if !ok {
		return errors.New("Invalid notify timer")
	}
	timer.TimerChan <- true
	clientNotifications.Delete(clientID)
	openNotification, err := models.GetOpenNotifications([]string{`notification = '` + timer.Notification + `'`})
	if err != nil {
		return err
	}
	if len(openNotification) > 0 {
		for i := 0; i < len(openNotification); i++ {
			openNotification[i].Delete(&dbc.Q)
		}
	}
	return nil
}
