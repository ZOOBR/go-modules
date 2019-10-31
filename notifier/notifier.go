package notifier

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

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
func Notify(lang, trigger string, clientID string, data interface{}) error {
	phones, tokens, mails := []string{}, []string{}, []string{}
	client, err := models.GetClient(&clientID)
	if err != nil {
		return err
	}
	if client.Phone != nil {
		phones = append(phones, *client.Phone)
	}
	if client.DeviceToken != nil {
		tokens = append(tokens, *client.DeviceToken)
	}
	if client.Email != nil {
		mails = append(mails, *client.Email)
	}

	notifications, err := models.GetNotifications([]string{`"triggerInit" = '` + trigger + `' OR "triggerClose" = '` + trigger + `'`})
	if err != nil {
		return err
	}
	for i := 0; i < len(notifications); i++ {
		n := notifications[i]
		if n.TriggerInit == trigger {
			go startNotify(n, lang, clientID, phones, tokens, mails, data)
		} else if n.TriggerClose != nil && *n.TriggerClose == trigger {
			err = closeNotify(clientID, n.Id)
			if err != nil {
				logrus.Error("error close notification: ", err)
				continue
			}
		}
		err = logNotify(clientID, n.Id, trigger)
		if err != nil {
			logrus.Error("error save notification log: ", err)
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

// startNotify send once notification or start repeatable interval notifications for client
func startNotify(n models.Notification, lang string, clientID string, phones, tokens, mails []string, data interface{}) {
	newOpenNotification := models.NewOpenNotification(clientID, n.Id, lang, data)
	err := newOpenNotification.Save(&dbc.Q)
	if err != nil {
		logrus.Error("error save open notification: ", err)
		return
	}
	newMessage := sender.NewMessage(lang, n.Template, n.TitleTemplate, phones, tokens, mails)
	newMessage.Mode = n.Mask
	newMessage.Send(data)
	if n.RepeatCount <= 1 || n.RepeatTime == 0 {
		return
	}
	time.Sleep(time.Second * time.Duration(n.RepeatTime*1000)) //sleep until repeat notification
	isNotificationOpen, err := models.GetOpenNotification(newOpenNotification.Id)
	if err != nil {
		logrus.Error("error get open notification: ", err)
		return
	}
	if isNotificationOpen != nil {
		return
	}
	newTimerNotify := timers.SetInterval(func() {
		newMessage := sender.NewMessage(lang, n.RepeatTemplate, n.TitleTemplate, phones, tokens, mails)
		newMessage.Mode = n.Mask
		newMessage.Send(data)
	}, int(n.RepeatTime)*1000, false)
	var clientNotificationsMap *sync.Map
	clientNotificationsMapInt, ok := clientNotifications.Load(clientID)
	if !ok {
		clientNotificationsMap = &sync.Map{}
	} else {
		clientNotificationsMap, ok = clientNotificationsMapInt.(*sync.Map)
		if !ok {
			logrus.Error("client notification map conversion err")
			return
		}
	}
	clientNotificationsMap.Store(n.Id, newTimerNotify)
	clientNotifications.Store(clientID, clientNotificationsMap)
}

// stopNotify stops repeatable interval notifications for client, remove them from DB and sync.Map
func stopNotify(clientID, notificationID string) {
	clientNotificationsMapInt, ok := clientNotifications.Load(clientID)
	if !ok {
		logrus.Error("Client notifications not found")
		return
	}
	clientNotificationsMap, ok := clientNotificationsMapInt.(*sync.Map)
	if !ok {
		logrus.Error("Can not convert client notifications to map")
		return
	}
	timerInt, ok := clientNotificationsMap.Load(notificationID)
	if !ok {
		logrus.Error("Notification not found")
		return
	}
	timer, ok := timerInt.(chan bool)
	if !ok {
		logrus.Error("Can not convert timerInt to chan bool")
		return
	}
	timer <- true
	clientNotificationsMap.Delete(notificationID)
	count := 0
	clientNotificationsMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count == 0 {
		clientNotifications.Delete(clientID)
	} else {
		clientNotifications.Store(clientID, clientNotificationsMap)
	}
}

func closeNotify(clientID, notificationID string) error {
	openNotification, err := models.GetOpenNotifications([]string{`recipient = '` + clientID + `' AND notification = '` + notificationID + `'`})
	if err != nil {
		return err
	}
	if len(openNotification) > 0 {
		err = openNotification[0].Delete(&dbc.Q)
		if err != nil {
			return err
		}
		if openNotification[0].Count > 1 {
			go stopNotify(clientID, notificationID)
		}
	}
	return nil
}

func logNotify(recipient, notification, trigger string) error {
	newNotificationLog := models.NewNotificationLog(recipient, notification, trigger)
	err := newNotificationLog.Save(&dbc.Q)
	if err != nil {
		return err
	}
	return nil
}
