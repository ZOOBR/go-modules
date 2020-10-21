// Package amqpconnector is a wrapper for amqp package
// with reconnect functional support
package amqpconnector

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	strUtil "gitlab.com/battler/modules/strings"
)

var (
	consumers     sync.Map
	consumersLock sync.Mutex

	updateExch  = os.Getenv("EXCHANGE_UPDATES")
	amqpURI     = os.Getenv("AMQP_URI")
	envName     = os.Getenv("CSX_ENV")
	consumerTag = os.Getenv("SERVICE_NAME")
)

// Update struc for send updates msg to services
type Update struct {
	ID         string      `json:"id"`
	ExtID      string      `json:"extId"`
	Cmd        string      `json:"cmd"`
	Collection string      `json:"collection"`
	Data       string      `json:"data"`
	Groups     []string    `json:"groups"`
	ExtData    interface{} `json:"extData"`
	Recipients []string    `json:"recipients"`
}

//Consumer structure for NewConsumer result
type Consumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	done     chan error
	exchange *Exchange
	queue    *Queue
	uri      string
	name     string // consumer name for logs
	handlers []func(*Delivery)
}

// Exchange struct for receive exchange params
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

// Queue struct for receive queue params
type Queue struct {
	Name        string
	AutoDelete  bool
	Durable     bool
	ConsumerTag string
	Keys        []string
	BindedKeys  []string
	NoAck       bool
	NoLocal     bool
	Exclusive   bool
	NoWait      bool
	Args        map[string]interface{}
}

type Delivery amqp.Delivery

// TODO:: Get from env
var (
	reconTime = time.Second * 20
	lifetime  = time.Duration(0)
)

func (c *Consumer) logInfo(log string) string {
	return "[" + c.name + "]" + log
}

// ExchangeDeclare dial amqp server, decrare echange an queue if set
func (c *Consumer) ExchangeDeclare() (string, error) {
	if c.exchange != nil {
		logrus.Info(c.logInfo("amqp declaring exchange: "), c.exchange.Name, " type: ", c.exchange.Type, " durable: ", c.exchange.Durable, " autodelete: ", c.exchange.AutoDelete)
		if err := c.channel.ExchangeDeclare(
			c.exchange.Name,       // name of the exchange
			c.exchange.Type,       // type
			c.exchange.Durable,    // durable
			c.exchange.AutoDelete, // delete when complete
			c.exchange.Internal,   // internal
			c.exchange.NoWait,     // noWait
			c.exchange.Args,       // arguments
		); err != nil {
			return "", err
		}
		return c.exchange.Name, nil
	}
	return "", nil
}

// BindKeys dial amqp server, decrare echange an queue if set
func (c *Consumer) BindKeys(keys []string) error {
	if len(keys) > 0 {
		if len(c.queue.Keys) == 0 {
			c.queue.Keys = make([]string, 0)
		}
		for _, key := range keys {
			keyExists := false
			for _, oldKey := range c.queue.Keys {
				if oldKey == key {
					keyExists = true
					break
				}
			}
			if !keyExists {
				err := c.channel.QueueBind(
					c.queue.Name,    // name of the queue
					key,             // bindingKey
					c.exchange.Name, // sourceExchange
					c.queue.NoWait,  // noWait
					c.queue.Args,    // arguments
				)
				if err != nil {
					return err
				}
				c.queue.Keys = append(c.queue.Keys, key)
			}
		}
		logrus.Info(c.logInfo("amqp bind keys: "), keys, " to queue: ", c.queue.Name)
	}
	return nil
}

// QueueDeclare dial amqp server, decrare echange an queue if set
func (c *Consumer) QueueDeclare(exchange string, keys []string) (<-chan amqp.Delivery, error) {
	// declare and bind queue
	if c.queue != nil {
		logrus.Info(c.logInfo("amqp declare queue: "), c.queue.Name, " durable: ", c.queue.Durable, " autodelete: ", c.queue.AutoDelete)
		_, err := c.channel.QueueDeclare(
			c.queue.Name,       // name of the queue
			c.queue.Durable,    // durable
			c.queue.AutoDelete, // delete when unused
			c.queue.Exclusive,  // exclusive
			c.queue.NoWait,     // noWait
			c.queue.Args,       // arguments
		)
		if err != nil {
			return nil, err
		}
		if len(c.queue.Keys) > 0 && keys == nil {
			err = c.BindKeys(c.queue.Keys)
		} else if keys != nil && len(keys) > 0 {
			err = c.BindKeys(keys) // if reconnect or create new consumer cases
		} else {
			err = c.BindKeys([]string{c.queue.ConsumerTag})
		}

		if err != nil {
			return nil, err
		}
		logrus.Info(c.logInfo("starting consume for queue: "), c.queue.Name)
		deliveries, err := c.channel.Consume(
			c.queue.Name,        // name
			c.queue.ConsumerTag, // consumerTag,
			c.queue.NoAck,       // noAck
			c.queue.Exclusive,   // exclusive
			c.queue.NoLocal,     // noLocal
			c.queue.NoWait,      // noWait
			c.queue.Args,        // arguments
		)
		if err != nil {
			return nil, err
		}
		return deliveries, nil

	}
	return nil, nil
}

// Connect dial amqp server, decrare echange an queue if set
func (c *Consumer) Connect(reconnect bool, keys []string) (<-chan amqp.Delivery, error) {
	var err error
	logrus.Info(c.logInfo("amqp connect to: "), c.uri)
	c.conn, err = amqp.Dial(c.uri)
	if err != nil {
		return nil, err
	}

	logrus.Info(c.logInfo("amqp get channel"))
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}
	// declare exchange
	exchange, err := c.ExchangeDeclare()
	if err != nil {
		return nil, err
	}
	// declare queue and bind routing keys
	deliveries, err := c.QueueDeclare(exchange, keys)
	if err != nil {
		return nil, err
	}
	if !reconnect {
		go c.handleDeliveries(deliveries)
	}
	go func() {
		logrus.Error(c.logInfo("amqp connection err: "), <-c.conn.NotifyClose(make(chan *amqp.Error)))
		c.done <- errors.New(c.logInfo("channel closed"))
	}()
	return deliveries, nil
}

// Reconnect reconnect to amqp server
func (c *Consumer) Reconnect(keys []string) <-chan amqp.Delivery {
	if err := c.Shutdown(); err != nil {
		logrus.Error(c.logInfo("error during shutdown: "), err)
	}
	reconnectInterval := 30
	logrus.Warn(c.logInfo("consumer wait reconnect"), " next try in ", reconnectInterval, "s")
	time.Sleep(time.Duration(reconnectInterval) * time.Second)
	deliveries, err := c.Connect(true, keys)
	if err != nil {
		logrus.Error(c.logInfo("consumer reconnect err: "), err.Error(), " next try in ", reconnectInterval, "s")
		return c.Reconnect(keys)
	}
	return deliveries
}

//NewConsumer create simple consumer for read messages with ack
func NewConsumer(amqpURI, name string, exchange *Exchange, queue *Queue, handlers []func(*Delivery)) (*Consumer, error) {
	c := &Consumer{
		exchange: exchange,
		queue:    queue,
		done:     make(chan error),
		uri:      amqpURI,
		name:     name,
	}
	if len(handlers) > 0 {
		c.handlers = handlers
	}
	var keys []string
	if len(c.queue.Keys) > 0 {
		keys = make([]string, len(c.queue.Keys))
		copy(keys, c.queue.Keys)
		c.queue.Keys = nil
	}
	_, err := c.Connect(false, keys)
	return c, err
}

// NewPublisher create publisher for send amqp messages
func NewPublisher(amqpURI, name string, exchange Exchange) (*Consumer, error) {
	c := &Consumer{
		exchange: &exchange,
		uri:      amqpURI,
		name:     name,
	}
	_, err := c.Connect(false, nil)
	return c, err
}

// PublishWithHeaders sends messages and reconnects in case of error
func (c *Consumer) PublishWithHeaders(msg []byte, routingKey string, headers map[string]interface{}) error {
	content := amqp.Publishing{
		ContentType: "text/plain",
		Body:        msg,
	}
	if headers != nil {
		content.Headers = headers
	}
	err := c.channel.Publish(c.exchange.Name, routingKey, false, false, content)
	if err != nil {
		logrus.Error(c.logInfo("try reconnect after publish err: "), err)
		c.Reconnect(nil)
		return c.PublishWithHeaders(msg, routingKey, headers)
	}
	return nil
}

// Publish sends messages and reconnects in case of error
func (c *Consumer) Publish(msg []byte, routingKey string) error {
	return c.PublishWithHeaders(msg, routingKey, nil)
}

// GetConsumer get or create publish/consume consumer
func GetConsumer(amqpURI, name string, exchange *Exchange, queue *Queue, handler func(*Delivery)) (consumer *Consumer, err error) {
	consumersLock.Lock()
	defer consumersLock.Unlock()
	consumerInt, ok := consumers.Load(name)
	if !ok {
		if queue == nil {
			exch := Exchange{}
			if exchange != nil {
				exch = *exchange
			}
			consumer, err = NewPublisher(amqpURI, name, exch)
		} else {
			consumer, err = NewConsumer(amqpURI, name, exchange, queue, []func(*Delivery){handler})
		}
		if err != nil {
			return nil, err
		}
		consumers.Store(name, consumer)
	} else {
		consumer = consumerInt.(*Consumer)
		var err error
		if queue != nil {
			if queue.Keys != nil && len(queue.Keys) > 0 {
				err = consumer.BindKeys(queue.Keys)
			} else {
				err = consumer.BindKeys([]string{queue.ConsumerTag})
			}
			if err != nil {
				return consumer, err
			}
		}
	}
	return consumer, nil
}

// Publish simple publisher with unique name and one connect
func Publish(amqpURI, consumerName, exchangeName, exchangeType, routingKey string, msg []byte, headers map[string]interface{}) error {
	consumer, err := GetConsumer(amqpURI, consumerName, &Exchange{Name: exchangeName, Type: exchangeType, Durable: true}, nil, nil)
	if err != nil {
		return err
	}
	if len(headers) > 0 {
		return consumer.PublishWithHeaders(msg, routingKey, headers)
	}
	return consumer.Publish(msg, routingKey)
}

// PublishDirect simple publisher with unique name and one connect
func PublishDirect(amqpURI, consumerName, exchangeName, routingKey string, msg []byte) error {
	return Publish(amqpURI, consumerName, exchangeName, "direct", routingKey, msg, nil)
}

// PublishTopic simple publisher with unique name and one connect
func PublishTopic(amqpURI, consumerName, exchangeName, routingKey string, msg []byte) error {
	return Publish(amqpURI, consumerName, exchangeName, "topic", routingKey, msg, nil)
}

// PublishFanout simple publisher with unique name and one connect
func PublishFanout(amqpURI, consumerName, exchangeName, routingKey string, msg []byte) error {
	return Publish(amqpURI, consumerName, exchangeName, "fanout", routingKey, msg, nil)
}

// PublishHeader simple publisher with unique name and one connect
func PublishHeader(amqpURI, consumerName, exchangeName string, msg []byte, headers map[string]interface{}) error {
	return Publish(amqpURI, consumerName, exchangeName, "header", "", msg, nil)
}

// SendUpdate Send rpc update command to services
func SendUpdate(amqpURI, collection, id, method string, data interface{}, options ...map[string]interface{}) error {
	objectJSON, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := Update{
		ID:         id,
		Cmd:        method,
		Data:       string(objectJSON),
		Collection: collection,
	}
	if len(options) > 0 {
		opts := options[0]
		if recipientsInt, ok := opts["recipients"]; ok {
			if recipients, ok := recipientsInt.([]string); ok {
				msg.Recipients = recipients
			}
		}
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	var consumer *Consumer
	consumerInt, ok := consumers.Load("SendUpdate")
	if !ok {
		consumer, err = NewPublisher(amqpURI, "SendUpdate", Exchange{Name: "csx.updates", Type: "direct", Durable: true})
		if err != nil {
			return err
		}
		consumers.Store("SendUpdate", consumer)
	} else {
		consumer = consumerInt.(*Consumer)
	}
	return consumer.Publish(msgJSON, collection)
}

func (c *Consumer) handleDeliveries(deliveries <-chan amqp.Delivery) {
	for {
		logrus.Info(c.logInfo("handle deliveries"))
		go func() {
			for d := range deliveries {
				for i := 0; i < len(c.handlers); i++ {
					cb := c.handlers[i]
					dv := Delivery(d)
					cb(&dv)
				}
				d.Ack(false)
			}
		}()
		if <-c.done != nil {
			logrus.Error(c.logInfo("deliveries channel closed"))
			if len(c.queue.Keys) > 0 {
				c.queue.BindedKeys = make([]string, len(c.queue.Keys))
				copy(c.queue.BindedKeys, c.queue.Keys)
				c.queue.Keys = nil
			}
			deliveries = c.Reconnect(c.queue.BindedKeys)
			continue
		}
	}
}

// AddConsumeHandler add handler for queue consumer
func (c *Consumer) AddConsumeHandler(handler func(*Delivery)) {
	if len(c.handlers) == 0 {
		c.handlers = []func(*Delivery){handler}
	} else {
		c.handlers = append(c.handlers, handler)
	}
}

// GenerateName generate random name for queue and exchange
func GenerateName(prefix string) string {
	queueName := prefix
	if envName != "" {
		queueName += "." + envName
	}
	if consumerTag != "" {
		queueName += "." + consumerTag
	} else {
		queueName += "." + *strUtil.NewId()
	}
	return queueName
}

// OnUpdates Listener to get models events update, create and delete
func OnUpdates(cb func(data *Delivery), keys []string) {
	if updateExch == "" {
		updateExch = "csx.updates"
	}
	exchange := Exchange{Name: updateExch, Type: "direct", Durable: true}
	queueName := GenerateName("onUpdates")
	queue := Queue{
		Name:        queueName,
		ConsumerTag: queueName,
		AutoDelete:  true,
		Durable:     false,
		Keys:        keys,
	}
	cUpdates, err := GetConsumer(amqpURI, "OnUpdates", &exchange, &queue, cb)
	if err != nil || cUpdates == nil {
		logrus.Error("[OnUpdates] consumer init err: ", err)
		logrus.Warn("[OnUpdates] try reconnect to rabbitmq after ", reconTime)
		time.Sleep(reconTime)
		OnUpdates(cb, keys)
		return
	}

	if cUpdates.handlers == nil || len(cUpdates.handlers) == 0 {
		cUpdates.handlers = make([]func(*Delivery), 0)
		cUpdates.handlers = append(cUpdates.handlers, cb)
	} else {
		cUpdates.handlers = append(cUpdates.handlers, cb)
	}
}

//Shutdown channel on set app time to live
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if c.channel != nil {
		if err := c.channel.Cancel("", true); err != nil {
			logrus.Error(c.logInfo("[Shutdown] consumer cancel err: "), err)
			return err
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			logrus.Error(c.logInfo("[Shutdown] AMQP connection close err: "), err)
			return err
		}
	}

	defer logrus.Warn(c.logInfo("[Shutdown] AMQP shutdown OK"))

	// wait for handle() to exit
	return <-c.done
}
