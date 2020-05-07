// Package amqpconnector is a wrapper for amqp package
// with reconnect functional support
package amqpconnector

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var consumers sync.Map

// Update struc for send updates msg to services
type Update struct {
	ID      string      `json:"id"`
	ExtID   string      `json:"extId"`
	Cmd     string      `json:"cmd"`
	Data    string      `json:"data"`
	Groups  []string    `json:"groups"`
	ExtData interface{} `json:"extData"`
}

//Consumer structure for NewConsumer result
type Consumer struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	Done       chan error
	Deliveries <-chan amqp.Delivery
	exchange   *Exchange
	queue      *Queue
	uri        string
	name       string // consumer name for logs
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
	NoAck       bool
	NoLocal     bool
	Exclusive   bool
	NoWait      bool
	Args        map[string]interface{}
}

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

// QueueDeclare dial amqp server, decrare echange an queue if set
func (c *Consumer) QueueDeclare(exchange string) error {
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
			return err
		}
		if len(c.queue.Keys) > 0 {
			logrus.Info(c.logInfo("amqp bind keys: "), c.queue.Keys, " to queue: ", c.queue.Name)
			for _, key := range c.queue.Keys {
				if err = c.channel.QueueBind(
					c.queue.Name,   // name of the queue
					key,            // bindingKey
					exchange,       // sourceExchange
					c.queue.NoWait, // noWait
					c.queue.Args,   // arguments
				); err != nil {
					return err
				}
			}
		} else {
			if err = c.channel.QueueBind(
				c.queue.Name,        // name of the queue
				c.queue.ConsumerTag, // bindingKey
				exchange,            // sourceExchange
				c.queue.NoWait,      // noWait
				c.queue.Args,        // arguments
			); err != nil {
				return err
			}
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
			return err
		}
		c.Deliveries = deliveries
	}
	return nil
}

// Connect dial amqp server, decrare echange an queue if set
func (c *Consumer) Connect() error {
	var err error
	logrus.Info(c.logInfo("amqp connect to: "), c.uri)
	c.conn, err = amqp.Dial(c.uri)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-c.Done:
			logrus.Warn(c.logInfo("try reconnect to rabbitmq after: "), reconTime)
			time.Sleep(reconTime)
			c.Reconnect()
			return
		case err := <-c.conn.NotifyClose(make(chan *amqp.Error)):
			logrus.Error(c.logInfo("amqp connection err: "), err)
			time.Sleep(reconTime)
			c.Reconnect()
			return
		}
	}()

	logrus.Info(c.logInfo("amqp get channel"))
	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}
	// declare exchange
	exchange, err := c.ExchangeDeclare()
	if err != nil {
		return err
	}
	// declare queue and bind routing keys
	err = c.QueueDeclare(exchange)
	if err != nil {
		return err
	}

	return nil
}

// Reconnect reconnect to amqp server
func (c *Consumer) Reconnect() {
	reconnectInterval := 30
	time.Sleep(time.Duration(reconnectInterval) * time.Second)
	if err := c.Connect(); err != nil {
		logrus.Error(c.logInfo("consumer reconnect err: "), err.Error(), " next try in ", reconnectInterval, "s")
		c.Reconnect()
	}
}

//NewConsumer create simple consumer for read messages with ack
func NewConsumer(amqpURI, name string, exchange *Exchange, queue *Queue) (*Consumer, error) {
	c := &Consumer{
		exchange: exchange,
		queue:    queue,
		Done:     make(chan error),
		uri:      amqpURI,
		name:     name,
	}
	return c, c.Connect()
}

// NewPublisher create publisher for send amqp messages
func NewPublisher(amqpURI, name string, exchange Exchange) (*Consumer, error) {
	c := &Consumer{
		exchange: &exchange,
		uri:      amqpURI,
		name:     name,
	}
	return c, c.Connect()
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
		c.Reconnect()
		return c.PublishWithHeaders(msg, routingKey, headers)
	}
	return nil
}

// Publish sends messages and reconnects in case of error
func (c *Consumer) Publish(msg []byte, routingKey string) error {
	return c.PublishWithHeaders(msg, routingKey, nil)
}

// GetConsumer get or create publish/consume consumer
func GetConsumer(amqpURI, name string, exchange *Exchange, queue *Queue) (consumer *Consumer, err error) {
	consumerInt, ok := consumers.Load(name)
	if !ok {
		if queue == nil {
			exch := Exchange{}
			if exchange != nil {
				exch = *exchange
			}
			consumer, err = NewPublisher(amqpURI, name, exch)
		} else {
			consumer, err = NewConsumer(amqpURI, name, exchange, queue)
		}
		if err != nil {
			return nil, err
		}
		consumers.Store(name, consumer)
	} else {
		consumer = consumerInt.(*Consumer)
		// declare exchange
		exchange, err := consumer.ExchangeDeclare()
		if err != nil {
			return nil, err
		}
		// declare queue and bind routing keys
		err = consumer.QueueDeclare(exchange)
		if err != nil {
			return nil, err
		}
	}
	return consumer, nil
}

// Publish simple publisher with unique name and one connect
func Publish(amqpURI, consumerName, exchangeName, exchangeType, routingKey string, msg []byte, headers map[string]interface{}) error {
	consumer, err := GetConsumer(amqpURI, consumerName, &Exchange{Name: exchangeName, Type: exchangeType}, nil)
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
func SendUpdate(amqpURI, collection, id, method string, data interface{}) error {
	objectJSON, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := Update{
		ID:   id,
		Cmd:  method,
		Data: string(objectJSON),
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	var consumer *Consumer
	consumerInt, ok := consumers.Load("SendUpdate")
	if !ok {
		consumer, err = NewConsumer(amqpURI, "SendUpdate", &Exchange{Name: "csx.updates", Type: "direct"}, &Queue{})
		if err != nil {
			return err
		}
		consumers.Store("SendUpdate", consumer)
	} else {
		consumer = consumerInt.(*Consumer)
	}
	return consumer.Publish(msgJSON, collection)
}

// OnUpdates Listener to get models events update, create and delete
func OnUpdates(cb func(consumer *Consumer), amqpURI, name string, exchange Exchange, queue Queue) {
	for {
		cUpdates, err := GetConsumer(amqpURI, "OnUpdates", &exchange, &queue)
		if err != nil {
			logrus.Error("[OnUpdates] consumer init err: ", err)
			logrus.Warn("[OnUpdates] try reconnect to rabbitmq after ", reconTime)
			time.Sleep(reconTime)
			continue
		}
		go cb(cUpdates)
		if lifetime > 0 {
			logrus.Info("[OnUpdates] running for: ", lifetime)
			time.Sleep(lifetime)
		} else {
			logrus.Info("[OnUpdates] consumer running success")
			select {
			case <-cUpdates.Done:
				logrus.Warn("[OnUpdates] try reconnect to rabbitmq after ", reconTime)
				time.Sleep(reconTime)
				continue
			}
		}

		logrus.Info("[OnUpdates] try reconnect to rabbitmq after ", reconTime)
		if err := cUpdates.Shutdown(); err != nil {
			logrus.Error("[OnUpdates] consumer shutdown err: ", err)
		}
	}
}

//Shutdown channel on set app time to live
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel("", true); err != nil {
		logrus.Error(c.logInfo("[Shutdown] consumer cancel err: "), err)
		return err
	}

	if err := c.conn.Close(); err != nil {
		logrus.Error(c.logInfo("[Shutdown] AMQP connection close err: "), err)
		return err
	}

	defer logrus.Warn(c.logInfo("[Shutdown] AMQP shutdown OK"))

	// wait for handle() to exit
	return <-c.Done
}
