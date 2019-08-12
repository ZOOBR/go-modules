// Package amqpconnector is a wrapper for amqp package
// with reconnect functional support
package amqpconnector

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

// Update struc for send updates msg to services
type Update struct {
	Id   string `json:"id"`
	Cmd  string `json:"cmd"`
	Data string `json:"data"`
}

//Consumer structure for NewConsumer result
type Consumer struct {
	conn         *amqp.Connection
	Channel      *amqp.Channel
	tag          string
	Done         chan error
	Deliveries   <-chan amqp.Delivery
	Exchange     string
	ExchangeType string
	Uri          string
}

// TODO:: Get from env
var (
	reconTime = time.Second * 20
	lifetime  = time.Duration(0)
)

//NewConsumer create simple consumer for read messages with ack
func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string, options ...map[string]interface{}) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		Channel: nil,
		tag:     ctag,
		Done:    make(chan error),
	}
	queueAutoDelete := false
	queueDurable := true
	var queueKeys []string
	if len(options) > 0 {
		optionsQueue := options[0]
		if val, ok := optionsQueue["queueAutoDelete"]; ok {
			queueAutoDelete = val.(bool)
		}
		if val, ok := optionsQueue["queueDurable"]; ok {
			queueDurable = val.(bool)
		}
		if val, ok := optionsQueue["queueKeys"]; ok {
			queueKeys = val.([]string)
		}
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.Channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.Channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.Channel.QueueDeclare(
		queueName,       // name of the queue
		queueDurable,    // durable
		queueAutoDelete, // delete when unused
		false,           // exclusive
		false,           // noWait
		nil,             // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if len(queueKeys) > 0 {
		for _, key := range queueKeys {
			if err = c.Channel.QueueBind(
				queue.Name, // name of the queue
				key,        // bindingKey
				exchange,   // sourceExchange
				false,      // noWait
				nil,        // arguments
			); err != nil {
				return nil, fmt.Errorf("Queue Bind: %s", err)
			}
		}
	} else {
		if err = c.Channel.QueueBind(
			queue.Name, // name of the queue
			key,        // bindingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			return nil, fmt.Errorf("Queue Bind: %s", err)
		}
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.Channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}
	c.Deliveries = deliveries
	return c, nil
}

func NewPublisher(amqpURI, exchange, exchangeType, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:         nil,
		Channel:      nil,
		tag:          ctag,
		Done:         make(chan error),
		Uri:          amqpURI,
		Exchange:     exchange,
		ExchangeType: exchangeType,
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.Channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.Channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}
	return c, nil
}

func (consumer *Consumer) Publish(msg []byte, rKey ...string) error {
	content := amqp.Publishing{
		ContentType: "text/plain",
		Body:        msg,
	}
	routingKey := ""
	if len(rKey) > 0 {
		routingKey = rKey[0]
	}
	err := consumer.Channel.Publish(consumer.Exchange, routingKey, false, false, content)
	return err
}

// SendUpdate Send rpc update command to services
func SendUpdate(amqpURI, table, id, method string, data interface{}) error {
	objectJSON, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := Update{
		Id:   id,
		Cmd:  method,
		Data: string(objectJSON),
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return Publish(amqpURI, "csx.updates", "direct", table, string(msgJSON), false)
}

// OnUpdates Listener to get models events update, create and delete
func OnUpdates(cb func(consumer *Consumer), queue string, options map[string]interface{}) {
	for {
		updateExch := os.Getenv("EXCHANGE_UPDATES")
		if updateExch == "" {
			updateExch = "csx.updates"
		}
		cUpdates, err := NewConsumer(os.Getenv("AMQP_URI"), updateExch, "direct", queue, "", "csx.updates", options)
		if err != nil {
			log.Printf("error init consumer: %s", err)
			log.Printf("try reconnect to rabbitmq after %s", reconTime)
			time.Sleep(reconTime)
			continue
		}
		go cb(cUpdates)
		if lifetime > 0 {
			log.Printf("running for %s", lifetime)
			time.Sleep(lifetime)
		} else {
			log.Printf("running forever")
			select {
			case <-cUpdates.Done:
				log.Printf("try reconnect to rabbitmq after %s", reconTime)
				time.Sleep(reconTime)
				continue
			}
		}

		log.Printf("shutting down")
		if err := cUpdates.Shutdown(); err != nil {
			log.Fatalf("error during shutdown: %s", err)
		}
	}
}

func Publish(amqpURI, exchange, exchangeType, routingKey, body string, reliable bool) error {

	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchange)
	if err := channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if reliable {
		log.Printf("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}

	log.Printf("declared Exchange, publishing %dB body (%q)", len(body), body)
	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

//Shutdown channel on set app time tio live
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.Channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.Done
}
