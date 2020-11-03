package gorabbit

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrDisconnected = errors.New("disconnected from rabbitMQ, trying to reconnect")
)

// HandlerFunc takes a amqp delivery and process it
type HandlerFunc func(msg *amqp.Delivery) error

type client struct {
	connection            *amqp.Connection
	channel               *amqp.Channel
	done                  chan os.Signal
	notifyConnectionError chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	isConnected           bool
	reconnectDelay        time.Duration
	resendDelay           time.Duration
	queues                map[string]*QueueSettings
	exchanges             map[string]*ExchangeSettings
	consumers             []*ConsumerSettings
	wg                    *sync.WaitGroup
}

// Client defines the methods of the rabbitMQ client
type Client interface {
	Publish(data []byte, exchange, key string) error
	UnsafePublish(data []byte, exchange, key string) error
	Consume(ctx context.Context, settings *ConsumerSettings) error
	DeclareQueue(settings *QueueSettings) (string, error)
	DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) error
	DeclareExchange(settings *ExchangeSettings) error
	DeleteExchange(name string, ifUnused, noWait bool) error
	DeclareQueueForExchange(settings *QueueSettings) (string, error)
	IsConnected() bool
}

// New Returns a new instance of the Client and opens a connection to the rabbitmq server
func New(user, password, host string, port int, reconnectDelay, resendDelay time.Duration) Client {
	doneChan := make(chan os.Signal, 1)
	signal.Notify(doneChan, syscall.SIGINT, syscall.SIGTERM)

	// set default reconnect delay
	if reconnectDelay == 0 {
		reconnectDelay = 5 * time.Second
	}

	// set default resend delay
	if resendDelay == 0 {
		resendDelay = time.Second
	}

	// init client
	client := client{
		done:           doneChan,
		reconnectDelay: reconnectDelay,
		resendDelay:    resendDelay,
		queues:         make(map[string]*QueueSettings),
		exchanges:      make(map[string]*ExchangeSettings),
		wg:             &sync.WaitGroup{},
	}

	go client.handleReconnect(address(user, password, host, port))

	return &client
}

// address builds the rabbitmq address
func address(user, password, host string, port int) string {
	var sb strings.Builder

	sb.WriteString("amqp://")
	if len(user) > 0 || len(password) > 0 {
		sb.WriteString(url.QueryEscape(user))
		sb.WriteString(":")
		sb.WriteString(url.QueryEscape(password))
		sb.WriteString("@")
	}
	sb.WriteString(url.QueryEscape(host))
	sb.WriteString(":")
	sb.WriteString(strconv.Itoa(port))
	sb.WriteString("/")

	return sb.String()
}

// handleReconnect will wait for a connection error on notifyConnectionError and then attempt to reconnect.
// In addition, it will redeclare all previously declared exchanges and queues and start
// all registered consumers
func (c *client) handleReconnect(addr string) {
	for {
		c.isConnected = false
		t := time.Now()

		fmt.Printf("attempting to connect to rabbitMQ: %s\n", addr)

		var retries int
		for !c.connect(addr) {
			select {
			case <-c.done:
				c.isConnected = false
				return
			case <-time.After(c.reconnectDelay + time.Duration(retries)*time.Second):
				log.Println("disconnected from rabbitMQ and failed to connect")
				retries++
			}
		}

		log.Println(fmt.Sprintf("connected to rabbitMQ in: %v ms", time.Since(t).Milliseconds()))

		err := c.handleReconnectConsumers()
		if err != nil {
			log.Printf("failed to reconnect consumers: %v", err.Error())
			c.isConnected = false
			continue
		}

		select {
		case <-c.done:
			c.isConnected = false
			return
		case <-c.notifyConnectionError:
		}
	}
}

func (c *client) handleReconnectConsumers() error {
	err := c.declareRegisteredExchanges()
	if err != nil {
		return err
	}

	err = c.declareRegisteredQueues()
	if err != nil {
		return err
	}

	c.startConsumers()

	return nil
}

func (c *client) declareRegisteredExchanges() error {
	for _, settings := range c.exchanges {
		err := c.DeclareExchange(settings)
		if err != nil {
			return errors.New("failed to declare exchange: " + settings.Name)
		}
	}

	log.Println(fmt.Sprintf("started up %v registered exchanges", len(c.exchanges)))
	return nil
}

func (c *client) declareRegisteredQueues() error {
	for name, settings := range c.queues {
		if settings.Exchange != "" {
			_, err := c.DeclareQueueForExchange(settings)
			if err != nil {
				return errors.New("failed to declare Queue " + name)
			}

		} else {
			_, err := c.DeclareQueue(settings)
			if err != nil {
				return errors.New("failed to declare Queue " + name)
			}
		}
	}

	log.Println(fmt.Sprintf("started up %v registered queues", len(c.queues)))
	return nil
}

func (c *client) startConsumers() {
	for _, settings := range c.consumers {
		go c.Consume(settings.CancelCtx, settings) // TODO handle errors with chan and let reconnect fail, if err != nil
	}

	log.Println(fmt.Sprintf("started up %v registered consumers", len(c.consumers)))
}

// connect will make a single attempt to connect to RabbitMQ
func (c *client) connect(addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		log.Println(fmt.Sprintf("failed to dial rabbitMQ server: %v", err))
		return false
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Println(fmt.Sprintf("failed connecting to channel: %v", err))
		return false
	}

	err = ch.Confirm(false)
	if err != nil {
		log.Println(fmt.Sprintf("failed setting channel on confirm mode: %v", err))
		return false
	}

	c.changeConnection(conn, ch)
	c.isConnected = true
	return true
}

// changeConnection takes a new connection and updates the channel listeners
func (c *client) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.channel = channel
	c.notifyConnectionError = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation)
	c.channel.NotifyClose(c.notifyConnectionError)
	c.channel.NotifyPublish(c.notifyConfirm)
}

// Publish will publish the data to an exchange with a routingKey.
// If no confirms are received until within the resendDelay,
// it continuously resends messages until a confirmation is received.
// This will block until the server sends a confirm.
func (c *client) Publish(data []byte, exchange, routingKey string) error {
	if !c.isConnected {
		return errors.New("failed to publish: not connected")
	}
	for {
		err := c.UnsafePublish(data, exchange, routingKey)
		if err != nil {
			if err == ErrDisconnected {
				continue
			}
			return err
		}

		select {
		case confirm := <-c.notifyConfirm:
			if confirm.Ack {
				return nil
			}
		case <-time.After(c.resendDelay):
		}
	}
}

// UnsafePublish will publish the data to an exchange with a routingKey
// without checking for confirmation. It returns an error if it fails to
// connect. No guarantees are provided for whether the server will
// receive the message.
func (c *client) UnsafePublish(data []byte, exchange, routingKey string) error {
	if !c.isConnected {
		return ErrDisconnected
	}

	return c.channel.Publish(
		exchange,   // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

// Consume will consume a Queue and apply a HandlerFunc to the received message
func (c *client) Consume(cancelCtx context.Context, settings *ConsumerSettings) error {
	if settings == nil {
		return errors.New("missing consumer settings")
	}

	for {
		if c.isConnected {
			break
		}
		time.Sleep(1 * time.Second)
	}

	err := c.channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

	var connectionDropped bool

	msgs, err := c.channel.Consume(
		settings.Queue,
		settings.Consumer,
		settings.AutoAck,
		settings.Exclusive,
		settings.NoLocal,
		settings.NoWait,
		settings.Args,
	)
	if err != nil {
		return err
	}

	c.consumers = append(c.consumers, settings)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-cancelCtx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					connectionDropped = true
					return
				}

				err := settings.HandlerFunc(&msg)
				if err != nil {
					log.Println(fmt.Sprintf("Error during msg handling: %v", err.Error()))
				}
			}
		}
	}()

	c.wg.Wait()

	if connectionDropped {
		return ErrDisconnected
	}

	return nil
}

// DeclareQueue creates a Queue if non exists
func (c *client) DeclareQueue(settings *QueueSettings) (string, error) {
	queue, err := c.channel.QueueDeclare(
		settings.Name,
		settings.Durable,
		settings.AutoDelete,
		settings.Exclusive,
		settings.NoWait,
		settings.Args,
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to declare Queue")
	}

	c.queues[settings.Name] = settings
	return queue.Name, err
}

// DeclareQueueForExchange creates a Queue for an exchange, if non exists
func (c *client) DeclareQueueForExchange(settings *QueueSettings) (string, error) {
	queue, err := c.channel.QueueDeclare(
		settings.Name,
		settings.Durable,
		settings.AutoDelete,
		settings.Exclusive,
		settings.NoWait,
		settings.Args,
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to declare Queue")
	}

	for _, key := range settings.BindingKeys {
		err = c.channel.QueueBind(queue.Name, key, settings.Exchange, settings.NoWait, settings.Args)
		if err != nil {
			return "", errors.Wrap(err, "failed to bind Queue to exchange "+settings.Exchange)
		}
	}

	// if no specific binding keys are provided, listen to all messages
	if len(settings.BindingKeys) == 0 {
		err = c.channel.QueueBind(queue.Name, "", settings.Exchange, settings.NoWait, settings.Args)
		if err != nil {
			return "", errors.Wrap(err, "failed to bind Queue to exchange "+settings.Exchange)
		}
	}

	c.queues[settings.Name] = settings

	return queue.Name, err
}

// DeclareExchange creates an exchange if it does not already exist
func (c *client) DeclareExchange(settings *ExchangeSettings) error {
	err := c.channel.ExchangeDeclare(
		settings.Name,
		settings.Kind,
		settings.Durable,
		settings.AutoDelete,
		settings.Internal,
		settings.NoWait,
		settings.Args,
	)
	if err != nil {
		return errors.Wrap(err, "failed to declare exchange")
	}

	c.exchanges[settings.Name] = settings
	return err
}

// IsConnected returns if the client is connected to the rabbitMQ server
func (c *client) IsConnected() bool {
	return c.isConnected
}

// DeleteQueue deletes a Queue
func (c *client) DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) error {
	purgedMessages, err := c.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
	if err != nil {
		return errors.Wrap(err, "failed to delete Queue")
	}

	delete(c.queues, name)

	log.Println(fmt.Sprintf(`deleted Queue. purged %v messages`, purgedMessages))
	return nil
}

// DeleteExchange deletes an exchange
func (c *client) DeleteExchange(name string, ifUnused, noWait bool) error {
	err := c.channel.ExchangeDelete(name, ifUnused, noWait)
	if err != nil {
		return errors.Wrap(err, "failed to delete exchange")
	}

	delete(c.exchanges, name)

	log.Println(fmt.Sprintf(`deleted exchange %s`, name))
	return err
}
