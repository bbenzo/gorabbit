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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrDisconnected = errors.New("disconnected from rabbitmq, trying to reconnect")
)

type HandlerFunc func(msg *amqp.Delivery) error

type client struct {
	connection            *amqp.Connection
	channel               *amqp.Channel
	done                  chan os.Signal
	notifyConnectionError chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	isConnected           bool
	threads               int
	reconnectDelay        time.Duration
	resendDelay           time.Duration
	wg                    *sync.WaitGroup
}

type Client interface {
	Publish(data []byte, exchange, queue string) error
	UnsafePublish(data []byte, exchange, key string) error
	Consume(ctx context.Context, handlerFunc HandlerFunc, queue string) error
	DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) (string, error)
	DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) error
	DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error
	DeleteExchange(name string, ifUnused, noWait bool) error
	DeclareQueueForExchange(name, exchange string, bindingKeys []string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) (string, error)
	IsConnected() bool
}

func New(user, password, host string, port int, reconnectDelay, resendDelay time.Duration) Client {
	threads := runtime.GOMAXPROCS(0)
	if numCPU := runtime.NumCPU(); numCPU > threads {
		threads = numCPU
	}

	doneChan := make(chan os.Signal, 1)
	signal.Notify(doneChan, syscall.SIGINT, syscall.SIGTERM)

	if reconnectDelay == 0 {
		reconnectDelay = 5 * time.Second
	}

	if resendDelay == 0 {
		resendDelay = time.Second
	}

	client := client{
		threads:        threads,
		done:           doneChan,
		reconnectDelay: reconnectDelay,
		resendDelay:    resendDelay,
		wg:             &sync.WaitGroup{},
	}
	client.wg.Add(threads)

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

// handleReconnect will wait for a connection error on
// notifyConnectionError, and then continuously attempt to reconnect.
func (c *client) handleReconnect(addr string) {
	for {
		c.isConnected = false
		t := time.Now()

		fmt.Printf("Attempting to connect to rabbitMQ: %s\n", addr)

		var retryCount int
		for !c.connect(addr) {
			select {
			case <-c.done:
				c.isConnected = false
				return
			case <-time.After(c.reconnectDelay + time.Duration(retryCount)*time.Second):
				log.Println("disconnected from rabbitMQ and failed to connect")
				retryCount++
			}
		}

		log.Println(fmt.Sprintf("connected to rabbitMQ in: %v ms", time.Since(t).Milliseconds()))
		select {
		case <-c.done:
			c.isConnected = false
			return
		case <-c.notifyConnectionError:
		}
	}
}

// connect will make a single attempt to connect to
// RabbitMq. It returns the success of the attempt.
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

// changeConnection takes a new connection and updates the channel listeners to reflect this.
func (c *client) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.channel = channel
	c.notifyConnectionError = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation)
	c.channel.NotifyClose(c.notifyConnectionError)
	c.channel.NotifyPublish(c.notifyConfirm)
}

// Publish will publish the data to an exchange with a routingKey.
// If no confirms are received until within the resendTimeout,
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

// Consume will consume a queue and apply a HandlerFunc to the received message
func (c *client) Consume(cancelCtx context.Context, handlerFunc HandlerFunc, queue string) error {
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

	for i := 1; i <= c.threads; i++ {
		msgs, err := c.channel.Consume(
			queue,
			"",    // Consumer
			false, // Auto-Ack
			false, // Exclusive
			false, // No-local
			false, // No-Wait
			nil,   // Args
		)
		if err != nil {
			return err
		}

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

					err := handlerFunc(&msg)
					if err != nil {
						log.Println(fmt.Sprintf("Error during msg handling: %v", err.Error()))
					}
				}
			}
		}()

	}

	c.wg.Wait()

	if connectionDropped {
		return ErrDisconnected
	}

	return nil
}

func (c *client) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) (string, error) {
	queue, err := c.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return "", errors.Wrap(err, "failed to declare queue")
	}

	return queue.Name, err
}

func (c *client) DeclareQueueForExchange(queueName, exchange string, bindingKeys []string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) (string, error) {
	queue, err := c.channel.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return "", errors.Wrap(err, "failed to declare queue")
	}

	for _, key := range bindingKeys {
		err = c.channel.QueueBind(queue.Name, key, exchange, noWait, args)
		if err != nil {
			return "", errors.Wrap(err, "failed to bind queue to exchange "+exchange)
		}
	}

	// if no specific binding keys are provided, listen to all messages
	if len(bindingKeys) == 0 {
		err = c.channel.QueueBind(queue.Name, "", exchange, noWait, args)
		if err != nil {
			return "", errors.Wrap(err, "failed to bind queue to exchange "+exchange)
		}
	}

	return queue.Name, err
}

func (c *client) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error {
	err := c.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		return errors.Wrap(err, "failed to declare exchange")
	}
	return err
}

func (c *client) IsConnected() bool {
	return c.isConnected
}

func (c *client) DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) error {
	purgedMessages, err := c.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)

	log.Println(fmt.Sprintf(`deleted queue. purged %v messages`, purgedMessages))
	return err
}

func (c *client) DeleteExchange(name string, ifUnused, noWait bool) error {
	err := c.channel.ExchangeDelete(name, ifUnused, noWait)

	log.Println(fmt.Sprintf(`deleted exchange %s`, name))
	return err
}
