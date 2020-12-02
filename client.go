package gorabbit

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
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
	ErrDisconnected = errors.New("disconnected from rabbitmq, trying to reconnect")
)

// HandlerFunc takes a amqp delivery and process it
type HandlerFunc func(msg *amqp.Delivery) error

type client struct {
	connection            *amqp.Connection
	channel               *amqp.Channel
	done                  chan os.Signal
	notifyConnectionError chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	notifyReturn          chan amqp.Return
	isConnected           bool
	reconnectDelay        time.Duration
	resendDelay           time.Duration
	queues                map[string]QueueSettings
	exchanges             map[string]ExchangeSettings
	consumers             []ConsumerSettings
	logger                *zerolog.Logger
	wg                    *sync.WaitGroup
}

// Client defines the methods of the rabbitmq client
type Client interface {
	Publish(data []byte, exchange, key string) error
	UnsafePublish(data []byte, exchange, key string) error
	Consume(ctx context.Context, settings ConsumerSettings) error
	DeclareQueue(settings QueueSettings) (string, error)
	DeleteQueue(name string, ifUnused, ifEmpty, noWait bool) error
	DeclareExchange(settings ExchangeSettings) error
	DeleteExchange(name string, ifUnused, noWait bool) error
	DeclareQueueForExchange(settings QueueSettings) (string, error)
	IsConnected() bool
}

// New Returns a new instance of the Client and opens a connection to the rabbitmq server
func New(settings ConnectionSettings, logger *zerolog.Logger) Client {
	doneChan := make(chan os.Signal, 1)
	signal.Notify(doneChan, syscall.SIGINT, syscall.SIGTERM)

	// set default reconnect delay
	if settings.ReconnectDelay == 0 {
		settings.ReconnectDelay = 5 * time.Second
	}

	// set default resend delay
	if settings.ResendDelay == 0 {
		settings.ResendDelay = time.Second
	}

	// init client
	client := client{
		done:           doneChan,
		reconnectDelay: settings.ReconnectDelay,
		resendDelay:    settings.ResendDelay,
		queues:         make(map[string]QueueSettings),
		exchanges:      make(map[string]ExchangeSettings),
		logger:         logger,
		wg:             &sync.WaitGroup{},
	}

	go client.handleReconnect(address(settings.User, settings.Password, settings.Host, settings.Port))

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
// In addition, it will redeclare all previously declared exchanges and queues and start all registered consumers
func (c *client) handleReconnect(addr string) {
	for {
		c.isConnected = false
		t := time.Now()

		c.logger.Info().Msgf("attempting to connect to rabbitmq: %s\n", addr)

		var retries int
		for !c.connect(addr) {
			select {
			case <-c.done:
				c.logger.Error().Msg("reconnect failed. closing rabbitmq client")
				c.isConnected = false
				return
			case <-time.After(c.reconnectDelay + time.Duration(retries)*time.Second):
				c.logger.Error().Msg("disconnected from rabbitmq and failed to connect")
				retries++
			}
		}

		c.logger.Info().Msgf("connected to rabbitmq in: %v ms", time.Since(t).Milliseconds())

		err := c.handleReconnectConsumers()
		if err != nil {
			c.logger.Error().Msgf("failed to reconnect consumers: %v", err.Error())
			c.isConnected = false
			continue
		}

		select {
		case <-c.done:
			c.logger.Error().Msg("reconnect failed. closing rabbitmq client")
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

	err = c.startConsumers()
	if err != nil {
		return err
	}

	return nil
}

func (c *client) declareRegisteredExchanges() error {
	for _, settings := range c.exchanges {
		err := c.DeclareExchange(settings)
		if err != nil {
			return errors.New("failed to declare exchange: " + settings.Name)
		}
	}

	c.logger.Info().Msgf("started up %v registered exchanges", len(c.exchanges))
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

	c.logger.Info().Msgf("started up %v registered queues", len(c.queues))
	return nil
}

func (c *client) startConsumers() error {
	for _, settings := range c.consumers {
		err := c.Consume(settings.CancelCtx, settings)
		if err != nil {
			return errors.New("failed to register consumer " + settings.Consumer + " for queue " + settings.Queue)
		}
	}

	c.logger.Info().Msgf("started up %v registered consumers", len(c.consumers))
	return nil
}

// connect will make a single attempt to connect to RabbitMQ
func (c *client) connect(addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		c.logger.Error().Msgf("failed to dial rabbitmq server: %v", err)
		return false
	}

	ch, err := conn.Channel()
	if err != nil {
		c.logger.Error().Msgf("failed connecting to channel: %v", err)
		return false
	}

	err = ch.Confirm(false)
	if err != nil {
		c.logger.Error().Msgf("failed setting channel on confirm mode: %v", err)
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
	c.notifyReturn = make(chan amqp.Return)
	c.channel.NotifyReturn(c.notifyReturn)
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
		case returned := <-c.notifyReturn:
			c.logger.Warn().Msgf("failed to publish message. there seems to be no listening queue. message %v dropped", returned.MessageId)
			return nil
		case <-time.After(c.resendDelay * time.Second):
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
func (c *client) Consume(cancelCtx context.Context, settings ConsumerSettings) error {
	for {
		if c.isConnected {
			break
		}
		c.logger.Error().Msgf("trying to reconnect consumer %v to queue %v ...", settings.Consumer, settings.Queue)
		time.Sleep(1 * time.Second)
	}

	err := c.channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

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

	go func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					c.notifyConnectionError <- &amqp.Error{
						Code:    999,
						Reason:  "consumer connection failed",
						Server:  false,
						Recover: true,
					}
					return
				}

				err := settings.HandlerFunc(&msg)
				if err != nil {
					c.logger.Error().Msgf("Error during msg handling: %v", err.Error())
				}
			}
		}
	}()

	return nil
}

// DeclareQueue creates a Queue if non exists
func (c *client) DeclareQueue(settings QueueSettings) (string, error) {
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
func (c *client) DeclareQueueForExchange(settings QueueSettings) (string, error) {
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
func (c *client) DeclareExchange(settings ExchangeSettings) error {
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

// IsConnected returns if the client is connected to the rabbitmq server
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

	c.logger.Info().Msgf(`deleted Queue. purged %v messages`, purgedMessages)
	return nil
}

// DeleteExchange deletes an exchange
func (c *client) DeleteExchange(name string, ifUnused, noWait bool) error {
	err := c.channel.ExchangeDelete(name, ifUnused, noWait)
	if err != nil {
		return errors.Wrap(err, "failed to delete exchange")
	}

	delete(c.exchanges, name)

	c.logger.Info().Msgf(`deleted exchange %s`, name)
	return err
}
