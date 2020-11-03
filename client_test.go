// +build integration

package gorabbit

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// ATTENTION: These tests need a rabbitMQ instance running on localhost:5672

const (
	validPayloadOne = "me likes"
	validPayloadTwo = "me likes too"
	invalidPayload  = "me don't like"
)

func TestConnection(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	assert.True(t, client.IsConnected())

	process, _ := os.FindProcess(os.Getpid())
	err := process.Signal(os.Interrupt)

	assert.Nil(t, err)

	time.Sleep(500 * time.Millisecond)

	assert.False(t, client.IsConnected())
}

func TestDeclareAndDeleteQueue(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	name := "unit-test-queue-1"
	queueSettings := &QueueSettings{
		name:       name,
		durable:    false,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}
	queue, err := client.DeclareQueue(queueSettings)

	assert.Nil(t, err)
	assert.Equal(t, name, queue)

	err = client.DeleteQueue(queue, false, true, false)

	assert.Nil(t, err)
}

func TestPublishToQueueAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	name := "unit-test-queue-2"
	queueSettings := &QueueSettings{
		name:       name,
		durable:    false,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}
	queue, err := client.DeclareQueue(queueSettings)

	assert.Nil(t, err)
	assert.Equal(t, name, queue)

	err = client.Publish([]byte(validPayloadOne), "", queue)

	assert.Nil(t, err)

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:  "",
		autoAck:   false,
		exclusive: false,
		noLocal:   false,
		noWait:    false,
		args:      nil,
	})

	assert.Nil(t, err)

	time.Sleep(2 * time.Second)

	err = client.DeleteQueue(queue, false, true, false)

	assert.Nil(t, err)
}

func TestPublishToFanoutExchangeAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-fanout"
	exchangeSettings := &ExchangeSettings{
		name:       exchangeName,
		kind:       "fanout",
		durable:    false,
		autoDelete: false,
		internal:   false,
		noWait:     true,
		args:       nil,
	}

	err := client.DeclareExchange(exchangeSettings)

	assert.Nil(t, err)

	queueSettings := &QueueSettings{
		name:        "",
		durable:     false,
		autoDelete:  false,
		exclusive:   false,
		noWait:      true,
		args:        nil,
		exchange:    exchangeName,
		bindingKeys: []string{},
	}
	consumeQueue, err := client.DeclareQueueForExchange(queueSettings)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, "")

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadTwo), exchangeName, "")

	assert.Nil(t, err)

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		handlerFunc: testHandler(t),
		cancelCtx:   context.TODO(),
		args:        nil,
	})

	time.Sleep(2 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(consumeQueue, false, true, false)

	assert.Nil(t, err)

	err = client.DeleteExchange(exchangeName, false, false)

	assert.Nil(t, err)
}

func TestPublishToDirectExchangeWithRoutingKeyAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-direct"
	exchangeSettings := &ExchangeSettings{
		name:       exchangeName,
		kind:       "direct",
		durable:    false,
		autoDelete: false,
		internal:   false,
		noWait:     true,
		args:       nil,
	}

	err := client.DeclareExchange(exchangeSettings)

	assert.Nil(t, err)

	bindKey := "test-bind-key"

	queueSettings := &QueueSettings{
		name:        "",
		durable:     false,
		autoDelete:  false,
		exclusive:   false,
		noWait:      true,
		args:        nil,
		exchange:    exchangeName,
		bindingKeys: []string{bindKey},
	}
	consumeQueue, err := client.DeclareQueueForExchange(queueSettings)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, bindKey)

	assert.Nil(t, err)

	err = client.Publish([]byte(invalidPayload), exchangeName, "dont consume me") // this should not be consumed

	assert.Nil(t, err)

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		args:        nil,
		handlerFunc: testHandler(t),
		cancelCtx:   context.TODO(),
	})

	time.Sleep(2 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(consumeQueue, false, true, false)

	assert.Nil(t, err)

	err = client.DeleteExchange(exchangeName, false, false)

	assert.Nil(t, err)
}

func TestPublishToTopicExchangeWithRoutingKeyAndConsumeWithWildcard(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-topic"
	exchangeSettings := &ExchangeSettings{
		name:       exchangeName,
		kind:       "topic",
		durable:    false,
		autoDelete: false,
		internal:   false,
		noWait:     true,
		args:       nil,
	}
	err := client.DeclareExchange(exchangeSettings)

	assert.Nil(t, err)

	bindKeyOne := "test.one"
	bindKeyTwo := "test.two"

	queueSettings := &QueueSettings{
		name:        "",
		durable:     false,
		autoDelete:  false,
		exclusive:   false,
		noWait:      true,
		args:        nil,
		exchange:    exchangeName,
		bindingKeys: []string{bindKeyOne, bindKeyTwo},
	}
	consumeQueue, err := client.DeclareQueueForExchange(queueSettings)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, bindKeyOne)
	err = client.Publish([]byte(validPayloadTwo), exchangeName, bindKeyTwo)

	assert.Nil(t, err)

	err = client.Publish([]byte(invalidPayload), exchangeName, "dont consume me") // this should not be consumed

	assert.Nil(t, err)

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		args:        nil,
		handlerFunc: testHandler(t),
		cancelCtx:   context.TODO(),
	})

	time.Sleep(2 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(consumeQueue, false, true, false)

	assert.Nil(t, err)

	err = client.DeleteExchange(exchangeName, false, false)

	assert.Nil(t, err)
}

func TestPublishToFanoutExchangeAndConsumeWithMultipleConsumers(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-fanout-multi"
	exchangeSettings := &ExchangeSettings{
		name:       exchangeName,
		kind:       "fanout",
		durable:    false,
		autoDelete: false,
		internal:   false,
		noWait:     true,
		args:       nil,
	}

	err := client.DeclareExchange(exchangeSettings)

	assert.Nil(t, err)

	queueSettings := &QueueSettings{
		name:        "",
		durable:     false,
		autoDelete:  false,
		exclusive:   false,
		noWait:      true,
		args:        nil,
		exchange:    exchangeName,
		bindingKeys: []string{},
	}
	consumeQueue, err := client.DeclareQueueForExchange(queueSettings)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, "")

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadTwo), exchangeName, "")

	assert.Nil(t, err)

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		handlerFunc: testHandler(t),
		cancelCtx:   context.TODO(),
		args:        nil,
	})

	go client.Consume(context.TODO(), &ConsumerSettings{
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		handlerFunc: testHandler(t),
		cancelCtx:   context.TODO(),
		args:        nil,
	})

	time.Sleep(2000 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(consumeQueue, false, true, false)

	assert.Nil(t, err)

	err = client.DeleteExchange(exchangeName, false, false)

	assert.Nil(t, err)
}

func createClient() Client {
	client := New("guest", "guest", "localhost", 5672, 0, 0)
	return client
}

func testHandler(t *testing.T) HandlerFunc {
	return func(msg *amqp.Delivery) error {

		switch string(msg.Body) {
		case validPayloadOne, validPayloadTwo:
			fmt.Println("received msg with body: " + string(msg.Body))
			msg.Ack(true)
			return nil
		default:
			errMsg := "don't like this message"
			t.Error(errMsg)
			msg.Ack(true)
			return errors.New(errMsg)
		}
	}
}
