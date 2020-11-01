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
	process.Signal(os.Interrupt)

	time.Sleep(500 * time.Millisecond)

	assert.False(t, client.IsConnected())
}

func TestDeclareAndDeleteQueue(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	name := "unit-test-q-1"
	queue, err := client.DeclareQueue(name, false, false, false, false, nil)

	assert.Nil(t, err)
	assert.Equal(t, name, queue)

	err = client.DeleteQueue(queue, false, true, false)

	assert.Nil(t, err)
}

func TestPublishToQueueAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	name := "unit-test-q-2"
	queue, err := client.DeclareQueue(name, false, false, false, true, nil)

	assert.Nil(t, err)
	assert.Equal(t, name, queue)

	err = client.Publish([]byte(validPayloadOne), "", queue)

	assert.Nil(t, err)

	go client.Consume(context.TODO(), testHandler(t), queue)

	time.Sleep(2 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(queue, false, true, false)

	assert.Nil(t, err)
}

func TestPublishToDirectExchangeWithRoutingKeyAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-direct"
	err := client.DeclareExchange(exchangeName, "direct", false, false, false, true, nil)

	assert.Nil(t, err)

	bindKey := "test-bind-key"

	consumeQueue, err := client.DeclareQueueForExchange("", exchangeName, []string{bindKey}, false, false, false, true, nil)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, bindKey)

	assert.Nil(t, err)

	err = client.Publish([]byte(invalidPayload), exchangeName, "dont consume me") // this should not be consumed

	assert.Nil(t, err)

	go client.Consume(context.TODO(), testHandler(t), consumeQueue)

	time.Sleep(2 * time.Second)

	assert.Nil(t, err)

	err = client.DeleteQueue(consumeQueue, false, true, false)

	assert.Nil(t, err)

	err = client.DeleteExchange(exchangeName, false, false)

	assert.Nil(t, err)
}

func TestPublishToFanoutExchangeAndConsume(t *testing.T) {
	client := createClient()

	time.Sleep(2 * time.Second)

	exchangeName := "unit-test-exchange-fanout"
	err := client.DeclareExchange(exchangeName, "fanout", false, false, false, true, nil)

	assert.Nil(t, err)

	consumeQueue, err := client.DeclareQueueForExchange("", exchangeName, []string{}, false, false, false, true, nil)

	assert.Nil(t, err)

	err = client.Publish([]byte(validPayloadOne), exchangeName, "")
	err = client.Publish([]byte(validPayloadTwo), exchangeName, "")

	assert.Nil(t, err)

	go client.Consume(context.TODO(), testHandler(t), consumeQueue)

	time.Sleep(2 * time.Second)

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
