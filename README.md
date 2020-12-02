# gorabbit

This is a robust [RabbitMQ](https://www.rabbitmq.com/) client written in Go. It is based
on the [streadway/amqp](https://github.com/streadway/amqp) Go library which implements
the AMQP 0-9-1 protocol with [RabbitMQ](https://www.rabbitmq.com/).

## How to use

```go
client := gorabbit.New(ConnectionSettings{
    User:           "guest",
    Password:       "guest",
    Host:           "localhost",
    Port:           5672,
    ReconnectDelay: 0,
    ResendDelay:    0,
}, &zerolog.Logger{})

client.Publish(data, "my-exchange", "my-queue")

client.Consume(context.TODO(), ConsumerSettings{
    Consumer:    "",
    AutoAck:     false,
    Exclusive:   false,
    NoLocal:     false,
    NoWait:      false,
    HandlerFunc: someHandler,
    CancelCtx:   context.TODO(),
})
```

Have a look at the tests to see more examples for applications.

## Run tests

To run the tests, you need to have a RabbitMQ server running on `localhost:5672`. If
you do not have one up and running, you can simply instantiate one via docker

```shell script
docker run --rm -d --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```