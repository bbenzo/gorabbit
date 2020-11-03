package gorabbit

import "context"

// ConsumerSettings holds the settings for the queue consumer
type ConsumerSettings struct {
	consumer    string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        map[string]interface{}
	queue       string
	handlerFunc HandlerFunc
	cancelCtx   context.Context
}
