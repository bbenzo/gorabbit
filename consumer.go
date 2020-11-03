package gorabbit

import "context"

// ConsumerSettings holds the settings for the Queue consumer
type ConsumerSettings struct {
	Consumer    string
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	NoWait      bool
	Args        map[string]interface{}
	Queue       string
	HandlerFunc HandlerFunc
	CancelCtx   context.Context
}
