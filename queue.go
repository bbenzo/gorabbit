package gorabbit

// QueueSettings holds the settings to declare and consume a queue
type QueueSettings struct {
	name             string
	durable          bool
	autoDelete       bool
	exclusive        bool
	noWait           bool
	args             map[string]interface{}
	exchange         string
	bindingKeys      []string
}
