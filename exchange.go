package gorabbit

// ExchangeSettings holds the settings to declare an exchange
type ExchangeSettings struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       map[string]interface{}
}
