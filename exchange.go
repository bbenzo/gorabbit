package gorabbit

// ExchangeSettings holds the settings to declare an exchange
type ExchangeSettings struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}
