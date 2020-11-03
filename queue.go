package gorabbit

// QueueSettings holds the settings to declare and consume a Queue
type QueueSettings struct {
	Name        string
	Durable     bool
	AutoDelete  bool
	Exclusive   bool
	NoWait      bool
	Args        map[string]interface{}
	Exchange    string
	BindingKeys []string
}
