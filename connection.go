package gorabbit

import "time"

type ConnectionSettings struct {
	User string
	Password string
	Host string
	Port int
	ReconnectDelay time.Duration
	ResendDelay time.Duration
}
