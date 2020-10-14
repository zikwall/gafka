package lib

import (
	"context"
)

type (
	Publisher interface {
		Publish(topic string, message string)
	}
	Subscriber interface {
		Subscribe(ctx context.Context, topic string, handler func(string)) error
	}
	PubScriber interface {
		Publisher
		Subscriber
		Close()
	}
)
