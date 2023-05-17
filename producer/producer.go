package producer

import (
	"context"
	"fmt"
)

var (
	ErrTopicNotFound = fmt.Errorf("topic not found")
)

// Header represents a single entry in a list of record headers.
type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Key     []byte
	Value   []byte
	Headers []Header
}

//go:generate moq -out producer_mock.go . IProducer
type IProducer interface {
	ProduceMsg(ctx context.Context, topic string, messages ...Message) error
	Shutdown() error
}
