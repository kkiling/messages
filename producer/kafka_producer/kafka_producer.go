package kafka_producer

import (
	"context"
	"fmt"
	"github.com/kkiling/messages/producer"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"sync"
	"time"
)

type Config struct {
	BootstrapServers []string
}

type Producer struct {
	producer map[string]*kafka.Writer
	cfg      Config
	mu       sync.RWMutex
}

func NewProducer(cfg Config) *Producer {
	return &Producer{
		producer: make(map[string]*kafka.Writer),
		cfg:      cfg,
	}
}

func (k *Producer) createProducer(topic string) *kafka.Writer {
	// Создание продюсера
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          k.cfg.BootstrapServers,
		Topic:            topic,
		CompressionCodec: &compress.GzipCodec,
		/*RequiredAcks: Это количество подтверждений,
		которые kafka.Writer должен получить от брокеров,
		прежде чем считать сообщение успешно отправленным.
		По умолчанию требуется подтверждение от всех брокеров.*/
		RequiredAcks: -1, //all
		/* Если этот параметр установлен в true, kafka.Writer будет
		отправлять сообщения асинхронно, что может улучшить производительность
		за счет параллелизма. По умолчанию он установлен в false,
		что означает синхронную отправку сообщений.*/
		Async: false,
		/*
			MaxAttempts: Это максимальное количество попыток отправить каждое сообщение.
			Если после этого количество попыток сообщение не может быть успешно отправлено,
			оно будет отброшено.
		*/
		MaxAttempts: 5,
		/*
			Balancer: Это объект, реализующий стратегию балансировки сообщений между доступными
			партициями в Kafka. По умолчанию используется &kafka.LeastBytes{},
			который пытается распределить сообщения таким образом,
			 чтобы партиции были равномерно заполнены.*/
		Balancer: &kafka.LeastBytes{},
		/*
			BatchTimeout: Это продолжительность ожидания перед отправкой пакета сообщений.
			Если кафка-писатель получает сообщения быстрее, чем успевает их отправлять,
			он будет накапливать их в пакеты и отправлять вместе, что может улучшить
			производительность за счет уменьшения количества отдельных операций записи.
		*/
		BatchTimeout: 100 * time.Millisecond,
	})
	writer.AllowAutoTopicCreation = false
	return writer
}

func mapKafkaMessages(messages ...producer.Message) []kafka.Message {
	msgs := make([]kafka.Message, 0, len(messages))
	for _, msg := range messages {
		headers := make([]kafka.Header, 0, len(msg.Headers))
		for _, h := range msg.Headers {
			headers = append(headers, kafka.Header{
				Key:   h.Key,
				Value: h.Value,
			})
		}
		msgs = append(msgs, kafka.Message{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		})
	}
	return msgs
}

func mapKafkaError(err error) error {
	if err == nil {
		return nil
	}
	if kafkaError, ok := err.(kafka.Error); ok {
		// Обработка ошибок Kafka
		switch kafkaError {
		case kafka.UnknownTopicOrPartition:
			return producer.ErrTopicNotFound
		default:
			// Обработка других ошибок Kafka
			return fmt.Errorf("unknow kafka error: %v\n", err)
		}
	} else {
		return fmt.Errorf("unknow error: %v\n", err)
	}
}

func (k *Producer) ProduceMsg(ctx context.Context, topic string, messages ...producer.Message) error {
	k.mu.RLock()
	writer, ok := k.producer[topic]
	k.mu.RUnlock()

	if !ok {
		k.mu.Lock()
		writer, ok = k.producer[topic]
		if !ok {
			writer = k.createProducer(topic)
			k.producer[topic] = writer
		}
		k.mu.Unlock()
	}

	msgs := mapKafkaMessages(messages...)
	err := writer.WriteMessages(ctx, msgs...)

	return mapKafkaError(err)
}

func (k *Producer) Shutdown() error {
	return nil
}
