package publisher

import (
	"context"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/util"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
)

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

type Publisher struct {
	kafkaSink protocol.Sink
}

func (d *Publisher) Close(ctx context.Context) error {
	return d.kafkaSink.Close(ctx)
}

func NewPublisher(kafkaSink protocol.Sink) *Publisher {
	return &Publisher{kafkaSink: kafkaSink}
}

func (d *Publisher) Publish(messageProvider protocol.MessageProvider) error {
	message, err := messageProvider.Get()
	if err != nil {
		return err
	}

	return d.kafkaSink.Sink(message)
}

type KafkaSink struct {
	writer *kafka.Writer
}

func (k *KafkaSink) Close(ctx context.Context) error {
	doneChan := util.Wait(func() {
		err := k.writer.Close()
		if err != nil {
			log.Println(err)
		}
	})

	select {
	case <-doneChan:
		fmt.Println("kafka publisher closed")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewKafkaSink(broker []string, topic string) *KafkaSink {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  broker,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	return &KafkaSink{
		writer: writer,
	}
}

func (k *KafkaSink) Sink(message *protocol.Message) error {
	key, err := proto.Marshal(message.Key)
	if err != nil {
		return err
	}

	value, err := proto.Marshal(message.Value)
	if err != nil {
		return err
	}

	kafkaMessage := kafka.Message{Key: key, Value: value}
	return k.writer.WriteMessages(context.Background(), kafkaMessage)
}

type ConsoleSink struct {
}

func (c *ConsoleSink) Close(ctx context.Context) error {
	return nil
}

func (c *ConsoleSink) Sink(message *protocol.Message) error {
	logger.Println(message.Key)
	logger.Println(message.Value)
	return nil
}

type DummySink struct {
}

func (d *DummySink) Close(ctx context.Context) error {
	return nil
}

func (d *DummySink) Sink(message *protocol.Message) error {
	return nil
}

type SinkFactory struct {
}

func (d *SinkFactory) Create(config *protocol.SinkConfig) protocol.Sink {
	switch config.Type {
	case protocol.Kafka:
		return NewKafkaSink(config.Broker, config.Topic)
	case protocol.Console:
		return &ConsoleSink{}
	case protocol.Dummy:
		return &DummySink{}
	}
	return nil
}
