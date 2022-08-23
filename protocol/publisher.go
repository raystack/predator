package protocol

import (
	"context"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"google.golang.org/protobuf/proto"
)

//PublisherType type of supported publisher
type PublisherType string

const (
	//Kafka for publish to apache kafka
	Kafka PublisherType = "kafka"
	//Console for publish to terminal console or log (for testing purpose)
	Console PublisherType = "console"
	//Dummy if publish to none
	Dummy PublisherType = "none"
)

//ProfilePublisher for profiler
type ProfilePublisher interface {
	Publish(profileJob *job.Profile, metrics []*metric.Metric) error
	Close(context.Context) error
}

type MessageProviderFactory interface {
	CreateProfileMessage(profile *job.Profile, metrics []*metric.Metric) []MessageProvider
	CreateAuditMessage(audit *job.Audit, auditResult []*AuditReport) []MessageProvider
}

type ProtoBuilder interface {
	Build() (proto.Message, error)
}

type Message struct {
	Key   proto.Message
	Value proto.Message
}

type MessageProvider interface {
	Get() (*Message, error)
}

type Publisher interface {
	Publish(provider MessageProvider) error
	Close(ctx context.Context) error
}

type Sink interface {
	Sink(message *Message) error
	Close(ctx context.Context) error
}

type SinkConfig struct {
	Type   PublisherType
	Broker []string
	Topic  string
}

type SinkFactory interface {
	Create(config *SinkConfig) Sink
}
