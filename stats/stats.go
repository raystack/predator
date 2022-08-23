package stats

import (
	"fmt"
	"github.com/odpf/predator/protocol"
	"strings"
	"time"
)

type KV struct {
	K string
	V string
}

func Metric(name string, tags ...KV) string {
	var tagStr []string
	for _, tag := range tags {
		t := fmt.Sprintf("%s=%s", tag.K, tag.V)
		tagStr = append(tagStr, t)
	}

	names := append([]string{name}, tagStr...)
	return strings.Join(names, ",")
}

type Client interface {
	WithTags(tags ...KV) Client
	Increment(metric string)
	IncrementBy(metric string, count int64)
	Gauge(metric string, value float64)
	Histogram(metric string, value float64)
	DurationUntilNow(metric string, start time.Time)
	DurationOf(metric string, start, end time.Time)
	Close()
}

type ClientBuilder interface {
	WithEntity(entity *protocol.Entity) ClientBuilder
	WithURN(urn *protocol.Label) ClientBuilder
	WithEnvironment(environment string) ClientBuilder
	WithPodName(podName string) ClientBuilder
	WithDeployment(deployment string) ClientBuilder
	Build() (Client, error)
}
