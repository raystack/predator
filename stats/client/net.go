package client

import (
	"fmt"
	netstatsd "github.com/netdata/go-statsd"
	"github.com/odpf/predator/stats"
	"sort"
	"strings"
	"time"
)

type StatsdConfig struct {
	AppName string
	Host    string
	Port    int
}

//NetStatsd statsd client using netstatsd library
type NetStatsd struct {
	client *netstatsd.Client
	tags   []stats.KV
}

func NewNetStatsd(conf *StatsdConfig, defaultTags []stats.KV) (*NetStatsd, error) {
	address := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	writer, err := netstatsd.UDP(address)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s_", conf.AppName)
	client := netstatsd.NewClient(writer, prefix)
	client.FlushEvery(time.Duration(200) * time.Millisecond)
	return &NetStatsd{
		client: client,
		tags:   defaultTags,
	}, nil
}

func (d *NetStatsd) WithTags(tags ...stats.KV) stats.Client {
	return &NetStatsd{
		client: d.client,
		tags:   append(d.tags, tags...),
	}
}

func (d *NetStatsd) Increment(metric string) {
	m := addTags(metric, formatTags(d.tags))

	err := d.client.Increment(m)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) IncrementBy(metric string, count int64) {
	m := addTags(metric, formatTags(d.tags))
	err := d.countInt64(m, count)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) countInt64(metric string, count int64) error {
	return d.client.WriteMetric(metric, netstatsd.Int64(count), netstatsd.Count, 1)
}

func (d *NetStatsd) Gauge(metric string, value float64) {
	m := addTags(metric, formatTags(d.tags))

	err := d.client.GaugeFloat64(m, value)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) Histogram(metric string, value float64) {
	m := addTags(metric, formatTags(d.tags))

	err := d.histogramFloat64(m, value)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) histogramFloat64(metric string, value float64) error {
	return d.client.WriteMetric(metric, netstatsd.Float64(value), netstatsd.Histogram, 1)
}

func (d *NetStatsd) DurationUntilNow(metric string, start time.Time) {
	end := time.Now().In(time.UTC)
	duration := end.Sub(start)
	m := addTags(metric, formatTags(d.tags))

	err := d.client.Time(m, duration)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) DurationOf(metric string, start, end time.Time) {
	duration := end.Sub(start)
	m := addTags(metric, formatTags(d.tags))

	err := d.client.Time(m, duration)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *NetStatsd) Close() {
	err := d.client.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func formatTags(tags []stats.KV) []string {
	mergedTags := make(map[string]string)
	for _, tag := range tags {
		mergedTags[tag.K] = tag.V
	}
	var keys []string
	for key := range mergedTags {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	var tagsFormatted []string
	for _, key := range keys {
		value := mergedTags[key]
		tf := fmt.Sprintf("%s=%s", key, value)
		tagsFormatted = append(tagsFormatted, tf)
	}

	return tagsFormatted
}

func addTags(metric string, tags []string) string {
	return strings.Join(append([]string{metric}, tags...), ",")
}
