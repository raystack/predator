package model

import (
	"github.com/odpf/predator/protocol/metric"
)

//Metric information about statistical measurement of a table resource
type Metric struct {
	FieldID  string          `json:"field_id"`
	Name     metric.Type     `json:"name"`
	Category metric.Category `json:"category"`
	Owner    metric.Owner    `json:"owner"`

	Value     float64                `json:"value"`
	Condition string                 `json:"condition"`
	Metadata  map[string]interface{} `json:"metadata"`
}

type MetricGroup struct {
	Group   string    `json:"group"`
	Metrics []*Metric `json:"metrics"`
}
