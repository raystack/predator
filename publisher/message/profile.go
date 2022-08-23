package message

import (
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/publisher/proto/odpf/predator/v1beta1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sort"
)

type ProfileKeyProtoBuilder struct {
	Metrics []*metric.Metric
	Profile *job.Profile
}

func (p *ProfileKeyProtoBuilder) Build() (proto.Message, error) {
	key, err := profileKeyProto(p.Profile, p.Metrics)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func profileKeyProto(profileJob *job.Profile, metrics []*metric.Metric) (proto.Message, error) {
	eventTimestampProto := timestamppb.New(profileJob.EventTimestamp)
	group := &predator.Group{
		Column: profileJob.GroupName,
	}
	protoKey := &predator.MetricsLogKey{
		Id:             profileJob.ID,
		EventTimestamp: eventTimestampProto,
		Group:          group,
	}
	if len(metrics) > 0 {
		sample := metrics[0]
		protoKey.Group.Value = sample.GroupValue
	}
	return protoKey, nil
}

type ProfileValueProtoBuilder struct {
	Metrics       []*metric.Metric
	Profile       *job.Profile
	MetadataStore protocol.MetadataStore
}

func (p *ProfileValueProtoBuilder) Build() (proto.Message, error) {
	tableSpec, err := p.MetadataStore.GetMetadata(p.Profile.URN)
	if err != nil {
		return nil, err
	}

	return profileValueProto(p.Profile, p.Metrics, tableSpec)
}

func profileValueProto(profileJob *job.Profile, metrics []*metric.Metric, spec *meta.TableSpec) (proto.Message, error) {
	eventTimestampProto, err := ptypes.TimestampProto(profileJob.EventTimestamp)
	if err != nil {
		return nil, err
	}
	group := &predator.Group{
		Column: profileJob.GroupName,
	}
	protoMessage := &predator.MetricsLogMessage{
		Id:             profileJob.ID,
		Urn:            profileJob.URN,
		EventTimestamp: eventTimestampProto,
		Group:          group,
		Filter:         profileJob.Filter,
		Mode:           profileJob.Mode.String(),
	}
	if len(metrics) > 0 {
		sample := metrics[0]
		protoMessage.Group.Value = sample.GroupValue
	} else {
		return protoMessage, nil
	}
	protoMessage.TableMetrics = generateProtoTableMetrics(metrics)
	protoColumnMetrics, err := generateProtoColumnMetrics(metrics, spec)
	if err != nil {
		return nil, err
	}
	protoMessage.ColumnMetrics = protoColumnMetrics
	return protoMessage, nil
}

func generateProtoTableMetrics(metrics []*metric.Metric) []*predator.Metric {
	tableMetrics := metric.NewFinder(metrics).WithOwner(metric.Table).Find()
	var protoTableMetrics []*predator.Metric
	for _, tableMetric := range tableMetrics {
		protoTableMetric := &predator.Metric{
			Name:      tableMetric.Type.String(),
			Value:     tableMetric.Value,
			Condition: tableMetric.Condition,
		}
		protoTableMetrics = append(protoTableMetrics, protoTableMetric)
	}
	return protoTableMetrics
}

func generateProtoColumnMetric(fieldID string, tableSpec *meta.TableSpec) (*predator.ColumnMetric, error) {
	fieldSpec, err := tableSpec.GetFieldSpecByID(fieldID)
	if err != nil {
		if err == meta.ErrFieldSpecNotFound {
			err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", fieldSpec.ID(), tableSpec.TableID(), err)
		}
		return nil, err
	}
	currentColumn := &predator.ColumnMetric{
		Id:      fieldID,
		Type:    fieldSpec.FieldType.String(),
		Metrics: []*predator.Metric{},
	}
	return currentColumn, nil
}

func generateProtoColumnMetricsMap(columnMetrics []*metric.Metric, tableSpec *meta.TableSpec) (map[string]*predator.ColumnMetric, error) {
	protoColumnMetricsMap := make(map[string]*predator.ColumnMetric)
	for _, fm := range columnMetrics {
		_, ok := protoColumnMetricsMap[fm.FieldID]
		if !ok {
			columnMetric, err := generateProtoColumnMetric(fm.FieldID, tableSpec)
			if err != nil {
				return nil, err
			}
			protoColumnMetricsMap[fm.FieldID] = columnMetric
		}
	}
	for _, fm := range columnMetrics {
		metricItem := &predator.Metric{
			Name:      fm.Type.String(),
			Value:     fm.Value,
			Condition: fm.Condition,
		}
		protoColumnMetricsMap[fm.FieldID].Metrics = append(protoColumnMetricsMap[fm.FieldID].Metrics, metricItem)
	}

	return protoColumnMetricsMap, nil
}

func getSortedProtoColumnMetrics(protoColumnMetricsMap map[string]*predator.ColumnMetric) []*predator.ColumnMetric {
	var columns []string
	for columnID := range protoColumnMetricsMap {
		columns = append(columns, columnID)
	}
	sort.Strings(columns)

	var protoColumnMetrics []*predator.ColumnMetric
	for _, c := range columns {
		column := protoColumnMetricsMap[c]
		protoColumnMetrics = append(protoColumnMetrics, column)
	}
	return protoColumnMetrics
}

func generateProtoColumnMetrics(metrics []*metric.Metric, tableSpec *meta.TableSpec) ([]*predator.ColumnMetric, error) {
	columnMetrics := metric.NewFinder(metrics).WithOwner(metric.Field).Find()
	protoColumnMetricsMap, err := generateProtoColumnMetricsMap(columnMetrics, tableSpec)
	if err != nil {
		return nil, err
	}
	protoColumnMetrics := getSortedProtoColumnMetrics(protoColumnMetricsMap)
	return protoColumnMetrics, nil
}
