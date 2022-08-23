// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: odpf/predator/v1beta1/metrics_log.proto

package predator

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type MetricsLogKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Group          *Group                 `protobuf:"bytes,2,opt,name=group,proto3" json:"group,omitempty"`
	EventTimestamp *timestamppb.Timestamp `protobuf:"bytes,99,opt,name=event_timestamp,json=eventTimestamp,proto3" json:"event_timestamp,omitempty"`
}

func (x *MetricsLogKey) Reset() {
	*x = MetricsLogKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MetricsLogKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricsLogKey) ProtoMessage() {}

func (x *MetricsLogKey) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricsLogKey.ProtoReflect.Descriptor instead.
func (*MetricsLogKey) Descriptor() ([]byte, []int) {
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP(), []int{0}
}

func (x *MetricsLogKey) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *MetricsLogKey) GetGroup() *Group {
	if x != nil {
		return x.Group
	}
	return nil
}

func (x *MetricsLogKey) GetEventTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.EventTimestamp
	}
	return nil
}

type MetricsLogMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Urn            string                 `protobuf:"bytes,2,opt,name=urn,proto3" json:"urn,omitempty"`
	Filter         string                 `protobuf:"bytes,3,opt,name=filter,proto3" json:"filter,omitempty"`
	Group          *Group                 `protobuf:"bytes,4,opt,name=group,proto3" json:"group,omitempty"`
	Mode           string                 `protobuf:"bytes,5,opt,name=mode,proto3" json:"mode,omitempty"`
	TableMetrics   []*Metric              `protobuf:"bytes,6,rep,name=table_metrics,json=tableMetrics,proto3" json:"table_metrics,omitempty"`
	ColumnMetrics  []*ColumnMetric        `protobuf:"bytes,7,rep,name=column_metrics,json=columnMetrics,proto3" json:"column_metrics,omitempty"`
	EventTimestamp *timestamppb.Timestamp `protobuf:"bytes,99,opt,name=event_timestamp,json=eventTimestamp,proto3" json:"event_timestamp,omitempty"`
}

func (x *MetricsLogMessage) Reset() {
	*x = MetricsLogMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MetricsLogMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricsLogMessage) ProtoMessage() {}

func (x *MetricsLogMessage) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricsLogMessage.ProtoReflect.Descriptor instead.
func (*MetricsLogMessage) Descriptor() ([]byte, []int) {
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP(), []int{1}
}

func (x *MetricsLogMessage) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *MetricsLogMessage) GetUrn() string {
	if x != nil {
		return x.Urn
	}
	return ""
}

func (x *MetricsLogMessage) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

func (x *MetricsLogMessage) GetGroup() *Group {
	if x != nil {
		return x.Group
	}
	return nil
}

func (x *MetricsLogMessage) GetMode() string {
	if x != nil {
		return x.Mode
	}
	return ""
}

func (x *MetricsLogMessage) GetTableMetrics() []*Metric {
	if x != nil {
		return x.TableMetrics
	}
	return nil
}

func (x *MetricsLogMessage) GetColumnMetrics() []*ColumnMetric {
	if x != nil {
		return x.ColumnMetrics
	}
	return nil
}

func (x *MetricsLogMessage) GetEventTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.EventTimestamp
	}
	return nil
}

type Metric struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name      string  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value     float64 `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	Condition string  `protobuf:"bytes,3,opt,name=condition,proto3" json:"condition,omitempty"`
}

func (x *Metric) Reset() {
	*x = Metric{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Metric) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Metric) ProtoMessage() {}

func (x *Metric) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Metric.ProtoReflect.Descriptor instead.
func (*Metric) Descriptor() ([]byte, []int) {
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP(), []int{2}
}

func (x *Metric) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Metric) GetValue() float64 {
	if x != nil {
		return x.Value
	}
	return 0
}

func (x *Metric) GetCondition() string {
	if x != nil {
		return x.Condition
	}
	return ""
}

type Group struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Column string `protobuf:"bytes,1,opt,name=column,proto3" json:"column,omitempty"`
	Value  string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Group) Reset() {
	*x = Group{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Group) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Group) ProtoMessage() {}

func (x *Group) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Group.ProtoReflect.Descriptor instead.
func (*Group) Descriptor() ([]byte, []int) {
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP(), []int{3}
}

func (x *Group) GetColumn() string {
	if x != nil {
		return x.Column
	}
	return ""
}

func (x *Group) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type ColumnMetric struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id      string    `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Type    string    `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
	Metrics []*Metric `protobuf:"bytes,3,rep,name=metrics,proto3" json:"metrics,omitempty"`
}

func (x *ColumnMetric) Reset() {
	*x = ColumnMetric{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ColumnMetric) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ColumnMetric) ProtoMessage() {}

func (x *ColumnMetric) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ColumnMetric.ProtoReflect.Descriptor instead.
func (*ColumnMetric) Descriptor() ([]byte, []int) {
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP(), []int{4}
}

func (x *ColumnMetric) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ColumnMetric) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *ColumnMetric) GetMetrics() []*Metric {
	if x != nil {
		return x.Metrics
	}
	return nil
}

var File_odpf_predator_v1beta1_metrics_log_proto protoreflect.FileDescriptor

var file_odpf_predator_v1beta1_metrics_log_proto_rawDesc = []byte{
	0x0a, 0x27, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x2f,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x5f,
	0x6c, 0x6f, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x15, 0x6f, 0x64, 0x70, 0x66, 0x2e,
	0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x98, 0x01, 0x0a, 0x0d, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x4c, 0x6f, 0x67,
	0x4b, 0x65, 0x79, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x02, 0x69, 0x64, 0x12, 0x32, 0x0a, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74,
	0x6f, 0x72, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x47, 0x72, 0x6f, 0x75, 0x70,
	0x52, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x43, 0x0a, 0x0f, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x63, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0e, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0xea, 0x02, 0x0a,
	0x11, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x4c, 0x6f, 0x67, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x75, 0x72, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x12, 0x32, 0x0a, 0x05,
	0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x6f, 0x64,
	0x70, 0x66, 0x2e, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x2e, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x52, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70,
	0x12, 0x12, 0x0a, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6d, 0x6f, 0x64, 0x65, 0x12, 0x42, 0x0a, 0x0d, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6d, 0x65,
	0x74, 0x72, 0x69, 0x63, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x6f, 0x64,
	0x70, 0x66, 0x2e, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x2e, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x52, 0x0c, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x12, 0x4a, 0x0a, 0x0e, 0x63, 0x6f, 0x6c, 0x75,
	0x6d, 0x6e, 0x5f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x23, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x4d,
	0x65, 0x74, 0x72, 0x69, 0x63, 0x52, 0x0d, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x4d, 0x65, 0x74,
	0x72, 0x69, 0x63, 0x73, 0x12, 0x43, 0x0a, 0x0f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x63, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0e, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x50, 0x0a, 0x06, 0x4d, 0x65, 0x74,
	0x72, 0x69, 0x63, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1c, 0x0a,
	0x09, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x35, 0x0a, 0x05, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x12, 0x16, 0x0a, 0x06, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x12, 0x14, 0x0a, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x22, 0x6b, 0x0a, 0x0c, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x4d, 0x65, 0x74, 0x72,
	0x69, 0x63, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x37, 0x0a, 0x07, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
	0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70,
	0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x52, 0x07, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x42,
	0x4d, 0x0a, 0x17, 0x69, 0x6f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x6e, 0x2e, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x42, 0x0f, 0x4d, 0x65, 0x74, 0x72,
	0x69, 0x63, 0x73, 0x4c, 0x6f, 0x67, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x1f, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x70, 0x72, 0x65, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_predator_v1beta1_metrics_log_proto_rawDescOnce sync.Once
	file_odpf_predator_v1beta1_metrics_log_proto_rawDescData = file_odpf_predator_v1beta1_metrics_log_proto_rawDesc
)

func file_odpf_predator_v1beta1_metrics_log_proto_rawDescGZIP() []byte {
	file_odpf_predator_v1beta1_metrics_log_proto_rawDescOnce.Do(func() {
		file_odpf_predator_v1beta1_metrics_log_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_predator_v1beta1_metrics_log_proto_rawDescData)
	})
	return file_odpf_predator_v1beta1_metrics_log_proto_rawDescData
}

var file_odpf_predator_v1beta1_metrics_log_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_odpf_predator_v1beta1_metrics_log_proto_goTypes = []interface{}{
	(*MetricsLogKey)(nil),         // 0: odpf.predator.v1beta1.MetricsLogKey
	(*MetricsLogMessage)(nil),     // 1: odpf.predator.v1beta1.MetricsLogMessage
	(*Metric)(nil),                // 2: odpf.predator.v1beta1.Metric
	(*Group)(nil),                 // 3: odpf.predator.v1beta1.Group
	(*ColumnMetric)(nil),          // 4: odpf.predator.v1beta1.ColumnMetric
	(*timestamppb.Timestamp)(nil), // 5: google.protobuf.Timestamp
}
var file_odpf_predator_v1beta1_metrics_log_proto_depIdxs = []int32{
	3, // 0: odpf.predator.v1beta1.MetricsLogKey.group:type_name -> odpf.predator.v1beta1.Group
	5, // 1: odpf.predator.v1beta1.MetricsLogKey.event_timestamp:type_name -> google.protobuf.Timestamp
	3, // 2: odpf.predator.v1beta1.MetricsLogMessage.group:type_name -> odpf.predator.v1beta1.Group
	2, // 3: odpf.predator.v1beta1.MetricsLogMessage.table_metrics:type_name -> odpf.predator.v1beta1.Metric
	4, // 4: odpf.predator.v1beta1.MetricsLogMessage.column_metrics:type_name -> odpf.predator.v1beta1.ColumnMetric
	5, // 5: odpf.predator.v1beta1.MetricsLogMessage.event_timestamp:type_name -> google.protobuf.Timestamp
	2, // 6: odpf.predator.v1beta1.ColumnMetric.metrics:type_name -> odpf.predator.v1beta1.Metric
	7, // [7:7] is the sub-list for method output_type
	7, // [7:7] is the sub-list for method input_type
	7, // [7:7] is the sub-list for extension type_name
	7, // [7:7] is the sub-list for extension extendee
	0, // [0:7] is the sub-list for field type_name
}

func init() { file_odpf_predator_v1beta1_metrics_log_proto_init() }
func file_odpf_predator_v1beta1_metrics_log_proto_init() {
	if File_odpf_predator_v1beta1_metrics_log_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MetricsLogKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MetricsLogMessage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Metric); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Group); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_predator_v1beta1_metrics_log_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ColumnMetric); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_odpf_predator_v1beta1_metrics_log_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_odpf_predator_v1beta1_metrics_log_proto_goTypes,
		DependencyIndexes: file_odpf_predator_v1beta1_metrics_log_proto_depIdxs,
		MessageInfos:      file_odpf_predator_v1beta1_metrics_log_proto_msgTypes,
	}.Build()
	File_odpf_predator_v1beta1_metrics_log_proto = out.File
	file_odpf_predator_v1beta1_metrics_log_proto_rawDesc = nil
	file_odpf_predator_v1beta1_metrics_log_proto_goTypes = nil
	file_odpf_predator_v1beta1_metrics_log_proto_depIdxs = nil
}
