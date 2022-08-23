package protocol

import (
	"context"

	"github.com/odpf/predator/protocol/job"
)

type entryKey string

const (
	jobIDKey     entryKey = "job_id"
	partitionKey entryKey = "partition"
	tableURNKey  entryKey = "table_urn"
	jobTypeKey   entryKey = "job_type"
	statusKey    entryKey = "status"
	groupKey     entryKey = "group"
)

//Entry as an entry struct for logging
type Entry struct {
	ctx context.Context
}

//NewEntry to construct Entry
func NewEntry() Entry {
	return Entry{
		ctx: context.Background(),
	}
}

//WithJobID to set Profile or AuditReport Partition
func (e Entry) WithJobID(jobID string) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, jobIDKey, jobID),
	}
}

//WithPartition to set partition
func (e Entry) WithPartition(partition string) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, partitionKey, partition),
	}
}

//WithTableURN to set table URN
func (e Entry) WithTableURN(tableURN string) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, tableURNKey, tableURN),
	}
}

//WithJobType to set job type
func (e Entry) WithJobType(jobType job.Type) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, jobTypeKey, jobType),
	}
}

//WithStatus to set status
func (e Entry) WithStatus(status string) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, statusKey, status),
	}
}

//WithGroup to set group
func (e Entry) WithGroup(group string) Entry {
	return Entry{
		ctx: context.WithValue(e.ctx, groupKey, group),
	}
}

//Status to get status
func (e Entry) Status() string {
	v := e.ctx.Value(statusKey)
	if v == nil {
		return ""
	}

	out, ok := v.(string)
	if !ok {
		return ""
	}

	return out
}

//Partition to get partition
func (e Entry) Partition() string {
	v := e.ctx.Value(partitionKey)
	if v == nil {
		return ""
	}

	out, ok := v.(string)
	if !ok {
		return ""
	}

	return out
}

//TableURN to get tableURN
func (e Entry) TableURN() string {
	v := e.ctx.Value(tableURNKey)
	if v == nil {
		return ""
	}

	out, ok := v.(string)
	if !ok {
		return ""
	}

	return out
}

//JobID to get jobID
func (e Entry) JobID() string {
	v := e.ctx.Value(jobIDKey)
	if v == nil {
		return ""
	}

	out, ok := v.(string)
	if !ok {
		return ""
	}

	return out
}

//JobType to get jobType
func (e Entry) JobType() job.Type {
	v := e.ctx.Value(jobTypeKey)
	if v == nil {
		return ""
	}

	out, ok := v.(job.Type)
	if !ok {
		return job.TypeUnknown
	}

	return out
}

//Group to get group
func (e Entry) Group() string {
	v := e.ctx.Value(groupKey)
	if v == nil {
		return ""
	}

	out, ok := v.(string)
	if !ok {
		return ""
	}

	return out
}
