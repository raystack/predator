package message

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/publisher/proto/odpf/predator/v1beta1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuditKeyProtoBuilder struct {
	AuditResult  []*protocol.AuditReport
	Audit        *job.Audit
	ProfileStore protocol.ProfileStore
}

func (a *AuditKeyProtoBuilder) Build() (proto.Message, error) {
	profileJob, err := a.ProfileStore.Get(a.Audit.ProfileID)
	if err != nil {
		return nil, err
	}

	group := &predator.Group{
		Column: profileJob.GroupName,
	}

	if len(a.AuditResult) > 0 {
		group.Value = a.AuditResult[0].GroupValue
	}

	return auditkeyProto(a.Audit, group)
}

func auditkeyProto(auditJob *job.Audit, group *predator.Group) (proto.Message, error) {
	eventTimestampProto := timestamppb.New(auditJob.EventTimestamp)
	return &predator.ResultLogKey{
		Id:             auditJob.ID,
		Group:          group,
		EventTimestamp: eventTimestampProto,
	}, nil
}

type AuditValueProtoBuilder struct {
	AuditResult  []*protocol.AuditReport
	Audit        *job.Audit
	ProfileStore protocol.ProfileStore
}

func (a *AuditValueProtoBuilder) Build() (proto.Message, error) {
	profileJob, err := a.ProfileStore.Get(a.Audit.ProfileID)
	if err != nil {
		return nil, err
	}

	group := &predator.Group{
		Column: profileJob.GroupName,
	}

	if len(a.AuditResult) > 0 {
		group.Value = a.AuditResult[0].GroupValue
	}

	return auditValueProto(a.Audit, group, a.AuditResult)
}

func auditValueProto(auditJob *job.Audit, group *predator.Group, reports []*protocol.AuditReport) (proto.Message, error) {
	eventTimestampProto, err := ptypes.TimestampProto(auditJob.EventTimestamp)
	if err != nil {
		return nil, err
	}
	resultLogMessage := &predator.ResultLogMessage{
		Id:             auditJob.ID,
		ProfileId:      auditJob.ProfileID,
		Urn:            auditJob.URN,
		Group:          group,
		EventTimestamp: eventTimestampProto,
	}
	if len(reports) > 0 {
		resultLogMessage.Results = generateAuditResultsProto(reports)
	}
	return resultLogMessage, nil
}

func generateAuditResultsProto(reports []*protocol.AuditReport) []*predator.Result {
	var results []*predator.Result
	for _, a := range reports {
		var rules []*predator.ToleranceRule
		for _, rule := range a.ToleranceRules {
			rules = append(rules, &predator.ToleranceRule{
				Name:  string(rule.Comparator),
				Value: rule.Value,
			})
		}
		results = append(results, &predator.Result{
			Name:      a.MetricName.String(),
			FieldId:   a.FieldID,
			Value:     a.MetricValue,
			Rules:     rules,
			PassFlag:  a.PassFlag,
			Condition: a.Condition,
		})
	}
	return results
}
