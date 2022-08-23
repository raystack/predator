package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/mock"
)

type mockAuditService struct {
	mock.Mock
}

//NewAuditService create mock AuditService
func NewAuditService() *mockAuditService {
	return &mockAuditService{}
}

func (m *mockAuditService) RunAudit(profileID string) (*protocol.AuditResult, error) {
	args := m.Called(profileID)
	return args.Get(0).(*protocol.AuditResult), args.Error(1)
}

type mockAuditor struct {
	mock.Mock
}

//NewAuditor to mock construct auditor
func NewAuditor() *mockAuditor {
	return &mockAuditor{}
}

func (a *mockAuditor) Audit(audit *job.Audit) ([]*protocol.AuditReport, error) {
	args := a.Called(audit)
	return args.Get(0).([]*protocol.AuditReport), args.Error(1)
}

type mockAuditStore struct {
	mock.Mock
}

func NewAuditStore() *mockAuditStore {
	return &mockAuditStore{}
}
func (m *mockAuditStore) CreateAudit(audit *job.Audit) (*job.Audit, error) {
	args := m.Called(&job.Audit{
		ID:        audit.ID,
		ProfileID: audit.ProfileID,
		Detail:    audit.Detail,
		State:     audit.State,
		URN:       audit.URN,
		Message:   audit.Message,
	})
	return args.Get(0).(*job.Audit), args.Error(1)
}

func (m *mockAuditStore) UpdateAudit(audit *job.Audit) error {
	args := m.Called(audit)
	return args.Error(0)
}

type mockResultStore struct {
	mock.Mock
}

//NewAuditResultStore to mock construct result store
func NewAuditResultStore() *mockResultStore {
	return &mockResultStore{}
}

//StoreResults to mock store results
func (r *mockResultStore) StoreResults(results []*protocol.AuditReport) error {
	args := r.Called(results)
	return args.Error(0)
}

type mockAuditSummaryFactory struct {
	mock.Mock
}

func (m *mockAuditSummaryFactory) Create(auditResults []*protocol.AuditReport, auditJob *job.Audit) (*protocol.AuditSummary, error) {
	args := m.Called(auditResults, auditJob)
	return args.Get(0).(*protocol.AuditSummary), args.Error(1)
}

func NewAuditSummaryFactory() *mockAuditSummaryFactory {
	return &mockAuditSummaryFactory{}
}
