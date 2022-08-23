package protocol

import (
	"errors"
	"time"

	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"

	"github.com/odpf/predator/protocol/job"
)

var (
	//ErrUniqueConstraintNotFound is an error
	ErrUniqueConstraintNotFound = errors.New("unique constraint not found")

	//ErrTableMetadataNotFound should be thrown when a table is not exist
	ErrTableMetadataNotFound = errors.New("table metadata not found")
)

//ConstraintStore interface of store that
type ConstraintStore interface {
	FetchConstraints(tableID string) ([]string, error)
}

//MetadataStore is store to get metadata information
type MetadataStore interface {
	//GetMetadata to fetch metadata as requirement for profiling
	GetMetadata(tableID string) (*meta.TableSpec, error)
	//GetUniqueConstraints to fetch unique constraints to calculate duplication metric
	GetUniqueConstraints(tableID string) ([]string, error)
}

//MetricSpecGenerator produce metric specification to be collected
type MetricSpecGenerator interface {
	Generate(tableSpec *meta.TableSpec, tolerances []*Tolerance) ([]*metric.Spec, error)
	GenerateMetricSpec(urn string) ([]*metric.Spec, error)
}

var (
	//ErrStatusNotFound is an error when getting status
	ErrStatusNotFound = errors.New("status not found")
)

//StatusStore to store status of profile and audit process
type StatusStore interface {
	Store(status *Status) error
	GetLatestStatusByIDandType(jobID string, jobType job.Type) (*Status, error)
	GetStatusLogByIDandType(jobID string, jobType job.Type) ([]*Status, error)
}

var (
	//ErrProfileNotFound is an error
	ErrProfileNotFound = errors.New("profile not found")
	//ErrProfileInvalid when a profile doesnt have any status in status Log
	//normally profile at least has one status in status Log
	ErrProfileInvalid = errors.New("profile invalid")
)

//ProfileStore to store profile
type ProfileStore interface {
	Create(profile *job.Profile) (*job.Profile, error)
	Update(profile *job.Profile) error
	Get(ID string) (*job.Profile, error)
}

//ProfileBQLogger to log profile id and bq job id mapping
type ProfileBQLogger interface {
	Log(entry Entry, bqJobID string) error
}

//StatusLogger to log status
type StatusLogger interface {
	Log(entry Entry, message string) error
}

var (
	//ErrAuditNotFound is an error
	ErrAuditNotFound = errors.New("audit not found")
)

//PartitionScanner to get affected partitions using last modified timestamp
type PartitionScanner interface {
	GetAffectedPartition(tableURN string, lastModifiedTimestamp time.Time) ([]string, error)
}

//ProfileConfig as an identifier to do profiling
type ProfileConfig struct {
	ProfileID   string
	TableSpec   *meta.TableSpec
	MetricSpecs []*metric.Spec
	Partition   string
}

//ToleranceSpecStateStore store of tolerance to be used for profile and audit
type ToleranceSpecStateStore interface {
	SaveTolerances(profileID string, tolerances []*Tolerance) error
	GetTolerancesByProfileID(profileID string) ([]*Tolerance, error)
}

var (
	//ErrAuditResultNotFound is an error
	ErrAuditResultNotFound = errors.New("audit result not found")
)

//MetricResultIdentifier to identify result beside ID
type MetricResultIdentifier struct {
	TableURN  string
	StartDate string
	EndDate   string
}

var (
	//ErrAuditIDInvalid thrown when access audit that is not belong to a profile
	ErrAuditIDInvalid = errors.New("error audit not belong to profile")
)

var (
	//ErrNoProfileMetricFound thrown when metric store do not find any profile metric record
	ErrNoProfileMetricFound = errors.New("no profile metric found")
)

//MetricQuery is field selector to query metrics
type MetricQuery struct {
	ProfileID   string
	Partition   string
	MetricTypes []metric.Type
	URN         string
}
