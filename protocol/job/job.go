package job

import (
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

//Strategy is profiling strategy
type Strategy struct {
	Type  StrategyType  `json:"type"`
	Value StrategyValue `json:"value"`
}

//Detail is configuration of profile
type Detail struct {
	URN               string   `json:"urn"`
	Strategy          Strategy `json:"strategy"`
	AffectedPartition []string `json:"partition"`
}

//UnmarshalJSON to deserialise Detail with multiple strategy implementation
func (d *Detail) UnmarshalJSON(b []byte) error {

	var obj struct {
		Strategy struct {
			Stype string `json:"type"`
		} `json:"strategy"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}

	switch obj.Strategy.Stype {
	case string(StrategyTypePartition):
		var ou struct {
			Urn      string `json:"urn"`
			Strategy struct {
				Stype  string   `json:"type"`
				Svalue []string `json:"value"`
			} `json:"strategy"`
			AffectedPartition []string `json:"partition"`
		}

		if err := json.Unmarshal(b, &ou); err != nil {
			return err
		}

		eee := Detail{
			URN: ou.Urn,
			Strategy: Strategy{
				Type:  StrategyTypePartition,
				Value: PartitionStrategy(ou.Strategy.Svalue),
			},
			AffectedPartition: ou.AffectedPartition,
		}
		*d = eee
	case string(StrategyTypeFullScan):
		var ou struct {
			Urn      string `json:"urn"`
			Strategy struct {
				Stype string `json:"type"`
			} `json:"strategy"`
			AffectedPartition []string `json:"partition"`
		}
		if err := json.Unmarshal(b, &ou); err != nil {
			return err
		}

		eee := Detail{
			URN: ou.Urn,
			Strategy: Strategy{
				Type: StrategyTypeFullScan,
			},
			AffectedPartition: ou.AffectedPartition,
		}
		*d = eee

	case string(StrategyTypeLastModified):
		var ou struct {
			Urn      string `json:"urn"`
			Strategy struct {
				Stype  string `json:"type"`
				Svalue string `json:"value"`
			} `json:"strategy"`
			AffectedPartition []string `json:"partition"`
		}
		if err := json.Unmarshal(b, &ou); err != nil {
			return err
		}

		eee := Detail{
			URN: ou.Urn,
			Strategy: Strategy{
				Type:  StrategyTypeLastModified,
				Value: LastModifiedStrategy(ou.Strategy.Svalue),
			},
			AffectedPartition: ou.AffectedPartition,
		}
		*d = eee
	default:
		return errors.New("unsupported StrategyValue")
	}

	return nil
}

type Mode string

func (m Mode) IsValid() error {
	switch m {
	case ModeIncremental:
		return nil
	case ModeComplete:
		return nil
	default:
		return fmt.Errorf("wrong Mode %s", string(m))
	}
}

func (m Mode) String() string {
	return string(m)
}

var (
	//ModeIncremental as incremental metrics mode
	ModeIncremental Mode = "incremental"
	//ModeComplete as complete metrics mode
	ModeComplete Mode = "complete"
)

//Profile is profile task
type Profile struct {
	ID string

	//Detail will be deprecated, do not use this

	Detail  *Detail
	Status  State
	Message string

	EventTimestamp time.Time
	//updatedTimestamp read only dont set this value
	UpdatedTimestamp time.Time

	GroupName string
	Filter    string
	Mode      Mode
	URN       string

	//TotalRecords is number of row profiled stat
	TotalRecords int64

	AuditTimestamp time.Time
}

//StrategyType is type of strategy
type StrategyType string

var (
	//StrategyTypePartition is Partition StrategyType
	StrategyTypePartition StrategyType = "partition"
	//StrategyTypeLastModified is Last Modified Timestamp StrategyType
	StrategyTypeLastModified StrategyType = "last_modified"
	//StrategyTypeFullScan is Full Scan StrategyType
	StrategyTypeFullScan StrategyType = "full_scan"
)

//StrategyValue is config for strategy
type StrategyValue interface {
	implementStrategyValue()
}

//PartitionStrategy is config for multiple partition strategy
type PartitionStrategy []string

func (p PartitionStrategy) implementStrategyValue() {
}

//LastModifiedStrategy is config for using last modified timestamp to calculate affected partition strategy
type LastModifiedStrategy string

func (l LastModifiedStrategy) implementStrategyValue() {
}

//Audit is an entity of one audit task
type Audit struct {
	ID             string
	ProfileID      string
	State          State
	Message        string
	Detail         *Detail
	URN            string
	TotalRecords   int64
	EventTimestamp time.Time
}

//State is state of a Job
type State string

func (s State) String() string {
	return string(s)
}

var (
	//StateCreated is
	StateCreated State = "created"
	//StateInProgress is
	StateInProgress State = "inprogress"
	//StateCompleted is
	StateCompleted State = "completed"
	//StateFailed is
	StateFailed State = "failed"
)

//Type is type of job
type Type string

var (
	//TypeProfile for profile
	TypeProfile Type = "profile"
	//TypeAudit for audit
	TypeAudit Type = "audit"
	//TypeUnknown when unable to get correct job type
	TypeUnknown Type = "unknown"
)

func (t Type) String() string {
	return string(t)
}

type Bigquery struct {
	Query *Query
	Job   *bigquery.Job
}

//Diff different of content between storage
type Diff struct {
	Add    []string
	Remove []string
	Update []string
}

func (d *Diff) AddedCount() int {
	return len(d.Add)
}

func (d *Diff) RemovedCount() int {
	return len(d.Remove)
}

func (d *Diff) UpdatedCount() int {
	return len(d.Update)
}

func DiffBetween(source []string, destination []string) *Diff {
	add := sliceSubtract(source, destination)
	remove := sliceSubtract(destination, source)
	update := sliceIntersect(source, destination)

	return &Diff{
		Add:    add,
		Remove: remove,
		Update: update,
	}
}

//sliceSubtract subtract return entries that exist on both x and y
func sliceIntersect(x []string, y []string) []string {
	var target = make(map[string]struct{}, len(y))

	for _, d := range y {
		target[d] = struct{}{}
	}

	var result []string
	for _, d := range x {
		if _, ok := target[d]; ok {
			result = append(result, d)
		}
	}
	return result
}

//sliceSubtract subtract x using y
//return entries that exist on x but not exist no y
func sliceSubtract(x []string, y []string) []string {
	var target = make(map[string]struct{}, len(y))

	for _, d := range y {
		target[d] = struct{}{}
	}

	var result []string
	for _, d := range x {
		if _, ok := target[d]; !ok {
			result = append(result, d)
		}
	}
	return result
}

type Query struct {
	URN     string
	Content string
	Type    QueryType
}

//QueryType is type of query
type QueryType string

func (t QueryType) String() string {
	return string(t)
}

const (
	//StatisticalQuery is query to fetch statistical metrics
	StatisticalQuery QueryType = "statistical"
	//FieldLevelQuery is query to fetch field level metrics
	FieldLevelQuery QueryType = "field_level"
	//TableLevelQuery is query to fetch table level metrics
	TableLevelQuery QueryType = "table_level"
)
