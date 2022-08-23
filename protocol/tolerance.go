package protocol

import (
	"errors"
	"strings"
	"time"

	"github.com/odpf/predator/protocol/metric"
)

//Comparator comparator of tolerance rule
type Comparator string

func (c Comparator) String() string {
	return string(c)
}

const (
	//ComparatorLessThan metric < value
	ComparatorLessThan Comparator = "less_than"
	//ComparatorLessThanEq metric <= value
	ComparatorLessThanEq Comparator = "less_than_eq"
	//ComparatorMoreThan metric > value
	ComparatorMoreThan Comparator = "more_than"
	//ComparatorMoreThanEq metric >= value
	ComparatorMoreThanEq Comparator = "more_than_eq"
)

//ToleranceRule represents tolerance comparator and its value
type ToleranceRule struct {
	Comparator Comparator `json:"comparator"`
	Value      float64    `json:"value"`
}

type ToleranceSpec struct {
	URN        string
	Tolerances []*Tolerance
}

//Tolerance is tolerance of quality metrics
type Tolerance struct {
	ID             string
	TableURN       string
	FieldID        string
	MetricName     metric.Type
	Condition      string //condition for invalid_pct metric
	Metadata       map[string]interface{}
	ToleranceRules []ToleranceRule
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

var (
	//ErrToleranceNotFound thrown when tolerance for a tableID not found
	ErrToleranceNotFound = errors.New("tolerance for tableID not found")
)

//ToleranceStore to fetch the quality tolerances
type ToleranceStore interface {
	Create(spec *ToleranceSpec) error
	GetByTableID(tableID string) (*ToleranceSpec, error)
	Delete(tableID string) error
	GetAll() ([]*ToleranceSpec, error)
	GetByProjectID(projectID string) ([]*ToleranceSpec, error)

	//GetResourceNames provide information all of tableID in the stored specs
	GetResourceNames() ([]string, error)
}

//ToleranceStoreFactory creator of ToleranceStore
type ToleranceStoreFactory interface {
	Create(URL string, multiTenancyEnabled bool) (ToleranceStore, error)
	CreateWithOptions(store FileStore, pathType PathType) (ToleranceStore, error)
}

//ErrSpecInvalid error thrown when a spec content is invalid, contains list of errors
type ErrSpecInvalid struct {
	URN    string
	Errors []error
}

func (e *ErrSpecInvalid) Error() string {
	var errorMessages []string

	for _, err := range e.Errors {
		errorMessages = append(errorMessages, err.Error())
	}

	fieldError := strings.Join(errorMessages, ",\n")
	return e.URN + " spec is invalid, reason: " + fieldError
}

func IsSpecInvalidError(err error) bool {
	var e *ErrSpecInvalid
	if errors.As(err, &e) {
		return true
	}
	return false
}

type SpecValidator interface {
	//Validate content of data quality spec should return error ErrSpecInvalid when field or table not found
	Validate(spec *ToleranceSpec) error
}
