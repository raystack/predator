package protocol

import (
	"errors"
	"strings"
)

//UploadFactory creator of UploadTask
type UploadFactory interface {
	Create(gitRepo *GitInfo) (Task, error)
}

//ErrUploadSpecValidation thrown when upload failed caused by invalid spec, contains list of invalid spec errors
type ErrUploadSpecValidation struct {
	Errors []error
}

func (s *ErrUploadSpecValidation) Error() string {
	var errorMessages []string

	for _, err := range s.Errors {
		errorMessages = append(errorMessages, err.Error())
	}

	return strings.Join(errorMessages, ",\n")
}

func IsUploadSpecValidationError(err error) bool {
	var e *ErrUploadSpecValidation
	if errors.As(err, &e) {
		return true
	}
	return false
}

//Task is an unit of an operation
type Task interface {
	Run() (interface{}, error)
}
