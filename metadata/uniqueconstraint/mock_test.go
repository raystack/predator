package uniqueconstraint_test

import (
	"github.com/stretchr/testify/mock"
)

type mockUniqueConstraintDictionaryStore struct {
	mock.Mock
}

func (m *mockUniqueConstraintDictionaryStore) Get() (map[string][]string, error) {
	args := m.Called()
	return args.Get(0).(map[string][]string), args.Error(1)
}

type mockFileReader struct {
	mock.Mock
}

func (m *mockFileReader) ReadFile(filePath string) ([]byte, error) {
	args := m.Called(filePath)
	return args.Get(0).([]byte), args.Error(1)
}
