package uniqueconstraint

import (
	"github.com/odpf/predator/protocol"
)

//DictionaryStore is store that contains of unique constraint dictionary
type DictionaryStore interface {
	Get() (map[string][]string, error)
}

//Store is store to get list of unique constraint column
type Store struct {
	dictionaryStore DictionaryStore
}

//NewStore is constructor
func NewStore(source DictionaryStore) *Store {
	return &Store{
		dictionaryStore: source,
	}
}

//FetchConstraints to get unique constraint
//will throw protocol.ErrUniqueConstraintNotFound when unique constraint not found
func (s *Store) FetchConstraints(tableID string) ([]string, error) {
	uniqueConstraintDictionary, err := s.dictionaryStore.Get()
	if err != nil {
		return nil, err
	}

	uniqueConstraints, ok := uniqueConstraintDictionary[tableID]
	if !ok {
		return nil, protocol.ErrUniqueConstraintNotFound
	}
	return uniqueConstraints, nil
}
