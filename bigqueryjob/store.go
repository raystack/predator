package bigqueryjob

import (
	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
)

//Store to store information of bigquery job execution
type Store struct {
	db *gorm.DB
}

//NewStore to construct profile bq store
func NewStore(db *gorm.DB, tableName string) *Store {
	return &Store{db.Table(tableName)}
}

//Store to store map of profile Partition and BigQuery job Partition
func (p *Store) Store(bigqueryJob *protocol.BigqueryJob) error {
	handler := p.db.Create(bigqueryJob)
	if err := handler.Error; err != nil {
		return err
	}
	return nil
}
