package mock

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

func NewDatabase(model interface{}) (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	clearDB := func() {
		db.Close()
		db.DropTableIfExists(model)
	}
	if exists := db.HasTable(model); !exists {
		db.CreateTable(model)
	}

	return db, clearDB
}

func NewEmptyDatabase() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	clearDB := func() {
		db.Close()
	}

	return db, clearDB
}
