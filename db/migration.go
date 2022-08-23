package db

import (
	"fmt"
	"log"
	"net/http"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/odpf/predator/conf"
	dbv1beta1 "github.com/odpf/predator/db/migrations/v1beta1"
)

// NewHTTPFSMigrator reads the migrations from httpfs and returns the migrate.Migrate
func NewHTTPFSMigrator(database *conf.Database, fileSystem http.FileSystem) (*migrate.Migrate, error) {
	src, err := httpfs.New(fileSystem, "/")
	if err != nil {
		log.Fatal(err)
	}

	dbHost := database.Host
	dbPort := database.Port
	dbName := database.Name
	dbUser := database.User
	dbPass := database.Pass

	dBConnURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)

	return migrate.NewWithSourceInstance("httpfs", src, dBConnURL)
}

func startMigrate(database *conf.Database, fileSystem http.FileSystem) {
	migrator, err := NewHTTPFSMigrator(database, fileSystem)
	if err != nil {
		log.Fatal(err)
	}
	if err := migrator.Up(); err != nil {
		if err == migrate.ErrNoChange {
			log.Println(err)
			return
		}
		log.Fatal(err)
	}
}

//Migrate to migrate database
func Migrate(confFile *conf.ConfigFile) {
	config, err := conf.LoadConfig(confFile)

	if err != nil {
		log.Fatal(err)
	}
	startMigrate(config.Database, dbv1beta1.DBMigrationFileSystem)
}

//Rollback migration
func Rollback(confFile *conf.ConfigFile) {
	config, err := conf.LoadConfig(confFile)
	if err != nil {
		log.Fatal(err)
	}
	startRollback(config.Database, dbv1beta1.DBMigrationFileSystem)
}

func startRollback(database *conf.Database, fileSystem http.FileSystem) {
	migrator, err := NewHTTPFSMigrator(database, fileSystem)
	if err != nil {
		log.Fatal(err)
	}
	if err := migrator.Steps(-1); err != nil {
		if err == migrate.ErrNoChange {
			log.Println(err)
			return
		}
		log.Fatal(err)
		return
	}
}
