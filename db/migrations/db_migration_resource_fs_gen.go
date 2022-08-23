//go:build ignore
// +build ignore

package main

import (
	"log"
	"net/http"

	"github.com/shurcooL/vfsgen"
)

var migrationsResourceFSV1Beta1 = http.Dir("./v1beta1/resources")
var migrationsFileNameV1Beta1 = "./v1beta1/db_migration_resource_fs.go"

func generateVFS(migrationsResourceFS http.FileSystem, migrationsFileName string) {
	err := vfsgen.Generate(migrationsResourceFS, vfsgen.Options{
		PackageName:  "db",
		VariableName: "DBMigrationFileSystem",
		Filename:     migrationsFileName,
	})
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	generateVFS(migrationsResourceFSV1Beta1, migrationsFileNameV1Beta1)
}
