module github.com/odpf/predator

go 1.14

replace github.com/googleapis/google-cloud-go-testing => github.com/vicknite/google-cloud-go-testing v0.0.0-20210426113657-6dade47e3126

require (
	cloud.google.com/go v0.81.0
	cloud.google.com/go/bigquery v1.17.0
	cloud.google.com/go/storage v1.10.0
	github.com/allegro/bigcache v1.2.1
	github.com/coocood/freecache v1.1.1
	github.com/eko/gocache v1.1.1
	github.com/golang-migrate/migrate/v4 v4.14.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/googleapis/google-cloud-go-testing v0.0.0-20200911160855-bcd43fbb19e8
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.4
	github.com/jinzhu/gorm v1.9.10
	github.com/joho/godotenv v1.3.0
	github.com/netdata/go-statsd v0.0.5
	github.com/segmentio/kafka-go v0.3.4
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/vmihailenco/msgpack/v5 v5.1.4
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/net v0.0.0-20210423184538-5f58ad60dda6 // indirect
	golang.org/x/oauth2 v0.0.0-20210413134643-5e61552d6c78
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.45.0
	google.golang.org/genproto v0.0.0-20210423144448-3a41ef94ed2b // indirect
	google.golang.org/protobuf v1.26.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/src-d/go-git.v4 v4.13.1
	gopkg.in/yaml.v2 v2.3.0
	gorm.io/datatypes v1.0.5
	gorm.io/driver/postgres v1.2.3 // indirect
	gorm.io/driver/sqlite v1.2.6 // indirect
	gorm.io/driver/sqlserver v1.2.1 // indirect
)
