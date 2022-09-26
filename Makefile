NAME = "github.com/odpf/predator"
PROTON_COMMIT := "5c0b3bb5df406f2d6ea0f20e2dc41bb89c5cfbe5"
LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_TAG := "$(shell git rev-list --tags --max-count=1)"
PREDATOR_VERSION := "$(shell git describe --tags ${LAST_TAG})-next"

.PHONY: build test migrate rollback run cover

all: build

build:
	@echo " > notice: skipped proto generation, use 'generate-proto' make command"
	@echo " > building predator version ${PREDATOR_VERSION}"
	@go build -ldflags "-X ${NAME}/conf.BuildVersion=${PREDATOR_VERSION} -X ${NAME}/conf.BuildCommit=${LAST_COMMIT}" -o predator ./cmd
	@echo " - build complete"

run:
	./predator

coverage:
	go test `go list ./... | grep -v /cmd | grep -v mock` -count 1 -cover -parallel 100 -coverprofile coverage.txt > /dev/null
	go tool cover -func coverage.txt

test:
	go test `go list ./... | grep -v /cmd | grep -v mock` -count 1 -cover -parallel 100

unit-test-ci:
	go test -count 5 -race -coverprofile coverage.txt -covermode=atomic -timeout 3m -tags=unit_test ./...

migrate:
	go run ./cmd/migrator/migration.go up

rollback:
	go run ./cmd/migrator/migration.go down

generate-db-resource:
	@echo " > generating resources"
	@go generate ./db/migrations

generate-proto:
	@echo " > generating protobuf from odpf/proton"
	@buf generate https://github.com/odpf/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --template buf.gen.yaml --path odpf/predator
	@echo " > protobuf compilation finished"

lint:
	golangci-lint run --fix
