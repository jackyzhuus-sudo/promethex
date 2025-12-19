GOHOSTOS:=$(shell go env GOHOSTOS)
GOPATH:=$(shell go env GOPATH)
VERSION=$(shell git describe --tags --always)


# 测试相关变量
TEST_PATH ?= ./internal/data/base
TEST_NAME ?= TestGetZsetFloat64

ifeq ($(GOHOSTOS), windows)
	#the `find.exe` is different from `find` in bash/shell.
	#to see https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/find.
	#changed to use git-bash.exe to run find cli or other cli friendly, caused of every developer has a Git.
	#Git_Bash= $(subst cmd\,bin\bash.exe,$(dir $(shell where git)))
	Git_Bash=$(subst \,/,$(subst cmd\,bin\bash.exe,$(dir $(shell where git))))
	INTERNAL_PROTO_FILES=$(shell $(Git_Bash) -c "find internal -name *.proto")
	API_PROTO_FILES=$(shell $(Git_Bash) -c "find api -name *.proto")
else
	INTERNAL_PROTO_FILES=$(shell find internal -name *.proto)
	API_PROTO_FILES=$(shell find api -name *.proto)
endif

.PHONY: init
# init env
init:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/go-kratos/kratos/cmd/kratos/v2@latest
	go install github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v2@latest
	go install github.com/google/gnostic/cmd/protoc-gen-openapi@latest
	go install github.com/google/wire/cmd/wire@latest

.PHONY: config
# generate internal proto
config:
	protoc --proto_path=./internal \
	       --proto_path=../market-proto/third_party \
 	       --go_out=paths=source_relative:./internal \
	       $(INTERNAL_PROTO_FILES)

.PHONY: api
# generate api proto
api:
	protoc --proto_path=./api \
	       --proto_path=../market-proto/third_party \
 	       --go_out=paths=source_relative:./api \
 	       --go-http_out=paths=source_relative:./api \
 	       --go-grpc_out=paths=source_relative:./api \
	       --openapi_out=fq_schema_naming=true,default_response=false:. \
	       $(API_PROTO_FILES)

.PHONY: grpc-api
# generate grpc api proto only (no http)
grpc-api:
	protoc --proto_path=./api \
	       --proto_path=../market-proto/third_party \
 	       --go_out=paths=source_relative:./api \
 	       --go-grpc_out=paths=source_relative:./api \
	       $(API_PROTO_FILES)

.PHONY: build
# build
build:
	mkdir -p bin/ && go build -ldflags "-X main.Version=$(VERSION)" -o ./bin/ ./...

.PHONY: build-linux
# build for linux
build-linux:
	mkdir -p bin/ && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.Version=$(VERSION)" -o ./bin/ ./...

# run
run: build
	./bin/market-service -conf ./configs

.PHONY: stop
# stop the running service
stop:
	@echo "Stopping running service..."
	@-pkill -f "./bin/market-service" || echo "No running service found"

.PHONY: backrun
# stop running service, build, clear logs and run in background
backrun: stop build
	mkdir -p logs
	rm -f logs/app.log
	nohup ./bin/market-service -conf ./configs > logs/app.log 2>&1 &
	@echo "Service started in background. Log file: logs/app.log"
	@echo "Use 'tail -f logs/app.log' to follow logs"
	@echo "Process ID: $$(pgrep -f './bin/market-service')"

.PHONY: generate
# generate
generate:
	go generate ./...
	go mod tidy

.PHONY: all
# generate all
all:
	make api;
	make config;
	make generate;

.PHONY: docker-build
# build docker image
docker-build:
	docker build -t market-service:latest .

.PHONY: docker-run
# run docker container
docker-run:
	docker run -d --name market-service -p 9000:9000 -v $(shell pwd)/configs:/app/configs market-service:latest

.PHONY: docker-stop
# stop and remove docker container
docker-stop:
	-docker stop market-service
	-docker rm market-service

.PHONY: docker-restart
# restart docker container
docker-restart: docker-stop docker-run

.PHONY: docker-local-dev
# build image and run container for local development
docker-local-dev: docker-stop
	docker build -t market-service:latest .
	docker run -d --name market-service -p 9000:9000 -v $(shell pwd)/configs:/app/configs market-service:latest
	@echo "Docker container 'market-service' is running. GRPC port 9000 is mapped to host."
	@echo "Use 'docker logs -f market-service' to follow logs"

# show help
help:
	@echo ''
	@echo 'Usage:'
	@echo ' make [target]'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
	helpMessage = match(lastLine, /^# (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")); \
			helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
			printf "\033[36m%-22s\033[0m %s\n", helpCommand,helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help


test:
	go test -v $(TEST_PATH) -run $(TEST_NAME)


build2:
	CGO_ENABLED=0 mkdir -p bin/ && \
	go build -a \
	-tags netgo,osusergo \
	-ldflags "-X main.Version=$(VERSION) -w -s -extldflags '-static'" \
	-o ./bin/ ./...
