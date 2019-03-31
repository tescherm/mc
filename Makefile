PROG := mc

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT := $(patsubst %/,%,$(dir $(MKFILE_PATH)))

CONTAINER_ROOT := /go/src/github.com/tescherm/mc

DOCKER_GO := docker run --rm -t --net host \
                -v "$(ROOT)":"$(CONTAINER_ROOT)" \
                -w "$(CONTAINER_ROOT)" \
                -e CGO_ENABLED=0 \
                golang:1.12-alpine3.9

PREFIX ?= $(ROOT)/build

BINDIR := $(PREFIX)/bin
BINARY := $(BINDIR)/$(PROG)

# Help
.PHONY: default
default:
	@echo "Please specify a build target. The choices are:"
	@echo "    bench:            Run benchmarks"
	@echo "    binary:           Create Go binary ($(BINARY))"
	@echo "    check:            Run lint checks"
	@echo "    clean:            Clean build directory"
	@echo "    compose-up:       Start the service locally using docker-compose"
	@echo "    image:            Create Docker image"
	@echo "    protoc:           Compile protobuf schema"
	@echo "    test:             Run unit tests"
	@echo "    test-integration: Run integration tests"
	@false

.PHONY: FORCE
FORCE:

$(BINARY): FORCE
	@echo "============= Building $@ ==============="
	@mkdir -p $(BINDIR)
	cd $(ROOT) && CGO_ENABLED=0 go build -o $@ .

.PHONY: binary
binary: $(BINARY)

.PHONY: protoc
protoc:
	protoc -I pb/ pb/memcached.proto --go_out=plugins=grpc:pb

.PHONY: image
image:
	@echo "============= Creating image $(BUILD_IMAGE) ==============="
	docker build -t mc:latest -f $(ROOT)/Dockerfile $(ROOT)

.PHONY: check
check:
	$(DOCKER_GO) go vet -v ./...

.PHONY: clean
clean:
	@echo "============= Cleaning ==============="
	rm -rf "$(BINARY)" "$(ROOT)/build"

.PHONY: compose-up
compose-up:
	docker-compose down >/dev/null 2>&1
	docker-compose -f docker-compose.yml up --build

.PHONY: test
test:
	$(DOCKER_GO) go test --cover -v -short ./...

.PHONY: test-integration
test-integration:
	docker-compose -f docker-compose.yml up --build -d
	$(DOCKER_GO) go test -v -tags=integration ./integration-test

.PHONY: bench
bench:
	$(DOCKER_GO) go test -bench=. ./...
