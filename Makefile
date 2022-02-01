GO_VERSION := 1.17.5

DOCKER_REPO ?= symbiosiscloud/csi-driver-symbiosis-block
PKG ?= github.com/symbiosis-cloud/csi-driver-symbiosis-block/cmd/symbiosis-block-csi
VERSION ?= 0.0.1

.PHONY: compile
compile:
	@echo "Building project"
	@docker run --rm -e GOOS=${OS} -e GOARCH=amd64 -v ${PWD}/:/app -w /app golang:${GO_VERSION}-alpine sh -c 'apk add git && go build -mod=vendor -o cmd/symbiosis-block-csi/${NAME} ${PKG}'

.PHONY: build
build:
	@echo "Building docker image"
	@docker build -t $(DOCKER_REPO):$(VERSION) --platform linux/amd64 cmd/symbiosis-block-csi -f cmd/symbiosis-block-csi/Dockerfile

.PHONY: push
push:
	@echo "Push docker image"
	@docker push $(DOCKER_REPO):$(VERSION)

.PHONY: all
all: compile build push
