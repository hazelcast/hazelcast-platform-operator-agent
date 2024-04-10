VERSION ?= 0.1.26
IMAGE_TAG_BASE ?= hazelcast/platform-operator-agent
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)

docker-build:
	docker build -t ${IMG} .

docker-push:
	docker push ${IMG}

.PHONY: test
test:
	go test -v ./...

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

## LINTER
lint: lint-go

LINTER_SETUP_DIR=$(shell pwd)/lintbin
LINTER_PATH="${LINTER_SETUP_DIR}/bin:${PATH}"
lint-go: setup-linters
	PATH=${LINTER_PATH} golangci-lint run

setup-linters:
	source hack/setup-linters.sh; get_linters ${LINTER_SETUP_DIR}
