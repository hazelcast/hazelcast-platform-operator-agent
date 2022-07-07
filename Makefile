VERSION ?= 0.1.4
IMAGE_TAG_BASE ?= hazelcast/platform-operator-agent
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)

docker-build:
	docker build -t ${IMG} .

docker-push:
	docker push ${IMG}

