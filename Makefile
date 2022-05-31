VERSION ?= latest
IMAGE_TAG_BASE ?= dokercagri/operator-agent
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)

docker-build:
	docker build -t ${IMG} .

docker-push:
	docker push ${IMG}

