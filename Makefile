# XMover Makefile - Minimal Setup

# Configuration
REGISTRY = cloud.registry.cr8.net
IMAGE_NAME = xmover
VERSION = v0.0.1
IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
LATEST = $(REGISTRY)/$(IMAGE_NAME):latest

.PHONY: help build push clean test

## Show available commands
help:
	@echo "XMover - Available Commands:"
	@echo "  build    Build Docker image"
	@echo "  push     Push to registry"
	@echo "  test     Test container"
	@echo "  clean    Remove local images"

## Build Docker image
build:
	@echo "Building $(IMAGE)..."
	docker build -t $(IMAGE) -t $(LATEST) .

## Push to registry
push: build
	@echo "Pushing to registry..."
	docker push $(IMAGE)
	docker push $(LATEST)

## Test container
test: build
	docker run --rm $(IMAGE) --version

## Clean up local images
clean:
	docker rmi $(IMAGE) $(LATEST) 2>/dev/null || true
