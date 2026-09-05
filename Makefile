.PHONY: build
build: tidy ## Build the CLI
	go build

IMAGE ?= ruslanrwx/kubernetes-event-exporter
VERSION ?= $(shell git describe --tags --always --dirty)

build-image: ## Build the Docker image
	docker build --platform linux/amd64 --build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

.PHONY: image-push
image-push: build-image ## Build, push, and print the digest to pin in deploy/02-deployment.yaml
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest
	@echo
	@echo "Pin this in deploy/02-deployment.yaml:"
	@docker inspect --format='  image: $(IMAGE):$(VERSION)@{{index (split (index .RepoDigests 0) "@") 1}}' $(IMAGE):$(VERSION)

.PHONY: fmt
fmt: ## Run go fmt against code
	gofmt -s -l -w .

.PHONY: vet
vet: ## Run go vet against code
	go vet ./...

tidy: ## Run go mod tidy
	go mod tidy

test: tidy ## Run tests
	go test -cover -mod=mod -v ./...

clean: ## Delete go.sum and clean mod cache
	go clean -modcache
	rm go.sum

.PHONY: help
help: ## Display this help.
	@cat $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } '
