# Makefile for building and pushing Docker images
# StackGuardian: sg-runner-autoscaler

VERSION ?= $(shell git describe --always --dirty)

BUILD_REGION ?= eu-central-1
IMAGE_NAME = runner/autoscaler
REGISTRY = $(ACCOUNT_ID).dkr.ecr.$(BUILD_REGION).amazonaws.com
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME)

PLATFORM = linux/amd64
DOCKERFILE = Dockerfile
DOCKER_BUILD_ARGS = --push --pull --platform $(PLATFORM) --provenance=false
SG_RUNNER_TYPE = shared-external

DASH_ACCOUNT_ID = 790543352839
PROD_ACCOUNT_ID = 476299211833

DASH_PROFILE = default
PROD_PROFILE = sg-prod

PROFILE ?=
ACCOUNT_ID ?=

.PHONY: help version login build dash prod

##@ General
help: ## Show this help message
	@echo 'Usage: make <target>'
	@echo ''
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)}' $(MAKEFILE_LIST)

version: ## Show current version
	@echo $(VERSION)

login:
	@echo "Logging into ECR using profile: $(PROFILE)..."
	@aws --profile $(PROFILE) ecr-public get-login-password --region us-east-1 \
		| docker login --username AWS --password-stdin public.ecr.aws
	@aws --profile $(PROFILE) ecr get-login-password --region $(BUILD_REGION) \
		| docker login --username AWS --password-stdin $(REGISTRY)

build: login ## Build and push Docker image
	@echo "Building and pushing image: $(FULL_IMAGE):$(VERSION)"
	@docker buildx build \
		-f $(DOCKERFILE) $(DOCKER_BUILD_ARGS) \
		--build-arg type="$(SG_RUNNER_TYPE)" \
		-t $(FULL_IMAGE):$(VERSION) .
	@echo "Build completed: $(FULL_IMAGE):$(VERSION)"

##@ DASH
dash: PROFILE=$(DASH_PROFILE)
dash: ACCOUNT_ID=$(DASH_ACCOUNT_ID)
dash: build ## Build and push to DASH

##@ PROD
prod: PROFILE=$(PROD_PROFILE)
prod: ACCOUNT_ID=$(PROD_ACCOUNT_ID)
prod: build ## Build and push to PROD
