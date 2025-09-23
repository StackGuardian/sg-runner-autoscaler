# Makefile for building and pushing Docker images for Lambda

# Default version - can be overridden via command line: make dash VERSION=1.2.3
VERSION ?= $(shell git describe --tags --always --dirty)

DASH_ACCOUNT_ID = 790543352839
PROD_ACCOUNT_ID =

# Registry and image configuration
REGISTRY = $(ACCOUNT_ID).dkr.ecr.eu-central-1.amazonaws.com
IMAGE_NAME = autoscaler/runner
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME)

# Docker build configuration
PLATFORM = linux/arm64
DOCKERFILE = Dockerfile.pyinstaller
DOCKER_BUILD_ARGS = --push --pull --platform $(PLATFORM) --provenance=false
SG_RUNNER_TYPE = shared-external

# AWS profiles
DASH_PROFILE = default
PROD_PROFILE = sg-prod

# Default values for dynamic build
PROFILE ?=
ACCOUNT_ID ?=

.PHONY: help version login build dash prod

help: ## Show this help message
	@echo 'Usage: make <target>'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

version: ## Show current version
	@echo $(VERSION)

login: ## Login to ECR using PROFILE variable
	@echo "Logging into ECR using profile: $(PROFILE)..."
	@aws --profile $(PROFILE) ecr get-login-password --region eu-central-1 \
		| docker login --username AWS --password-stdin $(REGISTRY)

build: login ## Build and push Docker image using VERSION variable
	@echo "Building and pushing image: $(FULL_IMAGE):$(VERSION)"
	@docker buildx build \
		-f $(DOCKERFILE) $(DOCKER_BUILD_ARGS) \
		--build-arg type="$(SG_RUNNER_TYPE)" \
		-t $(FULL_IMAGE):latest \
		-t $(FULL_IMAGE):$(VERSION) .
	@echo "Build completed: $(FULL_IMAGE):$(VERSION)"

dash: PROFILE=$(DASH_PROFILE)
dash: ACCOUNT_ID=$(DASH_ACCOUNT_ID)
dash: IMAGE_NAME="autoscaler/external-runner"
dash: SG_RUNNER_TYPE="external"
dash: build ## Build and push Docker image for Dash environment with compiled and tagged code

dash-shared: PROFILE=$(DASH_PROFILE)
dash-shared: ACCOUNT_ID=$(DASH_ACCOUNT_ID)
dash-shared: IMAGE_NAME="autoscaler/shared-runner"
dash-shared: SG_RUNNER_TYPE="shared-external"
dash-shared: build ## Build and push Docker image for Dash environment with shared runner

# dash-raw: PROFILE=$(DASH_PROFILE)
# dash-raw: ACCOUNT_ID=$(DASH_ACCOUNT_ID)
# dash-raw: DOCKERFILE=Dockerfile
# dash-raw: build ## Build and push Docker image for Dash environment with source Python code

# dash-lts: PROFILE=$(DASH_PROFILE)
# dash-lts: ACCOUNT_ID=$(DASH_ACCOUNT_ID)
# dash-lts: VERSION="latest"
# dash-lts: build ## Build and push Docker image for Dash environment with compiled and latest tag

prod: PROFILE=$(PROD_PROFILE)
prod: ACCOUNT_ID=$(PROD_ACCOUNT_ID)
prod: build ## Build and push Docker image for Prod environment

## TODO(adis.halilovic@stackguardian.io): Clean built images
# clean: ## Clean up local Docker images
# 	@echo "Cleaning up local images..."
# 	@docker image prune -f
# 	@docker builder prune -f
