AWS_ACCOUNT_ID ?= 771188043543
AWS_REGION ?= us-east-1

AZURE_RG_NAME ?= dev
AZURE_ACR_NAME ?= crc570d91c95c6aac0ea80afb1019a0c6f

GITHUB_OWNER ?= neuro-inc

IMAGE_TAG ?= latest

IMAGE_REPO_gke    = $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
IMAGE_REPO_aws    = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO_azure  = $(AZURE_ACR_NAME).azurecr.io
IMAGE_REPO_github = ghcr.io/$(GITHUB_OWNER)

IMAGE_REGISTRY ?= aws

IMAGE_NAME      = platformapi
IMAGE_REPO_BASE = $(IMAGE_REPO_$(IMAGE_REGISTRY))
IMAGE_REPO      = $(IMAGE_REPO_BASE)/$(IMAGE_NAME)

HELM_ENV           ?= dev
HELM_CHART         ?= platform-api
HELM_RELEASE       ?= platform-api
HELM_CHART_VERSION ?= 1.0.0
HELM_APP_VERSION   ?= 1.0.0

PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMSECRETS_IMAGE = $(shell cat PLATFORMSECRETS_IMAGE)
PLATFORMDISKAPI_IMAGE = $(shell cat PLATFORMDISKAPI_IMAGE)
PLATFORMADMIN_IMAGE = $(shell cat PLATFORMADMIN_IMAGE)

include k8s.mk

setup:
	pip install -U pip
	pip install -e .[dev]
	pre-commit install

lint: format
	mypy --show-error-codes platform_api tests alembic

format:
ifdef CI_LINT_RUN
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-integration.xml tests/integration

docker_build:
	rm -rf build dist
	pip install -U build
	python -m build
	docker build \
		--build-arg PYTHON_BASE=slim-buster \
		-t $(IMAGE_NAME):latest .

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):$(IMAGE_TAG)
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):latest
	docker push $(IMAGE_REPO):latest

run_api_k8s:
	NP_STORAGE_HOST_MOUNT_PATH=/tmp \
	NP_K8S_API_URL=https://$$(minikube ip):8443 \
	NP_K8S_CA_PATH=$$HOME/.minikube/ca.crt \
	NP_K8S_AUTH_CERT_PATH=$$HOME/.minikube/client.crt \
	NP_K8S_AUTH_CERT_KEY_PATH=$$HOME/.minikube/client.key \
	platform-api

run_api_k8s_container:
	docker run --rm -it --name platformapi \
	    -p 8080:8080 \
	    -v $$HOME/.minikube:$$HOME/.minikube \
	    -e NP_STORAGE_HOST_MOUNT_PATH=/tmp \
	    -e NP_K8S_API_URL=https://$$(minikube ip):8443 \
	    -e NP_K8S_CA_PATH=$$HOME/.minikube/ca.crt \
	    -e NP_K8S_AUTH_CERT_PATH=$$HOME/.minikube/client.crt \
	    -e NP_K8S_AUTH_CERT_KEY_PATH=$$HOME/.minikube/client.key \
	    $(IMAGE_NAME):latest

gke_login:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud auth configure-docker

aws_k8s_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

docker_pull_test_images:
	docker pull $(PLATFORMAUTHAPI_IMAGE)
	docker pull $(PLATFORMCONFIG_IMAGE)
	docker pull $(PLATFORMSECRETS_IMAGE)
	docker pull $(PLATFORMDISKAPI_IMAGE)
	docker pull $(PLATFORMADMIN_IMAGE)
	docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest
	docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest
	docker tag $(PLATFORMSECRETS_IMAGE) platformsecrets:latest
	docker tag $(PLATFORMDISKAPI_IMAGE) platformdiskapi:latest
	docker tag $(PLATFORMADMIN_IMAGE) platformadmin:latest

helm_create_chart:
	export IMAGE_REPO=$(IMAGE_REPO); \
	export IMAGE_TAG=$(IMAGE_TAG); \
	export CHART_VERSION=$(HELM_CHART_VERSION); \
	export APP_VERSION=$(HELM_APP_VERSION); \
	VALUES=$$(cat charts/$(HELM_CHART)/values.yaml | envsubst); \
	echo "$$VALUES" > charts/$(HELM_CHART)/values.yaml; \
	CHART=$$(cat charts/$(HELM_CHART)/Chart.yaml | envsubst); \
	echo "$$CHART" > charts/$(HELM_CHART)/Chart.yaml

helm_deploy: helm_create_chart
	helm dependency update charts/$(HELM_CHART)
	helm upgrade $(HELM_RELEASE) charts/$(HELM_CHART) \
		-f charts/$(HELM_CHART)/values-$(HELM_ENV).yaml \
		--set "platform.clusterName=$(CLUSTER_NAME)" \
		--namespace platform --install --wait --timeout 600s
