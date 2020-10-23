IMAGE_NAME ?= platformapi
DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
IMAGE_TAG ?= $(GITHUB_SHA)
IMAGE_TAG ?= latest

CLOUD_IMAGE_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
CLOUD_IMAGE_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
CLOUD_IMAGE_azure ?= $(AZURE_DEV_ACR_NAME).azurecr.io/$(IMAGE_NAME)

CLOUD_IMAGE  = ${CLOUD_IMAGE_${CLOUD_PROVIDER}}

SSH_IMAGE_NAME ?= ssh-auth

INGRESS_FALLBACK_IMAGE_NAME ?= platformingressfallback
INGRESS_FALLBACK_CLOUD_IMAGE_gke ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(INGRESS_FALLBACK_IMAGE_NAME)
INGRESS_FALLBACK_CLOUD_IMAGE_aws ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(INGRESS_FALLBACK_IMAGE_NAME)
INGRESS_FALLBACK_CLOUD_IMAGE_azure ?= $(AZURE_DEV_ACR_NAME).azurecr.io/$(INGRESS_FALLBACK_IMAGE_NAME)

INGRESS_FALLBACK_CLOUD_IMAGE  = ${INGRESS_FALLBACK_CLOUD_IMAGE_${CLOUD_PROVIDER}}

PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMSECRETS_IMAGE = $(shell cat PLATFORMSECRETS_IMAGE)
PLATFORMDISKAPI_IMAGE = $(shell cat PLATFORMDISKAPI_IMAGE)

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	pip install -U pip
	pip install --no-binary cryptography -r requirements/test.txt

lint:
	isort --check-only --diff platform_api tests setup.py alembic
	black --check platform_api tests setup.py alembic
	flake8 platform_api tests setup.py alembic
	mypy platform_api tests setup.py alembic

format:
	isort platform_api tests setup.py alembic
	black platform_api tests setup.py alembic

test_unit:
	pytest -vv --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-integration.xml tests/integration

test_e2e:
	pytest -vv tests/e2e

docker_build_ssh_auth:
	docker build --build-arg PIP_EXTRA_INDEX_URL \
		-f deploy/ssh_auth/docker/Dockerfile.ssh-auth.k8s -t $(SSH_IMAGE_NAME):latest .

docker_build:
	docker build --build-arg PIP_EXTRA_INDEX_URL \
		-f Dockerfile.k8s -t $(IMAGE_NAME):latest .

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
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(AWS_CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_DEV_RG_NAME) --name $(CLUSTER_NAME)

docker_pull_test_images:
	docker pull $(PLATFORMAUTHAPI_IMAGE)
	docker pull $(PLATFORMCONFIG_IMAGE)
	docker pull $(PLATFORMSECRETS_IMAGE)
	docker pull $(PLATFORMDISKAPI_IMAGE)
	docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest
	docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest
	docker tag $(PLATFORMSECRETS_IMAGE) platformsecrets:latest
	docker tag $(PLATFORMDISKAPI_IMAGE) platformdiskapi:latest

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):latest
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):$(IMAGE_TAG)
	docker push $(CLOUD_IMAGE):latest
	docker push $(CLOUD_IMAGE):$(IMAGE_TAG)

	make -C platform_ingress_fallback IMAGE_NAME=$(INGRESS_FALLBACK_IMAGE_NAME) build

	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_CLOUD_IMAGE):latest
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_CLOUD_IMAGE):$(IMAGE_TAG)
	docker push $(INGRESS_FALLBACK_CLOUD_IMAGE):latest
	docker push $(INGRESS_FALLBACK_CLOUD_IMAGE):$(IMAGE_TAG)

helm_deploy:
	helm \
		-f deploy/platformapi/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml \
		--set "ENV=$(HELM_ENV)" \
		--set "IMAGE=$(CLOUD_IMAGE):$(IMAGE_TAG)" \
		upgrade --install platformapi deploy/platformapi/ --wait --timeout 600 --namespace platform

