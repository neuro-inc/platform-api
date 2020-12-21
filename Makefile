IMAGE_NAME ?= platformapi
DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
TAG ?= $(GITHUB_SHA)
TAG ?= latest

CLOUD_IMAGE_NAME_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
CLOUD_IMAGE_NAME_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
CLOUD_IMAGE_NAME_azure ?= $(AZURE_ACR_NAME).azurecr.io/$(IMAGE_NAME)

CLOUD_IMAGE_NAME        = $(CLOUD_IMAGE_NAME_$(CLOUD_PROVIDER))
ARTIFACTORY_IMAGE_NAME  = $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME)

INGRESS_FALLBACK_IMAGE_NAME ?= platformingressfallback
INGRESS_FALLBACK_CLOUD_IMAGE_NAME_gke ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(INGRESS_FALLBACK_IMAGE_NAME)
INGRESS_FALLBACK_CLOUD_IMAGE_NAME_aws ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(INGRESS_FALLBACK_IMAGE_NAME)
INGRESS_FALLBACK_CLOUD_IMAGE_NAME_azure ?= $(AZURE_ACR_NAME).azurecr.io/$(INGRESS_FALLBACK_IMAGE_NAME)

INGRESS_FALLBACK_CLOUD_IMAGE_NAME             = $(INGRESS_FALLBACK_CLOUD_IMAGE_NAME_$(CLOUD_PROVIDER))
ARTIFACTORY_INGRESS_FALLBACK_IMAGE_NAME = $(ARTIFACTORY_DOCKER_REPO)/$(INGRESS_FALLBACK_IMAGE_NAME)

PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMSECRETS_IMAGE = $(shell cat PLATFORMSECRETS_IMAGE)
PLATFORMDISKAPI_IMAGE = $(shell cat PLATFORMDISKAPI_IMAGE)

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	pip install -U pip
	pip install --no-binary cryptography -r requirements/test.txt
	pre-commit install

lint: format
	mypy platform_api tests setup.py alembic

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

test_e2e:
	pytest -vv tests/e2e

docker_build:
	python setup.py sdist
	docker build -f Dockerfile.k8s -t $(IMAGE_NAME):latest \
	--build-arg PIP_EXTRA_INDEX_URL \
	--build-arg DIST_FILENAME=`python setup.py --fullname`.tar.gz .
	make -C platform_ingress_fallback IMAGE_NAME=$(INGRESS_FALLBACK_IMAGE_NAME) build

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
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

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
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE_NAME):latest
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE_NAME):$(TAG)
	docker push $(CLOUD_IMAGE_NAME):latest
	docker push $(CLOUD_IMAGE_NAME):$(TAG)

	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_CLOUD_IMAGE_NAME):latest
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_CLOUD_IMAGE_NAME):$(TAG)
	docker push $(INGRESS_FALLBACK_CLOUD_IMAGE_NAME):latest
	docker push $(INGRESS_FALLBACK_CLOUD_IMAGE_NAME):$(TAG)

artifactory_docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(ARTIFACTORY_IMAGE_NAME):$(TAG)
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(ARTIFACTORY_INGRESS_FALLBACK_IMAGE_NAME):$(TAG)
	docker login $(ARTIFACTORY_DOCKER_REPO) \
		--username=$(ARTIFACTORY_USERNAME) \
		--password=$(ARTIFACTORY_PASSWORD)
	docker push $(ARTIFACTORY_IMAGE_NAME):$(TAG)
	docker push $(ARTIFACTORY_INGRESS_FALLBACK_IMAGE_NAME):$(TAG)

_helm_expand_vars:
	rm -rf temp_deploy/platformapi
	mkdir -p temp_deploy/platformapi
	cp -Rf deploy/platformapi/. temp_deploy/platformapi/
	find temp_deploy/platformapi -type f -name 'values*' -delete
	cp deploy/platformapi/values-template.yaml temp_deploy/platformapi/values.yaml
	sed -i.bak "s/\$$IMAGE/$(subst /,\/,$(ARTIFACTORY_IMAGE_NAME):$(TAG))/g" temp_deploy/platformapi/values.yaml
	sed -i.bak "s/\$$INGRESS_FALLBACK_IMAGE/$(subst /,\/,$(ARTIFACTORY_INGRESS_FALLBACK_IMAGE_NAME):$(TAG))/g" temp_deploy/platformapi/values.yaml
	find temp_deploy/platformapi -type f -name '*.bak' -delete

helm_deploy:
	helm \
		-f deploy/platformapi/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml \
		--set "ENV=$(HELM_ENV)" \
		--set "IMAGE=$(CLOUD_IMAGE_NAME):$(TAG)" \
		upgrade --install platformapi deploy/platformapi/ --wait --timeout 600 --namespace platform

artifactory_helm_push: _helm_expand_vars
	helm package --app-version=$(TAG) --version=$(TAG) temp_deploy/platformapi/
	helm push-artifactory $(IMAGE_NAME)-$(TAG).tgz $(ARTIFACTORY_HELM_REPO) \
		--username $(ARTIFACTORY_USERNAME) \
		--password $(ARTIFACTORY_PASSWORD)
