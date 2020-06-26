IMAGE_NAME ?= platformapi
DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_TAG ?=$(shell echo "$(CIRCLE_TAG)" | awk -F/ '{print $$2}')
IMAGE_TAG ?= latest

IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
IMAGE_K8S_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)

SSH_IMAGE_NAME ?= ssh-auth
SSH_IMAGE_TAG ?= latest
SSH_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(SSH_IMAGE_NAME)
SSH_K8S_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(SSH_IMAGE_NAME)

INGRESS_FALLBACK_IMAGE_NAME ?= platformingressfallback
INGRESS_FALLBACK_IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(INGRESS_FALLBACK_IMAGE_NAME)
INGRESS_FALLBACK_IMAGE_K8S_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(INGRESS_FALLBACK_IMAGE_NAME)

ifdef CIRCLECI
    PIP_EXTRA_INDEX_URL ?= https://$(DEVPI_USER):$(DEVPI_PASS)@$(DEVPI_HOST)/$(DEVPI_USER)/$(DEVPI_INDEX)
else
    PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)
endif
export PIP_EXTRA_INDEX_URL

include k8s.mk

setup:
	pip install -U pip
	pip install --no-binary cryptography -r requirements/test.txt

lint:
	black --check platform_api tests setup.py
	flake8 platform_api tests setup.py
	mypy platform_api tests setup.py

format:
	isort -rc platform_api tests setup.py
	black platform_api tests setup.py

test_unit:
	pytest -vv --cov-config=setup.cfg --cov platform_api tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov-config=setup.cfg --cov platform_api tests/integration

test_e2e:
	pytest -vv tests/e2e

build_ssh_auth_k8s:
	docker build --build-arg PIP_EXTRA_INDEX_URL \
	    -f deploy/ssh_auth/docker/Dockerfile.ssh-auth.k8s -t $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) .

build_api_k8s:
	docker build --build-arg PIP_EXTRA_INDEX_URL \
	    -f Dockerfile.k8s -t $(IMAGE_NAME):$(IMAGE_TAG) .

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
	    $(IMAGE_K8S):latest

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

aws_login:
	pip install --upgrade awscli
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(AWS_CLUSTER_NAME)

docker_login:
	@docker login $(DOCKER_REPO) \
		--username=$(ARTIFACTORY_USERNAME) \
		--password=$(ARTIFACTORY_PASSWORD)

gke_docker_pull_test:
	docker pull $$(cat AUTH_SERVER_IMAGE_NAME)
	# use old platformconfig image that supports loading of config from storage
	docker pull $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/platformconfig:9d7cea532a7ab0e45871cb48cf355427a274dbd9

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v v2.11.0

gke_docker_push: build_api_k8s build_ssh_auth_k8s
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE_K8S):latest
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE_K8S):$(CIRCLE_SHA1)
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S):latest
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S):$(CIRCLE_SHA1)

	docker push $(IMAGE_K8S)
	docker push $(SSH_K8S)

	make -C platform_ingress_fallback IMAGE_NAME=$(INGRESS_FALLBACK_IMAGE_NAME) build

	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S):latest
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)
	docker push $(INGRESS_FALLBACK_IMAGE_K8S)

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

ecr_login:
	$$(aws ecr get-login --no-include-email --region $(AWS_REGION) )


aws_ssh_auth_docker_push: build_ssh_auth_k8s ecr_login
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S_AWS):latest
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S_AWS):$(CIRCLE_SHA1)
	docker push $(SSH_K8S_AWS):latest
	docker push $(SSH_K8S_AWS):$(CIRCLE_SHA1)

aws_docker_push: build_api_k8s ecr_login
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE_K8S_AWS):latest
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE_K8S_AWS):$(CIRCLE_SHA1)
	docker push $(IMAGE_K8S_AWS):latest
	docker push $(IMAGE_K8S_AWS):$(CIRCLE_SHA1)

	make -C platform_ingress_fallback IMAGE_NAME=$(INGRESS_FALLBACK_IMAGE_NAME) build

	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S_AWS):latest
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S_AWS):$(CIRCLE_SHA1)
	docker push $(INGRESS_FALLBACK_IMAGE_K8S_AWS):latest
	docker push $(INGRESS_FALLBACK_IMAGE_K8S_AWS):$(CIRCLE_SHA1)

gke_k8s_deploy: _helm
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	helm \
		--set "global.env=$(HELM_ENV)" \
		--set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" \
		--set "INGRESS_FALLBACK_IMAGE.$(HELM_ENV)=$(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)" \
		upgrade --install platformapi deploy/platformapi/ --wait --timeout 600

gke_k8s_deploy_ssh_auth: _helm
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	helm -f deploy/ssh_auth/values-$(HELM_ENV).yaml --set "IMAGE=$(SSH_K8S):$(CIRCLE_SHA1)" upgrade --install ssh-auth deploy/ssh_auth/ --wait --timeout 600

aws_k8s_deploy: _helm
	helm \
		--set "global.env=$(HELM_ENV)-aws" \
		--set "IMAGE.$(HELM_ENV)-aws=$(IMAGE_K8S_AWS):$(CIRCLE_SHA1)" \
		--set "INGRESS_FALLBACK_IMAGE.$(HELM_ENV)-aws=$(INGRESS_FALLBACK_IMAGE_K8S_AWS):$(CIRCLE_SHA1)" \
		upgrade --install platformapi deploy/platformapi/ --wait --timeout 600 --namespace platform

aws_k8s_deploy_ssh_auth: _helm
	helm -f deploy/ssh_auth/values-$(HELM_ENV)-aws.yaml --set "IMAGE=$(SSH_K8S_AWS):$(CIRCLE_SHA1)" upgrade --install ssh-auth deploy/ssh_auth/ --wait --timeout 600 --namespace platform


artifactory_docker_login:
	docker login $(ARTIFACTORY_DOCKER_REPO) --username=$(ARTIFACTORY_USERNAME) --password=$(ARTIFACTORY_PASSWORD)

artifactory_docker_pull_test: artifactory_docker_login
	docker pull $(shell cat SECRETS_SERVER_IMAGE_NAME)
	docker tag $(shell cat SECRETS_SERVER_IMAGE_NAME) platformsecrets:latest

artifactory_ssh_auth_docker_push: artifactory_docker_login build_ssh_auth_k8s
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(ARTIFACTORY_DOCKER_REPO)/$(SSH_IMAGE_NAME):$(ARTIFACTORY_TAG)
	docker push $(ARTIFACTORY_DOCKER_REPO)/$(SSH_IMAGE_NAME):$(ARTIFACTORY_TAG)

artifactory_ssh_auth_helm_push: _helm
	mkdir -p temp_deploy/$(SSH_IMAGE_NAME)
	cp -Rf deploy/ssh_auth/.  temp_deploy/$(SSH_IMAGE_NAME)
	cp temp_deploy/$(SSH_IMAGE_NAME)/values-template.yaml temp_deploy/$(SSH_IMAGE_NAME)/values.yaml
	sed -i "s/IMAGE_TAG/$(ARTIFACTORY_TAG)/g" temp_deploy/$(SSH_IMAGE_NAME)/values.yaml
	find temp_deploy/$(SSH_IMAGE_NAME) -type f -name 'values-*' -delete
	helm init --client-only
	helm package --app-version=$(ARTIFACTORY_TAG) --version=$(ARTIFACTORY_TAG) temp_deploy/$(SSH_IMAGE_NAME)/
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin
	helm push-artifactory $(SSH_IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz $(ARTIFACTORY_HELM_REPO) --username $(ARTIFACTORY_USERNAME) --password $(ARTIFACTORY_PASSWORD)

