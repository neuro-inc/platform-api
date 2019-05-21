IMAGE_NAME ?= platformapi
IMAGE_TAG ?= latest
IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME_K8S)
SSH_IMAGE_NAME ?= ssh-auth
SSH_IMAGE_TAG ?= latest
SSH_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(SSH_IMAGE_NAME)
INGRESS_FALLBACK_IMAGE_NAME ?= platformingressfallback
INGRESS_FALLBACK_IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(INGRESS_FALLBACK_IMAGE_NAME)


ifdef CIRCLECI
    PIP_INDEX_URL ?= https://$(DEVPI_USER):$(DEVPI_PASS)@$(DEVPI_HOST)/$(DEVPI_USER)/$(DEVPI_INDEX)
else
    PIP_INDEX_URL ?= $(shell python pip_extra_index_url.py)
endif
export PIP_INDEX_URL

include k8s.mk

setup:
	pip install --no-use-pep517 --no-binary cryptography -r requirements/test.txt

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
	@docker build --build-arg PIP_INDEX_URL="$(PIP_INDEX_URL)" \
	    -f deploy/ssh_auth/docker/Dockerfile.ssh-auth.k8s -t $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) .

build_api_k8s:
	@docker build --build-arg PIP_INDEX_URL="$(PIP_INDEX_URL)" \
	    -f Dockerfile.k8s -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) .

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

gke_docker_pull_test:
	docker pull $$(cat AUTH_SERVER_IMAGE_NAME)
	docker pull $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/platformconfig:latest
	docker pull $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/platformnotificationsapi:latest

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v v2.11.0

gke_docker_push: build_api_k8s build_ssh_auth_k8s
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):latest
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):$(CIRCLE_SHA1)
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S):latest
	docker tag $(SSH_IMAGE_NAME):$(SSH_IMAGE_TAG) $(SSH_K8S):$(CIRCLE_SHA1)

	docker push $(IMAGE_K8S)
	docker push $(SSH_K8S)

	make -C platform_ingress_fallback IMAGE_NAME=$(INGRESS_FALLBACK_IMAGE_NAME) build

	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S):latest
	docker tag $(INGRESS_FALLBACK_IMAGE_NAME):latest $(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)
	docker push $(INGRESS_FALLBACK_IMAGE_K8S)

gke_k8s_deploy: _helm
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	helm \
		--set "global.env=$(HELM_ENV)" \
		--set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" \
		--set "INGRESS_FALLBACK_IMAGE.$(HELM_ENV)=$(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)" \
		upgrade --install platformapi deploy/platformapi/ --wait --timeout 600

gke_k8s_deploy_ssh_auth: _helm
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	helm --set "global.env=$(HELM_ENV)" --set "IMAGE.$(HELM_ENV)=$(SSH_K8S):$(CIRCLE_SHA1)" upgrade --install ssh-auth deploy/ssh_auth/ --wait --timeout 600
