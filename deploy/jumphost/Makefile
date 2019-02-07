IMAGE_NAME ?= jumphost-openssh
IMAGE_TAG ?= latest
IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME_K8S)

build_jumphost_k8s:
	docker build -f docker/Dockerfile.k8s -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) docker/

gke_login:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	@if [ "$(CLUSTER_TYPE)" = "regional" ]; then\
		gcloud config set compute/region $(GKE_CLUSTER_REGION); \
	else \
		gcloud config set compute/zone $(GKE_COMPUTE_ZONE); \
	fi
	gcloud auth configure-docker

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v v2.11.0

gke_docker_jumphost_push: build_jumphost_k8s
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):latest
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):$(CIRCLE_SHA1)
	docker push $(IMAGE_K8S)

gke_k8s_deploy_jumphost: _helm
	@if [ "$(CLUSTER_TYPE)" = "regional" ]; then\
		gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) --region $(GKE_CLUSTER_REGION); \
	else \
		gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) --zone $(GKE_COMPUTE_ZONE); \
	fi
	helm --set "global.env=$(HELM_ENV)" --set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" upgrade --install jumphost helmpackage/ --wait --timeout 600
