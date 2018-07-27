IMAGE_NAME ?= platformapi
IMAGE_TAG ?= latest
IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME_K8S)

include k8s.mk

build_api_k8s:
	docker build -f Dockerfile.k8s -t $(IMAGE_K8S):latest -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) .

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
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	sudo /opt/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	sudo /opt/google-cloud-sdk/bin/gcloud config set project $(GKE_PROJECT_ID)
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	sudo /opt/google-cloud-sdk/bin/gcloud config set compute/zone $(GKE_COMPUTE_ZONE)
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash
	sudo /opt/google-cloud-sdk/bin/gcloud docker --authorize-only

gke_docker_push: build_api_k8s
	docker tag $(IMAGE_K8S):latest $(IMAGE_K8S):$(CIRCLE_SHA1)
	sudo /opt/google-cloud-sdk/bin/gcloud docker -- push $(IMAGE_K8S)

gke_k8s_deploy_dev:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME)
	sudo chown -R circleci: $(HOME)/.kube
	helm --set "global.env=dev" --set "IMAGE.dev=$(IMAGE_K8S):$(CIRCLE_SHA1)" --wait --timeout 600 upgrade platformapi deploy/platformapi/
	
gke_k8s_deploy_staging:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_STAGE_CLUSTER_NAME)
	sudo chown -R circleci: $(HOME)/.kube
	helm --set "global.env=staging" --set "IMAGE.staging=$(IMAGE_K8S):$(CIRCLE_SHA1)" --wait --timeout 600 upgrade platformapi deploy/platformapi/
