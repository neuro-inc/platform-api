pkgs = $(shell go list ./...)

DOCKER_SECRET ?= $(shell bash -c 'echo -n "$(DOCKER_USER):$(DOCKER_PASS)" | base64')
DOCKER_REGISTRY ?= registry.neuromation.io
export DOCKER_REGISTRY
DOCKER_REPO ?= $(DOCKER_REGISTRY)/neuromationorg
IMAGE_NAME ?= platformapi
IMAGE_TAG ?= latest
IMAGE ?= $(DOCKER_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)
<<<<<<< HEAD
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)-k8s
=======

IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(DOCKER_REPO)/$(IMAGE_NAME_K8S):$(IMAGE_TAG)

>>>>>>> d03f4a34108eca753f18dd25b5540d0f1fc46b5f
format:
	go fmt $(pkgs)
	gofmt -w -s .

build:
	go build

test: build
	go test -v -race -cover $(pkgs)

go_integration_test: build
	echo > coverage.txt
	for d in $(pkgs); do \
		go test -v -race -coverprofile=profile.out -covermode=atomic $$d -tags=integration;\
		if [ -f profile.out ]; then \
			cat profile.out >> coverage.txt; \
			rm profile.out; \
		fi; \
	done;

run: build
	./platform-api

lint:
	go vet $(pkgs)
	go list ./... | grep -v /vendor/ | xargs -L1 golint

pull:
	docker-compose -f tests/docker-compose.yml pull

up: tests/.docker/config.json
	# --project-directory .
	docker-compose -f tests/docker-compose.yml up -d

down:
	-docker-compose -f tests/docker-compose.yml down



build_api:
	docker build -t $(IMAGE) .

push_api: _docker_login
	docker push $(IMAGE)

create_storage_dir:
	mkdir -p /tmp/platformapi/data/result

run_api_built: create_storage_dir
	docker run -d --rm --link tests_singularity_1 --name platformapi \
	    -e PLATFORMAPI_SINGULARITYADDR=http://tests_singularity_1:7099 \
	    -e PLATFORMAPI_STORAGEBASEPATH=/go/storage \
	    -v /tmp/platformapi:/go/storage \
	    $(IMAGE)

build_api_tests:
	make -C tests/api build

run_api_tests_built:
	docker run --rm --link tests_singularity_1 --link platformapi \
	    platformapi-apitests pytest -vv .

ci_run_api_tests_built:
	docker run --rm --link tests_singularity_1 --link platformapi \
	    -v ${TEST_RESULTS}:/tmp/test-results platformapi-apitests pytest \
	    --junitxml=/tmp/test-results/junit/api-tests.xml -vv .

tests/.docker/config.json:
	sed -e "s/#PASS#/$(DOCKER_SECRET)/g" tests/.docker/config.tpl > tests/.docker/config.json



_docker_login:
	@docker login -u "$(DOCKER_USER)" -p "$(DOCKER_PASS)" $(DOCKER_REGISTRY)

pull_api_test_fixtures: _docker_login
	docker pull $(DOCKER_REPO)/platformapi-dummy

prepare_api_tests: pull_api_test_fixtures \
	build_api run_api_built \
	build_api_tests

run_api_tests: prepare_api_tests run_api_tests_built

ci_run_api_tests: prepare_api_tests ci_run_api_tests_built

include k8s.mk
include deploy.mk

build_api_k8s:
	docker build -f Dockerfile.k8s -t $(IMAGE_K8S):latest -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) .

run_api_k8s:
	NP_STORAGE_HOST_MOUNT_PATH=/tmp \
	NP_K8S_API_URL=https://$$(minikube ip):8443 \
	NP_K8S_CA_PATH=$$HOME/.minikube/ca.crt \
	NP_K8S_AUTH_CERT_PATH=$$HOME/.minikube/client.crt \
	NP_K8S_AUTH_CERT_KEY_PATH=$$HOME/.minikube/client.key \
	platform-api

push_api_k8s: _docker_login
	docker push $(IMAGE_K8S):latest

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
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME)
	sudo chown -R circleci: $(HOME)/.kube

gke_docker_push: build_api_k8s
	docker tag $(IMAGE_K8S):latest $(IMAGE_K8S):$(CIRCLE_SHA1)
	sudo /opt/google-cloud-sdk/bin/gcloud docker -- push $(IMAGE_K8S)

gke_k8s_deploy:
	kubectl patch replicaset platformapi -p '{"spec":{"template":{"spec":{"containers":[{"name":"platformapi","image":"$(IMAGE_K8S):$(CIRCLE_SHA1)"}]}}}}'	        
	kubectl scale --replicas=0 replicaset/platformapi
	sleep 2s;
	kubectl scale --replicas=1 replicaset/platformapi
	