pkgs = $(shell go list ./...)

DOCKER_SECRET ?= $(shell bash -c 'echo -n "$(DOCKER_USER):$(DOCKER_PASS)" | base64')
DOCKER_REGISTRY ?= registry.neuromation.io
DOCKER_REPO ?= $(DOCKER_REGISTRY)/neuromationorg
IMAGE_NAME ?= platformapi
IMAGE_TAG ?= latest
IMAGE ?= $(DOCKER_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)

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

K8S_DIND_CLUSTER_CMD := tests/k8s/dind-cluster-v1.10.sh

$(K8S_DIND_CLUSTER_CMD):
	mkdir -p $(@D)
	curl -Lo $@ https://cdn.rawgit.com/Mirantis/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh
	chmod u+x $@

start_k8s: $(K8S_DIND_CLUSTER_CMD) clean_k8s
	$(K8S_DIND_CLUSTER_CMD) up

K8S_PATH := $(HOME)/.kubeadm-dind-cluster
export PATH := $(K8S_PATH):$(PATH)

k8s_env:
	@echo -n 'export PATH="$(PATH)"'

test_k8s:
	docker exec kube-node-2 docker login -u "$(DOCKER_USER)" -p "$(DOCKER_PASS)" $(DOCKER_REGISTRY)
	PATH=$(PATH) kubectl get all
	PATH=$(PATH) kubectl create secret docker-registry np-docker-reg-secret \
	    --docker-server $(DOCKER_REGISTRY) \
	    --docker-username $$DOCKER_USER \
	    --docker-password $$DOCKER_PASS \
	    --docker-email $$DOCKER_EMAIL
	PATH=$(PATH) kubectl create -f tests/k8s/pod.yml

stop_k8s:
	$(K8S_DIND_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_DIND_CLUSTER_CMD) clean

include deploy.mk
