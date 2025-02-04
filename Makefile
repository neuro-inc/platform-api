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
ifdef CI
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --durations=20 --cov platform_api --cov-config=setup.cfg --cov-report xml:.coverage-integration.xml tests/integration

docker_build:
	rm -rf build dist
	pip install -U build
	python -m build
	docker build -t platformapi:latest .

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
	    platformapi:latest

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
