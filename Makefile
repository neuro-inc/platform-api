PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMSECRETS_IMAGE = $(shell cat PLATFORMSECRETS_IMAGE)
PLATFORMDISKAPI_IMAGE = $(shell cat PLATFORMDISKAPI_IMAGE)
PLATFORMADMIN_IMAGE = $(shell cat PLATFORMADMIN_IMAGE)

include k8s.mk

.PHONY: venv
venv:
	poetry lock
	poetry install --with dev;

.PHONY: build
build: venv poetry-plugins

.PHONY: setup
setup: venv
	poetry run pre-commit install;

.PHONY: poetry-plugins
poetry-plugins:
	poetry self add "poetry-dynamic-versioning[plugin]"; \
    poetry self add "poetry-plugin-export";

lint: format
	poetry run mypy --show-error-codes platform_api tests alembic

format:
ifdef CI
	poetry run pre-commit run --all-files --show-diff-on-failure
else
	poetry run pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	poetry run pytest -vv --cov platform_api --cov-config=pyproject.toml --cov-report xml:.coverage-unit.xml tests/unit

.PHONY: test_integration
test_integration:
	poetry run pytest -x -s --log-cli-level=DEBUG -vv --maxfail=3 --durations=20 --cov platform_api --cov-config=pyproject.toml --cov-report xml:.coverage-integration.xml tests/integration

.PHONY: docker_build
docker_build: .python-version dist
	PY_VERSION=$$(cat .python-version) && \
	docker build \
		-t platformapi:latest \
		--build-arg PY_VERSION=$$PY_VERSION \
		.

.python-version:
	@echo "Error: .python-version file is missing!" && exit 1

.PHONY: dist
dist: build
	rm -rf build dist; \
	poetry export -f requirements.txt --without-hashes -o requirements.txt; \
	poetry build -f wheel;

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
