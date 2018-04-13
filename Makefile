pkgs = $(shell go list ./...)

format:
	go fmt $(pkgs)
	gofmt -w -s .

build:
	go build

test: build
	go test -v -race $(pkgs)

go_integration_test: build
	go test -v -race $(pkgs) -tags=integration

run: build
	./platform-api

lint:
	go vet $(pkgs)
	go list ./... | grep -v /vendor/ | xargs -L1 golint


pull:
	docker-compose -f tests/docker-compose.yml pull

up:
	# --project-directory .
	docker-compose -f tests/docker-compose.yml up -d

down:
	-docker-compose -f tests/docker-compose.yml down

build_api_tests:
	make -C tests/api build

run_api_tests_built:
	docker run --rm --link tests_singularity_1 \
	    platformapi-apitests pytest -vv .

ci_run_api_tests_built:
	docker run --rm --link tests_singularity_1 \
	    -v ${TEST_RESULTS}:/tmp/test-results platformapi-apitests pytest \
	    --junitxml=/tmp/test-results/junit/api-tests.xml -vv .

run_api_tests: build_api_tests run_api_tests_built

ci_run_api_tests: build_api_tests ci_run_api_tests_built