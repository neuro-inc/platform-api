pull:
	docker-compose -f tests/docker-compose.yml pull

run:
	# --project-directory .
	docker-compose -f tests/docker-compose.yml up -d

clean:
	-docker-compose -f tests/docker-compose.yml stop
	-docker-compose -f tests/docker-compose.yml rm -f

run_test:
	# --project-directory .
	sed -e "s/#PASS#/${DOCKER_SECRET}/g" tests/.docker/config.tpl > tests/.docker/config.json
	tar czvf tests/docker.tar.gz -C tests .docker/
	docker-compose -f tests/docker-compose.test.yml up -d

build_api_tests:
	make -C tests/api build

run_api_tests_built:
	docker run --rm --link tests_singularity_1 \
	    platformapi-apitests pytest -vv .

run_api_tests: run_test build_api_tests run_api_tests_built
