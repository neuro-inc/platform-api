pull:
	docker-compose -f tests/docker-compose.yml pull

run:
	# --project-directory .
	docker-compose -f tests/docker-compose.yml up -d

clean:
	-docker-compose -f tests/docker-compose.yml stop
	-docker-compose -f tests/docker-compose.yml rm -f


build_api_tests:
	make -C tests/api build

run_api_tests_built:
	docker run --rm --link tests_singularity_1 \
	    platformapi-apitests pytest -vv .

run_api_tests: run build_api_tests run_api_tests_built
