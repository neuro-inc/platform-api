pull:
	docker-compose -f tests/docker-compose.yml pull

run:
	# --project-directory .
	docker-compose -f tests/docker-compose.yml up

clean:
	docker-compose -f tests/docker-compose.yml stop || :
	docker-compose -f tests/docker-compose.yml rm -f || :
