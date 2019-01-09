#!/usr/bin/env bash
set -o verbose
docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest

# NOTE (A Danshyn 10/04/18): in order to run this test locally using a VM-based
# minikube, one needs to uncomment these lines first
# docker save -o /tmp/platformauthapi.image platformauthapi:latest
# docker save -o /tmp/platformapi.image platformapi-k8s:latest
# eval $(minikube docker-env)
# docker load -i /tmp/platformauthapi.image
# docker load -i /tmp/platformapi.image
# kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
# kubectl delete -f tests/k8s/platformapi.yml

# kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
# kubectl create -f tests/k8s/platformapi.yml

# wait for containers to start

check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    until minikube service platformapi --url; do
	if [ $attempt == $max_attempts ]; then
	    echo "Can't connect to the container"
            exit 1
	fi
	sleep 1
	((attempt++))
    done    
}

max_attempts=30

check_service $max_attempts platformapi
check_service $max_attempts platformauthapi

# wait till our services are up to prevent flakes
sleep 10

export PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(minikube service platformauthapi --url)
make test_e2e
