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

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
kubectl create -f tests/k8s/platformapi.yml

wait for uptime


check_service() { # attempt, max_attempt, service
    local attempt=$1
    local max_attempts=$2
    local service=$3
    until minikube service platformapi --url; do
	if [ $attempt == $max_attempts ]; then
            exit 1
	fi
	sleep 1
	((attempt++))
    done    
}

attempt=1
max_attempts=30

check_service attempt max_attempts platformapi
check_service attempt max_attempts platformauthapi

# wait till everything is up to prevent flakes
sleep 10

export PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(minikube service platformauthapi --url)
make test_e2e
