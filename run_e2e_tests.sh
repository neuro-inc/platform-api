#!/usr/bin/env bash
set -o verbose
docker build -f tests/e2e/docker/Dockerfile.socat . -t socat:latest
docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest

if [ ! "$CI" = true ]; then
    echo "Setting up external services"
    docker save -o /tmp/platformauthapi.image platformauthapi:latest
    docker save -o /tmp/platformapi.image platformapi-k8s:latest
    docker save -o /tmp/ssh-auth.image ssh-auth:latest
    docker save -o /tmp/socat.image socat:latest
    eval $(minikube docker-env)
    docker load -i /tmp/platformauthapi.image
    docker load -i /tmp/platformapi.image
    docker load -i /tmp/ssh-auth.image
    docker load -i /tmp/socat.image
    kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
    kubectl delete -f tests/k8s/platformapi.yml
    echo "Services set up"
fi

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
kubectl create -f tests/k8s/platformapi.yml

# wait for containers to start

check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    until minikube service $service --url; do
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
check_service $max_attempts ssh-auth

# wait till our services are up to prevent flakes
sleep 10

export PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(minikube service platformauthapi --url)
export SSH_AUTH_URL=$(minikube service ssh-auth --url)
make test_e2e
