#!/usr/bin/env bash
set -o verbose

export MINIKUBE_IN_STYLE=true

eval $(minikube docker-env)
make docker_build
make docker_build_ssh_auth
make docker_pull_test_images

kubectl config use-context minikube

kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
kubectl delete -f tests/k8s/platformapi.yml
kubectl delete -f tests/k8s/platformconfig.yml
kubectl delete -f tests/k8s/platformapi_migraions.yml

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
kubectl create -f tests/k8s/platformconfig.yml
kubectl create -f tests/k8s/platformapi.yml
kubectl create -f tests/k8s/platformapi_migraions.yml

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

echo $PLATFORM_API_URL
echo $AUTH_API_URL
echo $SSH_AUTH_URL

make test_e2e
