#!/usr/bin/env bash
set -o verbose
docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest
docker tag $GKE_DOCKER_REGISTRY/$GKE_PROJECT_ID/platformconfig:9d7cea532a7ab0e45871cb48cf355427a274dbd9 platformconfig:latest

if [ ! "$CI" = true ]; then
    kubectl config use-context minikube
    echo "Setting up external services"
    docker save -o /tmp/platformauthapi.image platformauthapi:latest
    docker save -o /tmp/platformapi.image platformapi:latest
    docker save -o /tmp/ssh-auth.image ssh-auth:latest
    docker save -o /tmp/platformconfig.image platformconfig:latest

    eval $(minikube docker-env)
    docker load -i /tmp/platformauthapi.image
    docker load -i /tmp/platformapi.image
    docker load -i /tmp/ssh-auth.image
    docker load -i /tmp/platformconfig.image
    kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
    kubectl delete -f tests/k8s/platformapi.yml
    kubectl delete -f tests/k8s/platformconfig.yml
    echo "Services set up"
fi

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
kubectl create -f tests/k8s/platformconfig.yml
kubectl create -f tests/k8s/platformapi.yml

# wait for containers to start

check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    until MINIKUBE_IN_STYLE=true minikube service $service --url; do
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
