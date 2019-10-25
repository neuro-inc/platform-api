#!/usr/bin/env bash -x
DIR=`dirname $0`
source $DIR/tests/k8s/tools.sh

set -o verbose
docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest
docker tag $GKE_DOCKER_REGISTRY/$GKE_PROJECT_ID/platformconfig:9d7cea532a7ab0e45871cb48cf355427a274dbd9 platformconfig:latest

if [ ! "$CI" = true ]; then
    echo "Setting up external services"
    docker save -o /tmp/platformauthapi.image platformauthapi:latest
    docker save -o /tmp/platformapi.image platformapi:latest
    docker save -o /tmp/ssh-auth.image ssh-auth:latest
    docker save -o /tmp/platformconfig.image platformconfig:latest

    eval $(tools::minikube docker-env)
    docker load -i /tmp/platformauthapi.image
    docker load -i /tmp/platformapi.image
    docker load -i /tmp/ssh-auth.image
    docker load -i /tmp/platformconfig.image
    tools::kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
    tools::kubectl delete -f tests/k8s/platformapi.yml
    tools::kubectl delete -f tests/k8s/platformconfig.yml
    echo "Services set up"
fi

tools::kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
tools::kubectl create -f tests/k8s/platformconfig.yml
tools::kubectl create -f tests/k8s/platformapi.yml


check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    until tools::minikube service $service --url; do
      if [ $attempt == $max_attempts ]; then
          echo "Can't connect to the container"
          exit 1
      fi
      sleep 1
      ((attempt++))
    done
}

ping_service() { # max_attempt, url
    local attempt=1
    local max_attempts=$1
    local url=$2
    until curl -sS $url; do
      if [ $attempt == $max_attempts ]; then
          echo "Can't ping $url"
          exit 1
      fi
      sleep 1
      ((attempt++))
    done
}

max_attempts=30

# wait for containers to start
check_service $max_attempts platformapi
check_service $max_attempts platformauthapi
check_service $max_attempts ssh-auth

export PLATFORM_API_URL=$(tools::minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(tools::minikube service platformauthapi --url)
export SSH_AUTH_URL=$(tools::minikube service ssh-auth --url)

# wait until services will be available
ping_service $max_attempts $PLATFORM_API_URL/ping
ping_service $max_attempts $AUTH_API_URL/api/v1/ping

make test_e2e
