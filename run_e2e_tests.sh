#!/usr/bin/env bash
set -o verbose

export MINIKUBE_IN_STYLE=true

eval $(minikube docker-env)
make docker_build
make docker_pull_test_images

kubectl config use-context minikube

kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
kubectl delete -f tests/k8s/platformapi.yml
kubectl delete -f tests/k8s/platformconfig.yml
kubectl delete -f tests/k8s/platformapi_migrations.yml

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
kubectl create -f tests/k8s/platformconfig.yml
kubectl create -f tests/k8s/platformapi.yml
kubectl create -f tests/k8s/platformapi_migrations.yml

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

check_job_succeeded() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local job_name=$2
    until [[ `kubectl get jobs $job_name -o custom-columns=status:status.succeeded --no-headers` == "1" ]]; do
      if [ $attempt == $max_attempts ]; then
          echo "Can't connect to the container"
                exit 1
      fi
      sleep 1
      ((attempt++))
    done
}

max_attempts=30


# Wait for DB server to start and apply migrations
check_job_succeeded $max_attempts platformapi-migrations

check_service $max_attempts platformapi
check_service $max_attempts platformauthapi

# wait till our services are up to prevent flakes
sleep 10

export PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(minikube service platformauthapi --url)

echo $PLATFORM_API_URL
echo $AUTH_API_URL

make test_e2e
