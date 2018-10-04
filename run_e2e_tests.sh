#!/usr/bin/env bash

docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest

kubectl create -f deploy/rb.default.gke.yml
kubectl create -f tests/k8s/platformapi.yml

attempt=1
max_attempts=30
until minikube service platformapi --url; do
    if [ $attempt == $max_attempts ]; then
        exit 1
    fi
    sleep 1
    ((attempt++))
done

PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1 make test_e2e
