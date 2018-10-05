#!/usr/bin/env bash

docker tag $(cat AUTH_SERVER_IMAGE_NAME) platformauthapi:latest

# NOTE (A Danshyn 10/04/18): in order to run this test locally using a VM-based
# minikube, one needs to uncomment these lines first
# docker save -o /tmp/platformauthapi.image platformauthapi:latest
# docker save -o /tmp/platformapi.image platformapi-k8s:latest
# eval $(minikube docker-env)
# docker load -i /tmp/platformauthapi.image
# docker load -i /tmp/platformapi.image

kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
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
