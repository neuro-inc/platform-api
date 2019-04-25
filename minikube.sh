#!/usr/bin/env bash

mkdir -p ~/.minikube/files/files
cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf
cp tests/k8s/elasticsearch-auth/nginx/* ~/.minikube/files/files

minikube start --kubernetes-version=v1.10.0
