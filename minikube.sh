#!/usr/bin/env bash

mkdir -p ~/.minikube/files/files
cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf

minikube start --kubernetes-version=v1.10.0
