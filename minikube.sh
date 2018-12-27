#!/usr/bin/env bash

mkdir -p ~/.minikube/files
cp -R tests/k8s/fluentd ~/.minikube/files

minikube start --kubernetes-version=v1.10.0
