#!/usr/bin/env bash

DIR=`dirname $0`
source $DIR/tests/k8s/tools.sh

mkdir -p ~/.minikube/files/files

tools::minikube start --kubernetes-version=v1.10.13 --memory=4096
