#!/usr/bin/env bash

CONTEXT="minikube"

function tools::kubectl {
    kubectl --context $CONTEXT "$@"
}

function tools::minikube {
    sudo -E minikube "$@"
}

ARGUMENTS="${@:2}"

case "$1" in
    kubectl)
        tools::kubectl $ARGUMENTS
        ;;
    minikube)
        tools::minikube $ARGUMENTS
        ;;

esac
