#!/usr/bin/env bash

CONTEXT="platform-api"

function tools::kubectl {
    kubectl --context $CONTEXT "$@"
}

function tools::minikube {
    minikube --profile $CONTEXT "$@"
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
