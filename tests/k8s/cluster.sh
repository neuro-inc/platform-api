#!/usr/bin/env bash

func k8s::start {
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    chmod +x minikube

    local kubectl_version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${kubectl_version}/bin/linux/amd64/kubectl
    chmod +x kubectl

    export KUBECONFIG=$HOME/.kube/config
    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    sudo -E ./minikube start --vm-driver=none

    # this for loop waits until kubectl can access the api server that Minikube has created
    for i in {1..150}; do # timeout for 5 minutes
        ./kubectl get po &> /dev/null
        if [ $? -ne 1 ]; then
            break
        fi
        sleep 2
    done
}

func k8s::stop {
    sudo -E ./minikube stop
}


case "${1:-}" in
    up)
        k8s::start
        ;;
    down)
        k8s::stop
        ;;
    clean)
        k8s::stop
        ;;
    *)
        exit 1
        ;;
esac
