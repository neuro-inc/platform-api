#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install {
    echo "installing minikube..."
    local minikube_version="v1.34.0"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
    echo "minikube installed."

    echo "installing vcluster..."
    curl -L -o vcluster https://github.com/loft-sh/vcluster/releases/download/v0.30.0/vcluster-linux-amd64
    sudo install -c -m 0755 vcluster /usr/local/bin
    rm -f vcluster
    echo "vcluster installed."
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    minikube start \
        --driver=docker \
        --install-addons=true \
        --addons=ingress \
        --feature-gates=DevicePlugins=true \
        --wait=all \
        --wait-timeout=5m
    kubectl config use-context minikube

    # Install nvidia device plugin
    kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v1.9/nvidia-device-plugin.yml

    k8s::wait k8s::setup_namespace
    k8s::wait k8s::setup_storageclass
    k8s::wait "kubectl get po --all-namespaces"
}

function k8s::wait {
    local cmd=$1
    set +e
    # this for loop waits until kubectl can access the api server that Minikube has created
    for i in {1..150}; do # timeout for 5 minutes
        $cmd
        if [ $? -ne 1 ]; then
            break
        fi
        sleep 2
    done
    set -e
}

function k8s::stop {
    sudo -E minikube stop || :
    sudo -E minikube delete || :
    sudo -E rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}

function k8s::setup_namespace {
    kubectl apply -f tests/k8s/namespace.yml
}

function k8s::setup_storageclass {
    kubectl apply -f tests/k8s/storageclass.yml
}

function k8s::test {
    kubectl delete jobs testjob1 || :
    kubectl create -f tests/k8s/pod.yml
    for _ in {1..300}; do
        if [ "$(kubectl get job testjob1 --template {{.status.succeeded}})" == "1" ]; then
            exit 0
        fi
        if [ "$(kubectl get job testjob1 --template {{.status.failed}})" == "1" ]; then
            exit 1
        fi
        sleep 1
    done
    exit 1
}


case "${1:-}" in
    install)
        k8s::install
        ;;
    up)
        k8s::start
        ;;
    down)
        k8s::stop
        ;;
    clean)
        k8s::stop
        ;;
    test)
        k8s::test
        ;;
    *)
        exit 1
        ;;
esac
