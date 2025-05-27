#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.25.2/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
    sudo modprobe br_netfilter
    echo 1 | sudo tee -a /proc/sys/net/bridge/bridge-nf-call-iptables
    echo 1 | sudo tee -a /proc/sys/net/ipv4/ip_forward

    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    sudo -E mkdir -p ~/.minikube/files/files
    sudo -E minikube config set WantUpdateNotification false
    sudo -E minikube config set WantNoneDriverWarning false

    sudo -E minikube start \
        --driver=none \
        --install-addons=true \
        --addons=ingress \
        --feature-gates=DevicePlugins=true \
        --wait=all \
        --wait-timeout=5m

    # Install nvidia device plugin
    kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v1.9/nvidia-device-plugin.yml

    k8s::wait k8s::setup_namespace
    k8s::wait k8s::setup_storageclass
    k8s::wait k8s::start_nfs
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

function k8s::start_nfs {
    kubectl apply -f tests/k8s/nfs.yml
}

function k8s::stop_nfs {
    kubectl delete -f tests/k8s/nfs.yml
}


case "${1:-}" in
    install)
        k8s::install_minikube
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
    start-nfs)
        k8s::start_nfs
        ;;
    stop-nfs)
        k8s::stop_nfs
        ;;
    *)
        exit 1
        ;;
esac
