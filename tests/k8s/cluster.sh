#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_kubectl {
    local kubectl_version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${kubectl_version}/bin/linux/amd64/kubectl
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin/
}
function k8s::install_minikube {
    # we have to pin this version in order to run minikube on CircleCI
    # Ubuntu 14 VMs. The newer versions depend on systemd.
    local minikube_version="v0.25.2"
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
    sudo -E minikube config set WantReportErrorPrompt false
}

function k8s::install {
    k8s::install_kubectl
    k8s::install_minikube
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    sudo -E mkdir -p ~/.minikube/files
    sudo -E cp -R tests/k8s/fluentd ~/.minikube/files
    sudo -E cp -R tests/k8s/elasticsearch-auth ~/.minikube/files

    sudo -E minikube config set WantReportErrorPrompt false
    sudo -E minikube start --vm-driver=none --kubernetes-version=v1.10.0

    k8s::wait "kubectl get po"
    k8s::wait k8s::start_nfs
    k8s::wait k8s::setup_ingress
    k8s::wait k8s::setup_logging
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

function k8s::setup_registry {
    local DOCKER_REGISTRY=registry.neuromation.io
    kubectl delete secret np-docker-reg-secret || :
    kubectl create secret docker-registry np-docker-reg-secret \
        --docker-server $DOCKER_REGISTRY \
        --docker-username $DOCKER_USER \
        --docker-password $DOCKER_PASS \
        --docker-email $DOCKER_EMAIL
}

function k8s::setup_ingress {
    sudo -E minikube addons enable ingress
    # NOTE: minikube --vm-driver=none --kubernetes-version=v1.10.0 stopped
    # launching the ingress services for some unknown reason!
    find /etc/kubernetes/addons/ -name ingress* | xargs -L 1 sudo kubectl -n kube-system apply -f
    find /etc/kubernetes/addons/ -name kube-dns* | xargs -L 1 sudo kubectl -n kube-system apply -f
    kubectl create -f tests/k8s/platformjobsingress.yml
}

function k8s::setup_logging {
    kubectl apply -f tests/k8s/logging.yml
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
    start-nfs)
        k8s::start_nfs
        ;;
    stop-nfs)
        k8s::stop_nfs
        ;;
    setup-registry)
        k8s::setup_registry
        ;;
    *)
        exit 1
        ;;
esac
