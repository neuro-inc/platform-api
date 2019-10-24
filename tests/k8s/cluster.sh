#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

DIR=`dirname $0`
source $DIR/tools.sh

function k8s::install_kubectl {
    local kubectl_version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${kubectl_version}/bin/linux/amd64/kubectl
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin/
}
function k8s::install_minikube {
    local minikube_version="v1.2.0"
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::install {
    k8s::install_kubectl
    k8s::install_minikube
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)

    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    sudo -E mkdir -p ~/.minikube/files/files

    tools::minikube config set WantReportErrorPrompt false
    tools::minikube config set WantUpdateNotification false
    tools::minikube config set WantNoneDriverWarning false
    tools::minikube config set WantKubectlDownloadMsg false

    sudo minikube --profile $CONTEXT start --vm-driver=none --kubernetes-version=v1.10.13

    sudo chown -R $USER $HOME/.kube $HOME/.minikube
    k8s::wait k8s::setup_namespace
    k8s::wait k8s::start_nfs
    k8s::wait k8s::setup_ingress
    k8s::wait k8s::setup_logging

    tools::kubectl wait --for=condition=Ready  pod -l service=platformstoragenfs --timeout=5m

    tools::kubectl get all --all-namespaces
}

function k8s::wait {
    local cmd=$@
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
    tools::minikube stop || :
}

function k8s::clean  {
    tools::minikube delete || :

    orphans=`docker ps -a -q --filter "name=${CONTEXT}--"`
    if [ -n "$orphans" ]
    then
        docker kill $orphans || :
        docker rm $orphans || :
    fi
}

function k8s::setup_namespace {
    tools::kubectl apply -f tests/k8s/namespace.yml
}


function k8s::setup_ingress {
    tools::minikube addons enable ingress
    tools::kubectl create -f tests/k8s/platformjobsingress.yml --namespace=platformapi-tests
}

function k8s::setup_logging {
    tools::kubectl apply -f tests/k8s/logging.yml
}

function k8s::test {
    tools::kubectl delete jobs testjob1 || :
    tools::kubectl create -f tests/k8s/pod.yml
    tools::kubectl wait --for=condition=Ready  pod -l name=testjob1 --timeout=5m
    exit $?
}

function k8s::start_nfs {
    sudo modprobe nfs || :
    sudo modprobe nfsd || :
    tools::kubectl apply -f tests/k8s/nfs.yml
}

function k8s::stop_nfs {
    tools::kubectl delete -f tests/k8s/nfs.yml
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
        k8s::clean
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
