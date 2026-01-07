#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

set -euo pipefail

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
    echo "starting minikube..."
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p "$(dirname "$KUBECONFIG")"
    touch "$KUBECONFIG"

    minikube start \
        --driver=docker \
        --install-addons=true \
        --addons=ingress \
        --feature-gates=DevicePlugins=true \
        --extra-config=kubelet.fail-swap-on=false \
        --wait=all \
        --wait-timeout=5m
    echo "minikube started"
}

function k8s::setup {
    kubectl config use-context minikube

    # Install nvidia device plugin
    kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v1.9/nvidia-device-plugin.yml

    k8s::wait k8s::setup_namespace
    k8s::wait k8s::setup_storageclass
    k8s::wait "kubectl get po --all-namespaces"
    k8s::setup_platform_services
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

function k8s::setup_platform_services {
    echo "Applying platform services..."

    kubectl apply -f tests/k8s/rbac-services.yml
    kubectl apply -f tests/k8s/platformauthapi.yml
    kubectl apply -f tests/k8s/postgres-admin.yml
    kubectl apply -f tests/k8s/postgres-api.yml
    kubectl apply -f tests/k8s/platformconfig.yml
    echo "Waiting for postgres-admin to be ready..."
    kubectl wait --for=condition=ready pod -l service=postgres-admin --timeout=120s || true
    echo "Waiting for platformconfig-postgres to be ready..."
    kubectl wait --for=condition=ready pod -l service=platformconfig-postgres --timeout=120s || true
    echo "Waiting for postgres-api to be ready..."
    kubectl wait --for=condition=ready pod -l service=postgres-api --timeout=120s || true
    # Admin service (depends on auth, postgres, config)
    kubectl apply -f tests/k8s/platformadmin.yml
    kubectl apply -f tests/k8s/platformnotifications.yml
    # Secrets service (depends on auth)
    kubectl apply -f tests/k8s/platformsecrets.yml
    # Disk API service (depends on auth)
    kubectl apply -f tests/k8s/platformdiskapi.yml
    echo "Platform services applied."

    # Wait for all services to be ready
    k8s::wait_for_platform_services

    # Initialize the cluster in platformadmin and platformconfig
    k8s::create_cluster
}

function k8s::wait_for_platform_services {
    echo "Waiting for platform services to be ready..."

    kubectl wait --for=condition=ready pod -l service=platformauthapi --timeout=300s
    kubectl wait --for=condition=ready pod -l service=postgres-admin --timeout=300s
    kubectl wait --for=condition=ready pod -l service=postgres-api --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformconfig-postgres --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformconfig --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformadmin --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformnotifications --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformsecrets --timeout=300s
    kubectl wait --for=condition=ready pod -l service=platformdiskapi --timeout=300s

    echo "All platform services are ready."
}

function k8s::create_cluster {
    echo "Running create-cluster job..."

    # Delete old job if exists
    kubectl delete job create-cluster --ignore-not-found=true

    # Apply the job
    kubectl apply -f tests/k8s/create-cluster-job.yml

    # Wait for the job to complete
    echo "Waiting for create-cluster job to complete..."
    kubectl wait --for=condition=complete job/create-cluster --timeout=120s

    echo "Cluster initialization complete."
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
    setup)
        k8s::setup
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
    apply-services)
        k8s::setup_platform_services
        ;;
    wait-services)
        k8s::wait_for_platform_services
        ;;
    create-cluster)
        k8s::create_cluster
        ;;
    *)
        exit 1
        ;;
esac
