# K8S_DIND_CLUSTER_CMD := tests/k8s/dind-cluster-v1.10.sh
K8S_DIND_CLUSTER_CMD := tests/k8s/cluster.sh

$(K8S_DIND_CLUSTER_CMD):
	mkdir -p $(@D)
	curl -Lo $@ https://cdn.rawgit.com/Mirantis/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh
	chmod u+x $@


install_k8s:
	$(K8S_DIND_CLUSTER_CMD) install

start_k8s: $(K8S_DIND_CLUSTER_CMD) install_k8s clean_k8s
	$(K8S_DIND_CLUSTER_CMD) up

# K8S_PATH := $(HOME)/.kubeadm-dind-cluster
# K8S_PATH := $(shell pwd)
# export PATH := $(K8S_PATH):$(PATH)

k8s_env:
	@echo -n 'export PATH="$(PATH)"'

test_k8s:
	kubectl get all
	kubectl create secret docker-registry np-docker-reg-secret \
	    --docker-server $(DOCKER_REGISTRY) \
	    --docker-username $$DOCKER_USER \
	    --docker-password $$DOCKER_PASS \
	    --docker-email $$DOCKER_EMAIL
	kubectl create -f tests/k8s/pod.yml

stop_k8s:
	$(K8S_DIND_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_DIND_CLUSTER_CMD) clean
