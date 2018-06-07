K8S_DIND_CLUSTER_CMD := tests/k8s/dind-cluster-v1.10.sh

$(K8S_DIND_CLUSTER_CMD):
	mkdir -p $(@D)
	curl -Lo $@ https://cdn.rawgit.com/Mirantis/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh
	chmod u+x $@

# K8S_CLUSTER_CMD := $(K8S_DIND_CLUSTER_CMD)
K8S_CLUSTER_CMD := tests/k8s/cluster.sh

install_k8s:
	$(K8S_CLUSTER_CMD) install

start_k8s: $(K8S_CLUSTER_CMD) install_k8s clean_k8s
	$(K8S_CLUSTER_CMD) up

test_k8s:
	$(K8S_CLUSTER_CMD) test

stop_k8s:
	$(K8S_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_CLUSTER_CMD) clean

test_k8s_platform_api:
	pip install tox
	kubectl config view
	tox

test_k8s_platform_api_e2e: build_api_k8s
	kubectl create -f deploy/rb.default.gke.yml
	kubectl create -f tests/k8s/platformapi.yml
	PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1 tox e2e
