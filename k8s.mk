K8S_DIND_CLUSTER_CMD := tests/k8s/dind-cluster-v1.10.sh

$(K8S_DIND_CLUSTER_CMD):
	mkdir -p $(@D)
	curl -Lo $@ https://cdn.rawgit.com/Mirantis/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh
	chmod u+x $@

# K8S_CLUSTER_CMD := $(K8S_DIND_CLUSTER_CMD)
K8S_CLUSTER_CMD := tests/k8s/cluster.sh

install_k8s:
	$(K8S_CLUSTER_CMD) install

start_k8s: $(K8S_CLUSTER_CMD)
	$(K8S_CLUSTER_CMD) up

test_k8s:
	$(K8S_CLUSTER_CMD) test

stop_k8s:
	$(K8S_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_CLUSTER_CMD) clean
	-docker stop $$(docker ps -a -q)
	-docker rm $$(docker ps -a -q)

test_k8s_e2e:
	./run_e2e_tests.sh
