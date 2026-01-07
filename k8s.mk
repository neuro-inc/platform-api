tests/bin/kind:
	mkdir -p $(@D)
	# for Intel Macs
	[ $$(uname -m) = x86_64 ] && curl -Lo $@ https://kind.sigs.k8s.io/dl/v0.17.0/kind-darwin-amd64 || :
	# for M1 / ARM Macs
	[ $$(uname -m) = arm64 ] && curl -Lo $@ https://kind.sigs.k8s.io/dl/v0.17.0/kind-darwin-arm64 || :
	chmod +x $@

# tests/bin/kind create cluster --name neuro --wait 5m
# kubectl cluster-info --context kind-neuro

K8S_CLUSTER_CMD := tests/k8s/cluster.sh

install_k8s:
	$(K8S_CLUSTER_CMD) install

start_k8s: $(K8S_CLUSTER_CMD)
	$(K8S_CLUSTER_CMD) up


setup_k8s: $(K8S_CLUSTER_CMD)
	$(K8S_CLUSTER_CMD) setup

test_k8s:
	$(K8S_CLUSTER_CMD) test

stop_k8s:
	$(K8S_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_CLUSTER_CMD) clean
	-docker stop $$(docker ps -a -q)
	-docker rm $$(docker ps -a -q)
