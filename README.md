[![codecov](https://codecov.io/gh/neuromation/platform-api/branch/master/graph/badge.svg?token=UhSf3Bzfe0)](https://codecov.io/gh/neuromation/platform-api)
# Platform API

## Local Development
1. Install minikube (https://github.com/kubernetes/minikube#installation);
2. Launch minikube:
```shell
minikube start
```
3. Check the minikube k8s cluster status:
```shell
minikube status
```
4. Make sure the kubectl tool uses the minikube k8s cluster:
```shell
kubectl config use-context minikube
```
5. Apply some k8s fixture services:
```shell
kubectl apply -f tests/k8s/nfs.yml
kubectl apply -f tests/k8s/platformjobsingress.yml
```
