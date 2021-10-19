[![codecov](https://codecov.io/gh/neuromation/platform-api/branch/master/graph/badge.svg?token=UhSf3Bzfe0)](https://codecov.io/gh/neuromation/platform-api)
# Platform API


## Local Development
1. Install minikube (https://github.com/kubernetes/minikube#installation);
2. Launch minikube:
```shell
mkdir -p ~/.minikube/files/files
minikube start --kubernetes-version=v1.14.0
```
3. Check the minikube k8s cluster status:
```shell
minikube status
```
4. Make sure the kubectl tool uses the minikube k8s cluster:
```shell
kubectl config use-context minikube
```
5. Apply minikube configuration and some k8s fixture services:
```shell
minikube addons enable ingress
kubectl apply -f tests/k8s/namespace.yml
kubectl apply -f tests/k8s/nfs.yml
kubectl apply -f tests/k8s/storageclass.yml
```
6. Create a new virtual environment with Python 3.6:
```shell
python -m venv venv
source venv/bin/activate
```
7. Install dev dependencies:
```shell
pip install -e .[dev]
```
8. Run the unit test suite:
```shell
pytest -vv tests/unit
```
8. Run the integration test suite:
```shell
pytest -vv tests/integration
```
