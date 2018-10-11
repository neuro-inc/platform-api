
# Jump host / Bastion host

## Manual installation guide

### Build docker

```bash
export PROJECT_DIR=...
cd ${PROJECT_DIR}/deploy/jumphost/docker
docker build -t jumphost -f Dockerfile.k8s .
```
 
### Push docker to GCR

```bash
docker tag ...
docker push ...
```

### Deploy

#### Generate secret

```bash
ssh-keygen -q -f jumphost_id_rsa -N '' -t rsa
KEY=$$(cat jumphost_id_rsa.pub |base64)
sed "s/PUBLIC_KEY/$${KEY}/" ${PROJECT_DIR}/deploy/jumphost/docker/templates/secret.gke.yaml	> secret.yaml
kubectl create -f secret.yaml
```

TBD