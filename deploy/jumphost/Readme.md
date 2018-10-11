
# Jump host / Bastion host

## Manual installation guide

Please note that this section is manual deployment.

### Pre-requisites

- docker client authorized against GCR (gcloud usually does it)

- kubectl configured against your GCP GKE

- helm installed

### Build docker

```bash
export PROJECT_DIR=...
cd ${PROJECT_DIR}/deploy/jumphost/docker
docker build -t jumphost -f Dockerfile.k8s .
```
 
### Push docker to GCR

```bash
docker tag jumphost:latest gcr.io/light-reality-205619/jumphost-openssh-k8s:latest
docker push gcr.io/light-reality-205619/jumphost-openssh-k8s:latest
```

### Deploy

#### Generate secret

```bash
ssh-keygen -q -f jumphost_id_rsa -N '' -t rsa
KEY=$(cat jumphost_id_rsa.pub |base64)
sed "s/PUBLIC_KEY/${KEY}/" ${PROJECT_DIR}/deploy/jumphost/secret.gke.yml > secret.yml
kubectl create -f secret.yml
```

#### Deploy Service and JumpHost

```bash
helm --set "global.env=dev" install --debug jumphost/helmpackage
```

#### Post deploy smoke

Check kubernetes status:

```bash
# next would print jump hostpod
kubectl get pods | grep jumphost

# next should show you our service
kubectl get service jumphost
```

You would need to get and External IP address of your service, in order to use in the next command.

```bash
ssh -i jumphost_id_rsa root@<External IP>
```
