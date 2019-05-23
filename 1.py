from platform_api.orchestrator.kube_orchestrator import JobStatusItemFactory
from platform_api.orchestrator.kube_client import PodStatus
import yaml


payload = yaml.load("""
apiVersion: v1
kind: Pod
metadata:
  annotations:
    cni.projectcalico.org/podIP: 10.44.11.82/32
  creationTimestamp: 2019-05-23T12:35:35Z
  labels:
    job: job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
    platform.neuromation.io/job: job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
    platform.neuromation.io/user: shagren
  name: job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
  namespace: default
  resourceVersion: "40359114"
  selfLink: /api/v1/namespaces/default/pods/job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
  uid: 43754693-7d57-11e9-9d65-42010a800018
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: cloud.google.com/gke-preemptible
            operator: DoesNotExist
  containers:
  - args:
    - bash
    - -c
    - /bin/df --block-size M --output=target,avail /dev/shm | grep 64M
    image: ubuntu:latest
    imagePullPolicy: Always
    name: job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
    resources:
      limits:
        cpu: 100m
        memory: 20Mi
      requests:
        cpu: 100m
        memory: 20Mi
    terminationMessagePath: /dev/termination-log
    terminationMessagePolicy: FallbackToLogsOnError
    volumeMounts:
    - mountPath: /dev/shm
      name: dshm
      subPath: .
    - mountPath: /var/run/secrets/kubernetes.io/serviceaccount
      name: default-token-5ldz6
      readOnly: true
  dnsPolicy: ClusterFirst
  imagePullSecrets:
  - name: neurouser-shagren
  nodeName: gke-dev-regional-cluster-default-pool-d408f582-mlc3
  priority: 0
  restartPolicy: Never
  schedulerName: default-scheduler
  securityContext: {}
  serviceAccount: default
  serviceAccountName: default
  terminationGracePeriodSeconds: 30
  tolerations:
  - effect: NoExecute
    key: node.kubernetes.io/not-ready
    operator: Exists
    tolerationSeconds: 300
  - effect: NoExecute
    key: node.kubernetes.io/unreachable
    operator: Exists
    tolerationSeconds: 300
  volumes:
  - name: storage
    nfs:
      path: /dev2premium
      server: 10.76.126.154
  - emptyDir:
      medium: Memory
    name: dshm
  - name: default-token-5ldz6
    secret:
      defaultMode: 420
      secretName: default-token-5ldz6
status:
  conditions:
  - lastProbeTime: null
    lastTransitionTime: 2019-05-23T12:35:35Z
    status: "True"
    type: Initialized
  - lastProbeTime: null
    lastTransitionTime: 2019-05-23T12:35:35Z
    message: 'containers with unready status: [job-436d08e5-5ebf-4b25-bb18-9c7c589158aa]'
    reason: ContainersNotReady
    status: "False"
    type: Ready
  - lastProbeTime: null
    lastTransitionTime: 2019-05-23T12:35:35Z
    status: "True"
    type: PodScheduled
  containerStatuses:
  - containerID: docker://a5c1c8fed992b63d8b1e393ba1f58217ecf012217301b16e16041453e43c723c
    image: ubuntu:latest
    imageID: docker-pullable://ubuntu@sha256:b36667c98cf8f68d4b7f1fb8e01f742c2ed26b5f0c965a788e98dfe589a4b3e4
    lastState: {}
    name: job-436d08e5-5ebf-4b25-bb18-9c7c589158aa
    ready: false
    restartCount: 0
    state:
      terminated:
        containerID: docker://a5c1c8fed992b63d8b1e393ba1f58217ecf012217301b16e16041453e43c723c
        exitCode: 1
        finishedAt: 2019-05-23T12:35:41Z
        reason: Error
        startedAt: 2019-05-23T12:35:41Z
  hostIP: 10.128.0.35
  phase: Failed
  podIP: 10.44.11.82
  qosClass: Guaranteed
  startTime: 2019-05-23T12:35:35Z
""")

pod_status = PodStatus(payload['status'])
jsi_factory = JobStatusItemFactory(pod_status)
print(jsi_factory)
print(jsi_factory.create())