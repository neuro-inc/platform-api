import abc
import asyncio
import enum
import logging
import ssl
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Dict, List, Optional
from urllib.parse import urlsplit

import aiohttp
from async_generator import asynccontextmanager
from async_timeout import timeout

from .job_request import (
    ContainerResources, ContainerVolume, JobError, JobNotFoundException,
    JobRequest
)


logger = logging.getLogger(__name__)


class KubeClientException(Exception):
    pass


class StatusException(KubeClientException):
    pass


def _raise_status_job_exception(pod: dict, job_id: str):
    if pod['code'] == 409:
        raise JobError(f'job with {job_id} already exist')
    elif pod['code'] == 404:
        raise JobNotFoundException(f'job {job_id} was not found')
    elif pod['code'] == 422:
        raise JobError(f'cant create job with id {job_id}')
    else:
        raise JobError('unexpected')


@dataclass(frozen=True)
class Volume(metaclass=abc.ABCMeta):
    name: str

    @abc.abstractmethod
    def create_mount(
            self, container_volume: ContainerVolume
            ) -> 'VolumeMount':
        raise NotImplementedError(
            'Create specific implementation of Volume class.'
        )


@dataclass(frozen=True)
class PathVolume(Volume):
    path: PurePath

    def create_mount(
            self, container_volume: ContainerVolume
            ) -> 'VolumeMount':
        sub_path = container_volume.src_path.relative_to(self.path)
        return VolumeMount(  # type: ignore
            volume=self,
            mount_path=container_volume.dst_path,
            sub_path=sub_path,
            read_only=container_volume.read_only
        )


@dataclass(frozen=True)
class HostVolume(PathVolume):

    def to_primitive(self):
        return {
            'name': self.name,
            'hostPath': {
                'path': str(self.path),
                'type': 'Directory',
            },
        }


@dataclass(frozen=True)
class SharedMemoryVolume(Volume):

    def to_primitive(self):
        return {
            'name': self.name,
            'emptyDir': {
                'medium': 'Memory',
            },
        }

    def create_mount(
            self, container_volume: ContainerVolume
            ) -> 'VolumeMount':
        return VolumeMount(  # type: ignore
            volume=self,
            mount_path=PurePath('/dev/shm'),
            sub_path=PurePath(''),
            read_only=False
        )


@dataclass(frozen=True)
class NfsVolume(PathVolume):
    server: str

    def to_primitive(self):
        return {
            'name': self.name,
            'nfs': {
                'server': self.server,
                'path': str(self.path),
            },
        }


@dataclass(frozen=True)
class VolumeMount:
    volume: Volume
    mount_path: PurePath
    sub_path: PurePath = PurePath('')
    read_only: bool = False

    def to_primitive(self):
        return {
            'name': self.volume.name,
            'mountPath': str(self.mount_path),
            'readOnly': self.read_only,
            'subPath': str(self.sub_path),
        }


@dataclass(frozen=True)
class Resources:
    cpu: float
    memory: int
    gpu: Optional[int] = None
    shm: Optional[bool] = None

    @property
    def cpu_mcores(self) -> str:
        mcores = int(self.cpu * 1000)
        return f'{mcores}m'

    @property
    def memory_mib(self) -> str:
        return f'{self.memory}Mi'

    def to_primitive(self):
        payload = {
            'limits': {
                'cpu': self.cpu_mcores,
                'memory': self.memory_mib,
            },
        }
        if self.gpu:
            payload['limits']['nvidia.com/gpu'] = self.gpu
        return payload

    @classmethod
    def from_container_resources(
            cls, resources: ContainerResources) -> 'Resources':
        return cls(  # type: ignore
            cpu=resources.cpu, memory=resources.memory_mb, gpu=resources.gpu,
            shm=resources.shm
        )


@dataclass(frozen=True)
class Service:
    name: str
    target_port: int
    port: int = 80

    def to_primitive(self):
        return {
            'metadata': {'name': self.name},
            'spec': {
                'type': 'NodePort',
                'ports': [{
                    'port': self.port,
                    'targetPort': self.target_port,
                }],
                'selector': {
                    'job': self.name
                }
            },
        }

    @classmethod
    def create_for_pod(cls, pod: 'PodDescriptor') -> 'Service':
        return cls(pod.name, target_port=pod.port)  # type: ignore

    @classmethod
    def from_primitive(cls, payload) -> 'Service':
        port_payload = payload['spec']['ports'][0]
        return cls(  # type: ignore
            name=payload['metadata']['name'],
            target_port=port_payload['targetPort'],
            port=port_payload['port'],
        )


@dataclass(frozen=True)
class IngressRule:
    host: str
    service_name: Optional[str] = None
    service_port: Optional[int] = None

    @classmethod
    def from_primitive(cls, payload):
        http_paths = payload.get('http', {}).get('paths', [])
        http_path = http_paths[0] if http_paths else {}
        backend = http_path.get('backend', {})
        service_name = backend.get('serviceName')
        service_port = backend.get('servicePort')
        return cls(
            host=payload.get('host', ''),
            service_name=service_name,
            service_port=service_port,
        )

    def to_primitive(self):
        payload = {
            'host': self.host,
        }
        if self.service_name:
            payload['http'] = {
                'paths': [{
                    'backend': {
                        'serviceName': self.service_name,
                        'servicePort': self.service_port,
                    },
                }],
            }
        return payload

    @classmethod
    def from_service(cls, domain_name: str, service: Service) -> 'IngressRule':
        host = f'{service.name}.{domain_name}'
        return cls(  # type: ignore
            host=host, service_name=service.name, service_port=service.port)


@dataclass(frozen=True)
class Ingress:
    name: str
    rules: List[IngressRule] = field(default_factory=list)

    def to_primitive(self):
        rules = [rule.to_primitive() for rule in self.rules] or [None]
        return {
            'metadata': {'name': self.name},
            'spec': {'rules': rules}
        }

    @classmethod
    def from_primitive(cls, payload):
        # TODO (A Danshyn 06/13/18): should be refactored along with PodStatus
        kind = payload['kind']
        if kind == 'Ingress':
            rules = [
                IngressRule.from_primitive(rule)
                for rule in payload['spec']['rules']]
            return cls(name=payload['metadata']['name'], rules=rules)
        elif kind == 'Status':
            _raise_status_job_exception(payload, job_id=None)
        else:
            raise ValueError(f'unknown kind: {kind}')

    def find_rule_index_by_host(self, host: str) -> int:
        for idx, rule in enumerate(self.rules):
            if rule.host == host:
                return idx
        return -1


@dataclass(frozen=True)
class PodDescriptor:
    name: str
    image: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    volume_mounts: List[VolumeMount] = field(default_factory=list)
    volumes: List[Volume] = field(default_factory=list)
    resources: Optional[Resources] = None

    port: Optional[int] = None
    health_check_path: str = '/'

    status: Optional['PodStatus'] = None

    @classmethod
    def from_job_request(
            cls, volume: Volume, job_request: JobRequest) -> 'PodDescriptor':
        container = job_request.container
        volume_mounts = [
            volume.create_mount(container_volume)
            for container_volume in container.volumes]
        volumes = [volume]

        if job_request.container.resources.shm:
            dev_shm_volume = SharedMemoryVolume(   # type: ignore
                name='dshm'
            )
            container_volume = ContainerVolume(dst_path=PurePath('/dev/shm'),
                                               src_path=PurePath(''))
            volume_mounts.append(dev_shm_volume.create_mount(
                container_volume))
            volumes.append(dev_shm_volume)

        resources = Resources.from_container_resources(container.resources)
        return cls(  # type: ignore
            name=job_request.job_id,
            image=container.image,
            args=container.command_list,
            env=container.env.copy(),
            volume_mounts=volume_mounts,
            volumes=volumes,
            resources=resources,
            port=container.port,
            health_check_path=container.health_check_path,
        )

    @property
    def env_list(self):
        return [
            dict(name=name, value=value) for name, value in self.env.items()]

    def to_primitive(self):
        volume_mounts = [mount.to_primitive() for mount in self.volume_mounts]
        volumes = [volume.to_primitive() for volume in self.volumes]

        container_payload = {
            'name': f'{self.name}',
            'image': f'{self.image}',
            'env': self.env_list,
            'volumeMounts': volume_mounts,
            'terminationMessagePolicy': 'FallbackToLogsOnError',
        }
        if self.args:
            container_payload['args'] = self.args
        if self.resources:
            container_payload['resources'] = self.resources.to_primitive()
        if self.port:
            container_payload['ports'] = [{'containerPort': self.port}]
            container_payload['readinessProbe'] = {
                'httpGet': {
                    'port': self.port,
                    'path': self.health_check_path,
                },
                'initialDelaySeconds': 1,
                'periodSeconds': 1,
            }
        return {
            'kind': 'Pod',
            'apiVersion': 'v1',
            'metadata': {
                'name': self.name,
                'labels': {
                    # TODO (A Danshyn 06/13/18): revisit the naming etc
                    'job': self.name
                },
            },
            'spec': {
                'containers': [container_payload],
                'volumes': volumes,
                'restartPolicy': 'Never',
            }
        }

    @classmethod
    def _assert_resource_kind(cls, expected_kind: str, payload: Dict):
        kind = payload['kind']
        if kind == 'Status':
            _raise_status_job_exception(payload, job_id='')
        elif kind != expected_kind:
            raise ValueError(f'unknown kind: {kind}')

    @classmethod
    def from_primitive(cls, payload):
        cls._assert_resource_kind(expected_kind='Pod', payload=payload)

        metadata = payload['metadata']
        container_payload = payload['spec']['containers'][0]
        # TODO (R Zubairov 09/13/18): remove medium emptyDir
        # TODO (A Danshyn 06/19/18): set rest of attributes
        status = None
        if 'status' in payload:
            status = PodStatus.from_primitive(payload['status'])
        return cls(
            name=metadata['name'],
            image=container_payload['image'],
            status=status,
        )


class ContainerStatus:
    def __init__(self, payload=None):
        self._payload = payload or {}

    @property
    def _state(self) -> Dict:
        return self._payload.get('state', {})

    @property
    def is_waiting(self) -> bool:
        return not self._state or 'waiting' in self._state

    @property
    def is_terminated(self) -> bool:
        return bool(self._state) and 'terminated' in self._state

    @property
    def reason(self) -> Optional[str]:
        """Return the reason of the current state.

        'waiting' reasons:
            'PodInitializing'
            'ContainerCreating'
            'ErrImagePull'
        see
        https://github.com/kubernetes/kubernetes/blob/29232e3edc4202bb5e34c8c107bae4e8250cd883/pkg/kubelet/kubelet_pods.go#L1463-L1468
        https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43

        'terminated' reasons:
            'OOMKilled'
            'Completed'
            'Error'
            'ContainerCannotRun'
        see
        https://github.com/kubernetes/kubernetes/blob/c65f65cf6aea0f73115a2858a9d63fc2c21e5e3b/pkg/kubelet/dockershim/docker_container.go#L306-L409
        """
        for state in self._state.values():
            return state.get('reason')
        return None

    @property
    def message(self) -> Optional[str]:
        for state in self._state.values():
            return state.get('message')
        return None

    @property
    def exit_code(self) -> Optional[int]:
        assert self.is_terminated
        return self._state['terminated']['exitCode']

    @property
    def is_creating(self) -> bool:
        # TODO (A Danshyn 07/20/18): handle PodInitializing
        # TODO (A Danshyn 07/20/18): consider handling other reasons
        # https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43
        return (
            self.is_waiting and
            self.reason in (None, 'ContainerCreating')
        )


class PodStatus:
    def __init__(self, payload):
        self._payload = payload

    @property
    def phase(self):
        return self._payload['phase']

    @property
    def container_status(self) -> ContainerStatus:
        payload = None
        if 'containerStatuses' in self._payload:
            payload = self._payload['containerStatuses'][0]
        return ContainerStatus(payload=payload)

    @property
    def is_container_creating(self) -> bool:
        return self.container_status.is_creating

    @classmethod
    def from_primitive(cls, payload):
        return cls(payload)


class KubeClientAuthType(str, enum.Enum):
    NONE = 'none'
    # TODO: TOKEN = 'token'
    CERTIFICATE = 'certificate'


class KubeClient:
    def __init__(
            self, *, base_url: str, namespace: str,
            cert_authority_path: Optional[str]=None,
            auth_type: KubeClientAuthType=KubeClientAuthType.CERTIFICATE,
            auth_cert_path: Optional[str]=None,
            auth_cert_key_path: Optional[str]=None,
            conn_timeout_s: int=300,
            read_timeout_s: int=100,
            conn_pool_size: int=100) -> None:
        self._base_url = base_url
        self._namespace = namespace

        self._cert_authority_path = cert_authority_path

        self._auth_type = auth_type
        self._auth_cert_path = auth_cert_path
        self._auth_cert_key_path = auth_cert_key_path

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._client: Optional[aiohttp.ClientSession] = None

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == 'https'

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._is_ssl:
            return None
        ssl_context = ssl.create_default_context(
            cafile=self._cert_authority_path)
        if self._auth_type == KubeClientAuthType.CERTIFICATE:
            ssl_context.load_cert_chain(  # noqa
                self._auth_cert_path, self._auth_cert_key_path)
        return ssl_context

    async def init(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context())
        self._client = aiohttp.ClientSession(
            connector=connector,
            conn_timeout=self._conn_timeout_s,
            read_timeout=self._read_timeout_s
        )

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> 'KubeClient':
        await self.init()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    @property
    def _namespace_url(self) -> str:
        return f'{self._base_url}/api/v1/namespaces/{self._namespace}'

    @property
    def _pods_url(self) -> str:
        return f'{self._namespace_url}/pods'

    def _generate_pod_url(self, pod_id: str) -> str:
        return f'{self._pods_url}/{pod_id}'

    @property
    def _v1beta1_namespace_url(self) -> str:
        return (
            f'{self._base_url}/apis/extensions/v1beta1'
            f'/namespaces/{self._namespace}'
        )

    @property
    def _ingresses_url(self) -> str:
        return f'{self._v1beta1_namespace_url}/ingresses'

    def _generate_ingress_url(self, ingress_name: str) -> str:
        return f'{self._ingresses_url}/{ingress_name}'

    @property
    def _services_url(self) -> str:
        return f'{self._namespace_url}/services'

    def _generate_service_url(self, service_name: str) -> str:
        return f'{self._services_url}/{service_name}'

    def _generate_pod_log_url(self, pod_name: str, container_name: str) -> str:
        return (
            f'{self._generate_pod_url(pod_name)}/log'
            f'?container={pod_name}&follow=true'
        )

    async def _request(self, *args, **kwargs):
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            logging.debug('k8s response payload: %s', payload)
            return payload

    async def create_pod(self, descriptor: PodDescriptor) -> PodStatus:
        payload = await self._request(
            method='POST', url=self._pods_url, json=descriptor.to_primitive())
        return PodDescriptor.from_primitive(payload).status

    async def get_pod(self, pod_name: str) -> PodDescriptor:
        url = self._generate_pod_url(pod_name)
        payload = await self._request(method='GET', url=url)
        return PodDescriptor.from_primitive(payload)

    async def get_pod_status(self, pod_id: str) -> PodStatus:
        pod = await self.get_pod(pod_id)
        return pod.status  # type: ignore

    async def delete_pod(self, pod_id: str) -> PodStatus:
        url = self._generate_pod_url(pod_id)
        payload = await self._request(method='DELETE', url=url)
        return PodDescriptor.from_primitive(payload).status

    async def create_ingress(self, name) -> Ingress:
        ingress = Ingress(name=name)  # type: ignore
        payload = await self._request(
            method='POST', url=self._ingresses_url,
            json=ingress.to_primitive())
        return Ingress.from_primitive(payload)

    async def get_ingress(self, name) -> Ingress:
        url = self._generate_ingress_url(name)
        payload = await self._request(method='GET', url=url)
        return Ingress.from_primitive(payload)

    async def delete_ingress(self, name) -> None:
        url = self._generate_ingress_url(name)
        payload = await self._request(method='DELETE', url=url)
        self._check_status_payload(payload)

    def _check_status_payload(self, payload):
        assert payload['kind'] == 'Status'
        if payload['status'] == 'Failure':
            raise StatusException('Failure')

    async def add_ingress_rule(self, name: str, rule: IngressRule) -> Ingress:
        # TODO (A Danshyn 06/13/18): test if does not exist already
        url = self._generate_ingress_url(name)
        headers = {
            'Content-Type': 'application/json-patch+json',
        }
        patches = [{
            'op': 'add',
            'path': '/spec/rules/-',
            'value': rule.to_primitive(),
        }]
        payload = await self._request(
            method='PATCH', url=url, headers=headers, json=patches)
        return Ingress.from_primitive(payload)

    async def remove_ingress_rule(self, name: str, host: str) -> Ingress:
        # TODO (A Danshyn 06/13/18): this one should have a retry in case of
        # a race condition
        ingress = await self.get_ingress(name)
        rule_index = ingress.find_rule_index_by_host(host)
        if rule_index < 0:
            raise StatusException('Not found')
        url = self._generate_ingress_url(name)
        rule = [{
            'op': 'test',
            'path': f'/spec/rules/{rule_index}/host',
            'value': host,
        }, {
            'op': 'remove',
            'path': f'/spec/rules/{rule_index}',
        }]
        headers = {
            'Content-Type': 'application/json-patch+json',
        }
        payload = await self._request(
            method='PATCH', url=url, headers=headers, json=rule)
        return Ingress.from_primitive(payload)

    async def create_service(self, service: Service) -> Service:
        url = self._services_url
        payload = await self._request(
            method='POST', url=url, json=service.to_primitive())
        return Service.from_primitive(payload)

    async def delete_service(self, name: str) -> None:
        url = self._generate_service_url(name)
        payload = await self._request(method='DELETE', url=url)
        self._check_status_payload(payload)

    async def wait_pod_is_running(
            self, pod_name: str,
            timeout_s: float=10. * 60, interval_s: float=1.) -> None:
        """Wait until the pod transitions from the waiting state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(pod_name)
                if not pod_status.container_status.is_waiting:
                    return
                await asyncio.sleep(interval_s)

    @asynccontextmanager
    async def create_pod_container_logs_stream(
            self, pod_name: str, container_name: str,
            conn_timeout_s: float=60 * 5,
            read_timeout_s: float=60 * 30) -> aiohttp.StreamReader:
        url = self._generate_pod_log_url(pod_name, container_name)
        client_timeout = aiohttp.ClientTimeout(
            connect=conn_timeout_s, sock_read=read_timeout_s)
        async with self._client.get(  # type: ignore
                url, timeout=client_timeout) as response:
            await self._check_response_status(response)
            yield response.content

    async def _check_response_status(self, response) -> None:
        if response.status != 200:
            payload = await response.text()
            raise KubeClientException(payload)
