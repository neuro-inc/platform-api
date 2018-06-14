import abc
from asyncio import AbstractEventLoop
from dataclasses import dataclass, field
import enum
import logging
from pathlib import PurePath
import ssl
from typing import Dict, List, Optional
from urllib.parse import urlsplit

import aiohttp

from .base import Orchestrator
from .job_request import (
    Container, ContainerResources, ContainerVolume,
    JobRequest, JobStatus, JobError
)


logger = logging.getLogger(__name__)


class KubeClientException(Exception):
    pass


class StatusException(KubeClientException):
    pass


def _raise_status_job_exception(pod: dict, job_id: str):
    if pod['code'] == 409:
        raise JobError(f"job with {job_id} already exist")
    elif pod['code'] == 404:
        raise JobError(f"job with {job_id} not exist")
    elif pod['code'] == 422:
        raise JobError(f"cant create job with id {job_id}")
    else:
        raise JobError('unexpected')


def _status_for_pending_pod(pod_status: dict) -> JobStatus:
    container_statuses = pod_status.get('containerStatuses')
    if container_statuses is None:
        return JobStatus.PENDING
    else:
        container_status = container_statuses[0]
        if container_status['state']['waiting']['reason'] == 'ContainerCreating':
            return JobStatus.PENDING
        else:
            return JobStatus.FAILED


def _status_for_running_pod(pod_status: dict) -> JobStatus:
    container_statuses = pod_status.get('containerStatuses')
    if container_statuses is not None and container_statuses[0]['ready']:
        # TODO (A Danshyn 05/24/18): how come a running pod is succeeded?
        # it seems we need to differenciate between the batch jobs and
        # services.
        return JobStatus.SUCCEEDED
    else:
        return JobStatus.FAILED


def _status_pod_from_dict(pod_status: dict) -> JobStatus:
    phase = pod_status['phase']
    if phase == 'Pending':
        return _status_for_pending_pod(pod_status)
    elif phase == 'Running':
        return _status_for_running_pod(pod_status)
    elif phase == 'Succeeded':
        return JobStatus.SUCCEEDED
    elif phase == 'Failed':
        return JobStatus.FAILED
    else:
        return JobStatus.FAILED


@dataclass(frozen=True)
class Volume(metaclass=abc.ABCMeta):
    name: str
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
class HostVolume(Volume):

    def to_primitive(self):
        return {
            'name': self.name,
            'hostPath': {
                'path': str(self.path),
                'type': 'Directory',
            },
        }


@dataclass(frozen=True)
class NfsVolume(Volume):
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
            cpu=resources.cpu, memory=resources.memory_mb, gpu=resources.gpu)


@dataclass(frozen=True)
class Service:
    name: str
    target_port: int
    port: int = 80

    def to_primitive(self):
        return {
            'metadata': {'name': self.name},
            'spec': {
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
    host: Optional[str] = None
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
            host=payload.get('host'),
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
                    'path': '/',
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
    volume_mounts: List[Volume] = field(default_factory=list)
    volumes: List[Volume] = field(default_factory=list)
    resources: Optional[Resources] = None
    port: Optional[int] = None

    @classmethod
    def from_job_request(
            cls, volume: Volume, job_request: JobRequest) -> 'PodDescriptor':
        container = job_request.container
        volume_mounts = [
            volume.create_mount(container_volume)
            for container_volume in container.volumes]
        volumes = [volume]
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
        }
        if self.args:
            container_payload['args'] = self.args
        if self.resources:
            container_payload['resources'] = self.resources.to_primitive()
        # TODO: ports
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


class PodStatus:
    def __init__(self, payload):
        self._payload = payload

    @property
    def status(self) -> JobStatus:
        return _status_pod_from_dict(self._payload)

    @classmethod
    def from_primitive(cls, payload):
        # TODO (A Danshyn 05/22/18): should be refactored further
        kind = payload['kind']
        if kind == 'Pod':
            return cls(payload['status'])
        elif kind == 'Status':
            _raise_status_job_exception(payload, job_id=None)
        else:
            raise ValueError(f'unknown kind: {kind}')


class VolumeType(str, enum.Enum):
    HOST = 'host'
    NFS = 'nfs'


class KubeClientAuthType(str, enum.Enum):
    NONE = 'none'
    # TODO: TOKEN = 'token'
    CERTIFICATE = 'certificate'


@dataclass(frozen=True)
class KubeConfig:
    storage_mount_path: PurePath

    endpoint_url: str
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None

    namespace: str = 'default'

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    storage_type: VolumeType = VolumeType.HOST
    nfs_volume_server: Optional[str] = None
    nfs_volume_export_path: Optional[PurePath] = None

    # TODO: these two should not have default values assigned
    jobs_ingress_name: str = 'platformjobsingress'
    jobs_ingress_domain_name: str = 'jobs.platform.neuromation.io'


class KubeClient:
    def __init__(
            self, *, base_url: str, namespace: str,
            cert_authority_path: Optional[str]=None,
            auth_type: KubeClientAuthType=KubeClientAuthType.CERTIFICATE,
            auth_cert_path: Optional[str]=None,
            auth_cert_key_path: Optional[str]=None,
            conn_timeout_s: int=KubeConfig.client_conn_timeout_s,
            read_timeout_s: int=KubeConfig.client_read_timeout_s,
            conn_pool_size: int=KubeConfig.client_conn_pool_size) -> None:
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
            ssl_context.load_cert_chain(
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

    async def _request(self, *args, **kwargs):
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            logging.debug('k8s response payload: %s', payload)
            return payload

    async def create_pod(self, descriptor: PodDescriptor) -> PodStatus:
        payload = await self._request(
            method='POST', url=self._pods_url, json=descriptor.to_primitive())
        return PodStatus.from_primitive(payload)

    async def get_pod_status(self, pod_id: str) -> PodStatus:
        url = self._generate_pod_url(pod_id)
        payload = await self._request(method='GET', url=url)
        return PodStatus.from_primitive(payload)

    async def delete_pod(self, pod_id: str) -> PodStatus:
        url = self._generate_pod_url(pod_id)
        payload = await self._request(method='DELETE', url=url)
        return PodStatus.from_primitive(payload)

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


class KubeOrchestrator(Orchestrator):
    def __init__(
            self, *, config: KubeConfig,
            loop: Optional[AbstractEventLoop]=None) -> None:
        self._loop = loop

        self._config = config

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing

        self._client = KubeClient(
            base_url=config.endpoint_url,

            cert_authority_path=config.cert_authority_path,

            auth_type=config.auth_type,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,

            namespace=config.namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size
        )

        self._storage_volume = self._create_storage_volume()

    def _create_storage_volume(self) -> Volume:
        name = 'storage'
        if self._config.storage_type == VolumeType.NFS:
            return NfsVolume(  # type: ignore
                name=name,
                server=self._config.nfs_volume_server,
                path=self._config.nfs_volume_export_path,
            )
        return HostVolume(  # type: ignore
            name=name, path=self._config.storage_mount_path)

    async def __aenter__(self) -> 'KubeOrchestrator':
        await self._client.init()
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.close()

    async def start_job(self, job_request: JobRequest) -> JobStatus:
        descriptor = PodDescriptor.from_job_request(
            self._storage_volume, job_request)
        status = await self._client.create_pod(descriptor)
        # TODO: have a properly named property
        if descriptor.port:
            service = await self._client.create_service(
                Service.create_for_pod(descriptor))
            await self._client.add_ingress_rule(
                name=self._config.jobs_ingress_name,
                rule=IngressRule.from_service(
                    domain_name=self._config.jobs_ingress_domain_name,
                    service=service))
        return status.status

    async def status_job(self, job_id: str) -> JobStatus:
        pod_id = job_id
        status = await self._client.get_pod_status(pod_id)
        return status.status

    def _get_ingress_rule_host_for_pod(self, pod_id) -> str:
        ingress_rule = IngressRule.from_service(
            domain_name=self._config.jobs_ingress_domain_name,
            service=Service(name=pod_id, target_port=0)  # type: ignore
        )
        return ingress_rule.host

    async def _delete_service(self, pod_id: str) -> None:
        host = self._get_ingress_rule_host_for_pod(pod_id)
        try:
            await self._client.remove_ingress_rule(
                name=self._config.jobs_ingress_name, host=host)
        except Exception:
            logger.exception(f'Failed to remove ingress rule {host}')
        try:
            await self._client.delete_service(name=pod_id)
        except Exception:
            logger.exception(f'Failed to remove service {pod_id}')

    async def delete_job(self, job_id: str) -> JobStatus:
        pod_id = job_id
        await self._delete_service(pod_id)
        status = await self._client.delete_pod(pod_id)
        return status.status
