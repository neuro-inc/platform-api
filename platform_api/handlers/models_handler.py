from typing import Dict, List, Optional

import aiohttp.web
import trafaret as t

from platform_api.config import Config, StorageConfig
from platform_api.orchestrator import (
    JobRequest, JobsService,)
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume, JobStatus,)


class ModelRequest:
    def __init__(
            self, payload, *, storage_config: StorageConfig,
            env_prefix: str = '') -> None:
        self._payload = payload

        self._env_prefix = env_prefix
        self._storage_config = storage_config

        self._dataset_env_var_name = self._create_env_var_name('DATASET_PATH')
        self._result_env_var_name = self._create_env_var_name('RESULT_PATH')

        self._dataset_volume = self._create_dataset_volume()
        self._result_volume = self._create_result_volume()
        self._volumes = self._create_volumes()
        self._env = self._create_env()
        self._resources = self._create_resources()

    @property
    def _container_image(self) -> str:
        return self._payload['container']['image']

    @property
    def _container_command(self) -> Optional[str]:
        return self._payload['container'].get('command')

    @property
    def _container_http(self) -> Dict:
        return self._payload['container'].get('http', {})

    @property
    def _container_port(self) -> Optional[int]:
        return self._container_http.get('port')

    @property
    def _container_health_check_path(self) -> str:
        return self._container_http.get(
            'health_check_path', Container.health_check_path)

    def _create_dataset_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['dataset_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=True,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_result_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['result_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=False,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_volumes(self) -> List[ContainerVolume]:
        return [self._dataset_volume, self._result_volume]

    def _create_env_var_name(self, name):
        if self._env_prefix:
            return f'{self._env_prefix}_{name}'
        return name

    def _create_env(self) -> Dict[str, str]:
        env = self._payload['container'].get('env', {})
        env[self._dataset_env_var_name] = str(self._dataset_volume.dst_path)
        env[self._result_env_var_name] = str(self._result_volume.dst_path)
        return env

    def _create_resources(self) -> ContainerResources:
        return ContainerResources(  # type: ignore
            cpu=self._payload['container']['resources']['cpu'],
            memory_mb=self._payload['container']['resources']['memory_mb'],
            gpu=self._payload['container']['resources'].get('gpu'),
        )

    def to_container(self) -> Container:
        return Container(  # type: ignore
            image=self._container_image,
            command=self._container_command,
            env=self._env,
            volumes=self._volumes,
            resources=self._resources,
            port=self._container_port,
            health_check_path=self._container_health_check_path,
        )


def create_model_response_validator() -> t.Trafaret:
    return t.Dict({
        'job_id': t.String,
        'status': t.Enum(*JobStatus.values()),
        t.Key('http_url', optional=True): t.String,
    })


class ModelsHandler:
    def __init__(
            self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._storage_config = config.storage

        self._model_request_validator = self._create_model_request_validator()
        self._model_response_validator = create_model_response_validator()

    @property
    def _jobs_service(self) -> JobsService:
        return self._app['jobs_service']

    def _create_model_request_validator(self) -> t.Trafaret:
        return t.Dict({
            'container': t.Dict({
                'image': t.String,
                t.Key('command', optional=True): t.String,
                t.Key('env', optional=True): t.Mapping(
                    t.String, t.String(allow_blank=True)),
                'resources': t.Dict({
                    'cpu': t.Float(gte=0.1),
                    'memory_mb': t.Int(gte=16),
                    t.Key('gpu', optional=True): t.Int(gte=1),
                }),
                t.Key('http', optional=True): t.Dict({
                    'port': t.Int(gte=0, lte=65535),
                    t.Key('health_check_path', optional=True): t.String,
                }),
            }),
            # TODO (A Danshyn 05/25/18): we may move the storage URI parsing
            # and validation here at some point
            'dataset_storage_uri': t.String,
            'result_storage_uri': t.String,
        })

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('', self.handle_post),
            # TODO add here get method for model not for job
        ))

    async def _create_job(self, model_request: ModelRequest) -> Dict:
        container = model_request.to_container()
        job_request = JobRequest.create(container)
        job, status = await self._jobs_service.create_job(job_request)
        payload = {
            'job_id': job.id,
            'status': status.value,
        }
        if container.has_http_server_exposed:
            payload['http_url'] = job.http_url
        return payload

    async def handle_post(self, request):
        request_payload = await request.json()
        self._model_request_validator.check(request_payload)

        model_request = ModelRequest(
            request_payload, storage_config=self._storage_config,
            env_prefix=self._config.env_prefix)

        response_payload = await self._create_job(model_request)
        self._model_response_validator.check(response_payload)

        return aiohttp.web.json_response(
            data=response_payload,
            status=aiohttp.web.HTTPAccepted.status_code)
