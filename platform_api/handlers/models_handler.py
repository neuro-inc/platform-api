import aiohttp.web
import trafaret as t
from typing import Dict, List, Optional

from platform_api.config import Config, StorageConfig
from platform_api.orchestrator import Job, JobRequest, Orchestrator, StatusService, Status
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume)


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

    @property
    def _container_image(self) -> str:
        return self._payload['container']['image']

    @property
    def _container_command(self) -> Optional[str]:
        return self._payload['container'].get('command')

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

    def to_container(self) -> Container:
        return Container(  # type: ignore
            image=self._container_image,
            command=self._container_command,
            env=self._env,
            volumes=self._volumes,
            resources=ContainerResources(cpu=1, memory_mb=128),  # type: ignore
        )


class ModelsHandler:
    def __init__(
            self, *, config: Config, orchestrator: Orchestrator,
            status_service: StatusService
            ) -> None:
        self._orchestrator = orchestrator
        self._config = config
        self._storage_config = config.storage
        self._status_service = status_service

        self._model_request_validator = self._create_model_request_validator()

    def _create_model_request_validator(self) -> t.Trafaret:
        return t.Dict({
            'container': t.Dict({
                'image': t.String,
                t.Key('command', optional=True): t.String,
                t.Key('env', optional=True): t.Mapping(
                    t.String, t.String(allow_blank=True))
            }),
            # TODO (A Danshyn 05/25/18): resources
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

    async def _create_job(self, model_request: ModelRequest) -> (Job, Status):
        job_request = JobRequest.create(model_request.to_container())
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        _ = await job.start()
        status = await self._status_service.create(job=job)
        return job, status

    async def handle_post(self, request):
        data = await request.json()
        self._model_request_validator.check(data)
        model_request = ModelRequest(
            data, storage_config=self._storage_config,
            env_prefix=self._config.env_prefix)
        job, status = await self._create_job(model_request)
        status_value = await status.value()
        return aiohttp.web.json_response(
            data={'status': status_value, 'job_id': job.id, 'status_id': status.id},
            status=aiohttp.web.HTTPAccepted.status_code)

