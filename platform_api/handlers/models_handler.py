import aiohttp.web
import trafaret as t
from typing import List, Optional

from platform_api.config import StorageConfig
from platform_api.orchestrator import Job, JobRequest, Orchestrator, StatusService
from platform_api.orchestrator.job_request import Container, ContainerVolume


class ModelRequest:
    def __init__(self, payload, *, storage_config: StorageConfig) -> None:
        self._payload = payload

        self._storage_config = storage_config

    @property
    def _container_image(self) -> str:
        return self._payload['container']['image']

    @property
    def _container_command(self) -> Optional[str]:
        return self._payload['container'].get('command')

    @property
    def _dataset_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['dataset_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=True,
            scheme=self._storage_config.uri_scheme,
        )

    @property
    def _result_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['result_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=False,
            scheme=self._storage_config.uri_scheme,
        )

    @property
    def _volumes(self) -> List[ContainerVolume]:
        # TODO (A Danshyn 05/25/18): address the issue of duplicate dst_paths
        return [self._dataset_volume, self._result_volume]

    def to_container(self) -> Container:
        return Container(  # type: ignore
            image=self._container_image,
            command=self._container_command,
            volumes=self._volumes,
        )


class ModelsHandler:
    def __init__(
            self, *, storage_config: StorageConfig, orchestrator: Orchestrator,
            status_service: StatusService
            ) -> None:
        self._orchestrator = orchestrator
        self._storage_config = storage_config
        self._status_service = status_service

        self._model_request_validator = self._create_model_request_validator()

    def _create_model_request_validator(self) -> t.Trafaret:
        return t.Dict({
            'container': t.Dict({
                'image': t.String,
                t.Key('command', optional=True): t.String
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
            aiohttp.web.get('/{job_id}', self.handle_get),
        ))

    async def _create_job(self, model_request: ModelRequest):
        job_request = JobRequest.create(model_request.to_container())
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        start_status = await job.start()
        import uuid
        status_id = str(uuid.uuid4())
        await self._status_service.set(status_id=status_id)

        return start_status, job.id, status_id

    async def handle_post(self, request):
        data = await request.json()
        self._model_request_validator.check(data)
        model_request = ModelRequest(
            data, storage_config=self._storage_config)
        status, job_id, status_id = await self._create_job(model_request)
        return aiohttp.web.json_response(
            data={'status': status, 'job_id': job_id, 'status_id': status_id},
            status=aiohttp.web.HTTPAccepted.status_code)

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        status = await self._orchestrator.status_job(job_id)
        return aiohttp.web.json_response(
            data={'status': status}, status=aiohttp.web.HTTPOk.status_code)
