import logging
from typing import Any, Dict, Optional, Sequence

import aiohttp.web
import trafaret as t
from aiohttp_security import check_permission

from platform_api.config import Config
from platform_api.orchestrator import JobRequest
from platform_api.orchestrator.job_request import Container
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.resource import GPUModel
from platform_api.user import User, authorized_user

from .job_request_builder import ModelRequest
from .jobs_handler import infer_permissions_from_container
from .validators import (
    create_container_request_validator,
    create_job_name_validator,
    create_job_status_validator,
)


logger = logging.getLogger(__name__)


def create_model_request_validator(
    *, allowed_gpu_models: Sequence[GPUModel]
) -> t.Trafaret:
    return t.Dict(
        {
            "container": create_container_request_validator(
                allowed_gpu_models=allowed_gpu_models
            ),
            # TODO (A Danshyn 05/25/18): we may move the storage URI parsing
            # and validation here at some point
            "dataset_storage_uri": t.String,
            "result_storage_uri": t.String,
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("is_preemptible", optional=True, default=False): t.Bool,
        }
    )


def create_model_response_validator() -> t.Trafaret:
    return t.Dict(
        {
            "job_id": t.String,
            "status": create_job_status_validator(),
            "is_preemptible": t.Bool,
            t.Key("http_url", optional=True): t.String,
            t.Key("http_url_named", optional=True): t.String,
            t.Key("ssh_server", optional=True): t.String,
            t.Key("internal_hostname", optional=True): t.String,
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
        }
    )


class ModelsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

        self._model_response_validator = create_model_response_validator()

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.post("", self.handle_post),
                # TODO add here get method for model not for job
            )
        )

    async def _create_model_request_validator(self, user: User) -> t.Trafaret:
        gpu_models = await self._jobs_service.get_available_gpu_models(user)
        return create_model_request_validator(allowed_gpu_models=gpu_models)

    async def _create_job(
        self,
        user: User,
        container: Container,
        name: Optional[str] = None,
        description: Optional[str] = None,
        is_preemptible: bool = False,
    ) -> Dict[str, Any]:
        job_request = JobRequest.create(container, description)
        job, status = await self._jobs_service.create_job(
            job_request, user=user, job_name=name, is_preemptible=is_preemptible
        )
        payload = {
            "job_id": job.id,
            "status": status.value,
            "is_preemptible": job.is_preemptible,
        }
        if container.has_http_server_exposed:
            payload["http_url"] = job.http_url
            if job.http_url_named:
                payload["http_url_named"] = job.http_url_named
        if container.has_ssh_server_exposed:
            payload["ssh_server"] = job.ssh_server
        if job.internal_hostname:
            payload["internal_hostname"] = job.internal_hostname
        if job.name:
            payload["name"] = job.name
        if job.description:
            payload["description"] = job.description
        return payload

    async def handle_post(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        user = await authorized_user(request)

        orig_payload = await request.json()
        model_request_validator = await self._create_model_request_validator(user)
        request_payload = model_request_validator.check(orig_payload)

        cluster_config = await self._jobs_service.get_cluster_config(user)

        container = ModelRequest(
            request_payload,
            storage_config=cluster_config.storage,
            env_prefix=self._config.env_prefix,
        ).to_container()

        permissions = infer_permissions_from_container(
            user, container, cluster_config.registry
        )
        logger.info("Checking whether %r has %r", user, permissions)
        await check_permission(request, permissions[0].action, permissions)

        job_name = request_payload.get("name")
        description = request_payload.get("description")
        is_preemptible = request_payload["is_preemptible"]
        response_payload = await self._create_job(
            user,
            container,
            name=job_name,
            description=description,
            is_preemptible=is_preemptible,
        )
        self._model_response_validator.check(response_payload)

        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )
