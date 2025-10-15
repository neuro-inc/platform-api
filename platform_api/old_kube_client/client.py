import asyncio
import logging
import ssl
from contextlib import suppress
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import aiohttp

from platform_api.old_kube_client.config import KubeClientAuthType
from platform_api.old_kube_client.errors import (
    KubeClientException,
    KubeClientUnauthorized,
    ResourceBadRequest,
    ResourceExists,
    ResourceGone,
    ResourceInvalid,
    ResourceNotFound,
)

logger = logging.getLogger(__name__)


class KubeClient:
    def __init__(
        self,
        *,
        base_url: str,
        namespace: str,
        cert_authority_path: str | None = None,
        cert_authority_data_pem: str | None = None,
        auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE,
        auth_cert_path: str | None = None,
        auth_cert_key_path: str | None = None,
        token: str | None = None,
        token_path: str | None = None,
        token_update_interval_s: int = 300,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        watch_timeout_s: int = 1800,
        conn_pool_size: int = 100,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ) -> None:
        self._base_url = base_url
        self._namespace = namespace

        self._cert_authority_data_pem = cert_authority_data_pem
        self._cert_authority_path = cert_authority_path

        if auth_type == KubeClientAuthType.TOKEN:
            assert token or token_path, "token or token path must be provided"
        elif auth_type == KubeClientAuthType.CERTIFICATE:
            assert auth_cert_path and auth_cert_key_path, "certs must be provided"  # noqa

        self._auth_type = auth_type
        self._auth_cert_path = auth_cert_path
        self._auth_cert_key_path = auth_cert_key_path
        self._token = token
        self._token_path = token_path
        self._token_update_interval_s = token_update_interval_s

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._watch_timeout_s = watch_timeout_s
        self._conn_pool_size = conn_pool_size
        self._trace_configs = trace_configs

        self._client: aiohttp.ClientSession | None = None
        self._token_updater_task: asyncio.Task[None] | None = None

    def __str__(self) -> str:
        return self.__class__.__name__

    __repr__ = __str__

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def init(self) -> None:
        logger.info("%s: initializing", self)
        if self._token_path:
            self._refresh_token_from_file()
            self._token_updater_task = asyncio.create_task(self._start_token_updater())
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context()
        )
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            trace_configs=self._trace_configs,
        )

    async def close(self) -> None:
        logger.info("%s: closing", self)
        if self._client:
            await self._client.close()
            self._client = None
        if self._token_updater_task:
            self._token_updater_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._token_updater_task
            self._token_updater_task = None
        logger.info("%s: closed", self)

    @property
    def api_v1_url(self) -> str:
        return f"{self._base_url}/api/v1"

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def namespaces_url(self) -> str:
        return f"{self.api_v1_url}/namespaces"

    def generate_namespace_url(self, namespace_name: str | None = None) -> str:
        namespace_name = namespace_name or self._namespace
        return f"{self.namespaces_url}/{namespace_name}"

    def generate_network_policy_url(self, namespace: str) -> str:
        return (
            f"{self._base_url}"
            f"/apis/networking.k8s.io/v1/namespaces"
            f"/{namespace}/networkpolicies"
        )

    def generate_endpoint_slice_url(self, namespace: str, name: str) -> str:
        return (
            f"{self._base_url}"
            f"/apis/discovery.k8s.io/v1/namespaces"
            f"/{namespace}/endpointslices/{name}"
        )

    async def request(
        self,
        *args: Any,
        raise_for_status: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        assert self._client, "client is not initialized"
        headers = kwargs.pop("headers", {}) or {}
        headers.update(self._auth_headers)  # populate auth (if exists)

        async with self._client.request(*args, headers=headers, **kwargs) as response:
            payload = await response.json()
            logger.debug("%s: k8s response payload: %s", self, payload)
            if raise_for_status:
                self._raise_for_status(payload)
            return cast(dict[str, Any], payload)

    async def get(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self.request("GET", *args, **kwargs)

    async def post(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self.request("POST", *args, **kwargs)

    async def patch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self.request("PATCH", *args, **kwargs)

    async def put(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self.request("PUT", *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self.request("DELETE", *args, **kwargs)

    @property
    def _auth_headers(self) -> dict[str, Any]:
        if self._auth_type != KubeClientAuthType.TOKEN or not self._token:
            return {}
        return {"Authorization": f"Bearer {self._token}"}

    @staticmethod
    def _raise_for_status(payload: dict[str, Any]) -> None:
        kind = payload.get("kind")
        if kind == "Status":
            if payload.get("status") == "Success":
                return
            code = payload.get("code")
            if code == 400:
                raise ResourceBadRequest(payload)
            if code == 401:
                raise KubeClientUnauthorized(payload)
            if code == 404:
                raise ResourceNotFound(payload)
            if code == 409:
                raise ResourceExists(payload)
            if code == 410:
                raise ResourceGone(payload)
            if code == 422:
                raise ResourceInvalid(payload["message"])
            raise KubeClientException(payload["message"])

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == "https"

    def _create_ssl_context(self) -> ssl.SSLContext | bool:
        if not self._is_ssl:
            return False
        ssl_context = ssl.create_default_context(
            cafile=self._cert_authority_path, cadata=self._cert_authority_data_pem
        )
        if self._auth_type == KubeClientAuthType.CERTIFICATE:
            ssl_context.load_cert_chain(
                self._auth_cert_path,  # type: ignore
                self._auth_cert_key_path,
            )
        return ssl_context

    async def _start_token_updater(self) -> None:
        """
        A task which periodically reads from the `token_path` and refreshes the token
        """
        if not self._token_path:
            logger.info("%s: token path does not exist. updater won't be started", self)
            return

        logger.info("%s: starting token updater task", self)

        while True:
            try:
                self._refresh_token_from_file()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("%s: failed to update kube token: %s", self, exc)
            await asyncio.sleep(self._token_update_interval_s)

    def _refresh_token_from_file(self) -> None:
        """Reads token from the file and updates a token value"""
        if not self._token_path:
            return
        token = Path(self._token_path).read_text().strip()
        if token == self._token:
            return
        self._token = token
        logger.info("%s: kube token was refreshed", self)
