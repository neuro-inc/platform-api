import abc
from typing import Optional

import aiohttp

from .kube_orchestrator import KubeClient


class LogReader:
    async def __aenter__(self) -> 'LogReader':
        return self

    async def __aexit__(self, *args) -> None:
        pass

    @abc.abstractmethod
    async def read(self, size: int=-1) -> bytes:
        pass


class PodContainerLogReader(LogReader):

    def __init__(
            self, client: KubeClient, pod_name: str,
            container_name: str) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name

        self._stream_cm = None
        self._stream: Optional[aiohttp.StreamReader] = None

    async def __aenter__(self) -> LogReader:
        await self._client.wait_pod_is_running(self._pod_name)
        self._stream_cm = self._client.create_pod_container_logs_stream(
            pod_name=self._pod_name, container_name=self._container_name)
        self._stream = await self._stream_cm.__aenter__()
        return self

    async def __aexit__(self, *args) -> None:
        stream_cm = self._stream_cm
        self._stream = None
        self._stream_cm = None
        await stream_cm.__aexit__(*args)

    async def read(self, size: int=-1) -> bytes:
        assert self._stream
        return await self._stream.read(size)
