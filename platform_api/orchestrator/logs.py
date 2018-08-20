from io import StringIO
from typing import Optional

import aiohttp

from .base import LogReader
from .kube_client import KubeClient


class PodContainerLogReader(LogReader):

    def __init__(
            self, client: KubeClient, pod_name: str, container_name: str,
            client_conn_timeout_s: Optional[float]=None,
            client_read_timeout_s: Optional[float]=None,) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name
        self._client_conn_timeout_s = client_conn_timeout_s
        self._client_read_timeout_s = client_read_timeout_s

        self._stream_cm = None
        self._stream: Optional[aiohttp.StreamReader] = None

        self._buffer = StringIO()

    async def __aenter__(self) -> LogReader:
        await self._client.wait_pod_is_running(self._pod_name)
        kwargs = {}
        if self._client_conn_timeout_s is not None:
            kwargs['conn_timeout_s'] = self._client_conn_timeout_s
        if self._client_read_timeout_s is not None:
            kwargs['read_timeout_s'] = self._client_read_timeout_s
        self._stream_cm = self._client.create_pod_container_logs_stream(
            pod_name=self._pod_name, container_name=self._container_name,
            **kwargs)
        self._stream = await self._stream_cm.__aenter__()  # type: ignore
        return self

    async def __aexit__(self, *args) -> None:
        stream_cm = self._stream_cm
        self._stream = None
        self._stream_cm = None
        await stream_cm.__aexit__(*args)  # type: ignore
        self._buffer.close()

    async def read(self, size: int=-1) -> bytes:
        assert self._stream
        line = await self.readline(size)
        return line.encode()

    def _readline_from_buffer(self, size: int = -1) -> str:
        line = self._buffer.readline(size)
        if not line and self._buffer.tell():
            self._buffer.seek(0)
            self._buffer.truncate()
        return line

    async def readline(self, size: int=-1) -> str:
        line = self._readline_from_buffer(size)
        if line:
            return line

        assert self._stream
        line = await self._stream.readline(size)
        # https://github.com/neuromation/platform-api/issues/131
        # k8s API (and the underlying docker API) sometimes returns an rpc
        # error as the last log line. it says that the corresponding container
        # does not exist. we should try to not expose such internals, but only
        # if it is the last line indeed.
        if line.startswith('rpc error: code ='):
            next_line = await self._stream.readline(size)
            if next_line:
                self._buffer.write(next_line)
            else:
                line = ''
        return line
