import io
import logging
from typing import Optional

import aiohttp

from .base import LogReader
from .kube_client import KubeClient


logger = logging.getLogger(__name__)


class FilteredStreamWrapper:
    def __init__(self, stream: aiohttp.StreamReader) -> None:
        self._stream = stream
        self._buffer = io.BytesIO()

    def close(self) -> None:
        self._buffer.close()

    async def read(self, size: int = -1) -> bytes:
        chunk = self._read_from_buffer(size)
        if chunk:
            return chunk

        chunk = await self._readline()

        self._append_to_buffer(chunk)
        return self._read_from_buffer(size)

    def _read_from_buffer(self, size: int = -1) -> bytes:
        chunk = self._buffer.read(size)
        if not chunk and self._buffer.tell():
            self._buffer.seek(0)
            self._buffer.truncate()
        return chunk

    def _append_to_buffer(self, chunk: bytes) -> None:
        pos = self._buffer.tell()
        self._buffer.seek(0, io.SEEK_END)
        self._buffer.write(chunk)
        self._buffer.seek(pos)

    async def _readline(self) -> bytes:
        line = await self._stream.readline()
        # https://github.com/neuromation/platform-api/issues/131
        # k8s API (and the underlying docker API) sometimes returns an rpc
        # error as the last log line. it says that the corresponding container
        # does not exist. we should try to not expose such internals, but only
        # if it is the last line indeed.
        if line.startswith(b"rpc error: code ="):
            next_line = await self._stream.readline()
            if next_line:
                logging.warning("An rpc error line was not at the end of the log")
                self._stream.unread_data(next_line)
            else:
                logging.info("Skipping an rpc error line at the end of the log")
                line = next_line
        return line


class PodContainerLogReader(LogReader):
    def __init__(
        self,
        client: KubeClient,
        pod_name: str,
        container_name: str,
        client_conn_timeout_s: Optional[float] = None,
        client_read_timeout_s: Optional[float] = None,
    ) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name
        self._client_conn_timeout_s = client_conn_timeout_s
        self._client_read_timeout_s = client_read_timeout_s

        self._stream_cm = None
        self._stream: Optional[FilteredStreamWrapper] = None

    async def __aenter__(self) -> LogReader:
        await self._client.wait_pod_is_running(self._pod_name)
        kwargs = {}
        if self._client_conn_timeout_s is not None:
            kwargs["conn_timeout_s"] = self._client_conn_timeout_s
        if self._client_read_timeout_s is not None:
            kwargs["read_timeout_s"] = self._client_read_timeout_s
        self._stream_cm = self._client.create_pod_container_logs_stream(
            pod_name=self._pod_name, container_name=self._container_name, **kwargs
        )
        assert self._stream_cm
        stream = await self._stream_cm.__aenter__()
        self._stream = FilteredStreamWrapper(stream)
        return self

    async def __aexit__(self, *args) -> None:
        assert self._stream
        assert self._stream_cm
        stream_cm = self._stream_cm
        self._stream.close()
        self._stream = None
        self._stream_cm = None
        await stream_cm.__aexit__(*args)

    async def read(self, size: int = -1) -> bytes:
        assert self._stream
        return await self._stream.read(size)
