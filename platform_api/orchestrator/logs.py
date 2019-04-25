import io
import logging
import warnings
from typing import Any, Dict, Optional

import aiohttp
from aioelasticsearch import Elasticsearch
from aioelasticsearch.helpers import Scan

from .base import LogReader
from .kube_client import KubeClient


logger = logging.getLogger(__name__)


class LogBuffer:
    def __init__(self) -> None:
        self._buffer = io.BytesIO()

    def close(self) -> None:
        self._buffer.close()

    def read(self, size: int = -1) -> bytes:
        chunk = self._buffer.read(size)
        if not chunk and self._buffer.tell():
            self._buffer.seek(0)
            self._buffer.truncate()
        return chunk

    def write(self, chunk: bytes) -> None:
        pos = self._buffer.tell()
        self._buffer.seek(0, io.SEEK_END)
        self._buffer.write(chunk)
        self._buffer.seek(pos)


class FilteredStreamWrapper:
    def __init__(self, stream: aiohttp.StreamReader) -> None:
        self._stream = stream
        self._buffer = LogBuffer()

    def close(self) -> None:
        self._buffer.close()

    async def read(self, size: int = -1) -> bytes:
        chunk = self._buffer.read(size)
        if chunk:
            return chunk

        chunk = await self._readline()

        self._buffer.write(chunk)
        return self._buffer.read(size)

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
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning)
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

    async def __aexit__(self, *args: Any) -> None:
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


class ElasticsearchLogReader(LogReader):
    def __init__(
        self,
        es_client: Elasticsearch,
        namespace_name: str,
        pod_name: str,
        container_name: str,
    ) -> None:
        self._es_client = es_client
        self._index = "logstash-*"
        self._doc_type = "fluentd"

        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name

        self._scan: Optional[Scan] = None

        self._buffer = LogBuffer()

    def _combine_search_query(self) -> Dict[str, Any]:
        terms = [
            {"term": {"kubernetes.namespace_name.keyword": self._namespace_name}},
            {"term": {"kubernetes.pod_name.keyword": self._pod_name}},
            {"term": {"kubernetes.container_name.keyword": self._container_name}},
        ]
        return {"query": {"bool": {"must": terms}}, "sort": [{"@timestamp": "asc"}]}

    async def __aenter__(self) -> LogReader:
        query = self._combine_search_query()
        self._scan = Scan(
            self._es_client,
            index=self._index,
            doc_type=self._doc_type,
            # scroll="1m" means that the requested search context will be
            # preserved in the ES cluster for at most 1 minutes. in other
            # words, our client code has up to 1 minute to process previous
            # results and fetch next ones.
            scroll="1m",
            query=query,
            preserve_order=True,
            size=100,
        )
        await self._scan.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._buffer.close()
        assert self._scan
        scan = self._scan
        self._scan = None
        await scan.__aexit__(*args)

    async def read(self, size: int = -1) -> bytes:
        chunk = self._buffer.read(size)
        if chunk:
            return chunk

        chunk = await self._readline()

        self._buffer.write(chunk)
        return self._buffer.read(size)

    async def _readline(self) -> bytes:
        assert self._scan
        try:
            doc = await self._scan.__anext__()
            return doc["_source"]["log"].encode()
        except StopAsyncIteration:
            return b""
