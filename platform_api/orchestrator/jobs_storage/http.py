import json
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Iterable, List, Optional

import aiohttp
from yarl import URL

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobError
from platform_api.orchestrator.jobs_storage import (
    JobFilter,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)


class HttpJobsStorage(JobsStorage):

    _client: Optional[aiohttp.ClientSession] = None

    def __init__(self, url: URL, token: str):
        self._base_url = url
        self._token = token

    async def init(self) -> None:
        if self._client:
            return
        self._client = aiohttp.ClientSession(
            headers={"Authorization": "Bearer " + self._token}
        )

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "HttpJobsStorage":
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _check_for_error(self, resp: aiohttp.ClientResponse) -> None:
        if resp.status == 400:
            error = await resp.json()
            if error["type"] == "JobStorageJobFoundError":
                raise JobStorageJobFoundError(
                    job_name=error["job_name"],
                    job_owner=error["job_owner"],
                    found_job_id=error["found_job_id"],
                )
            if error["type"] == "JobStorageTransactionError":
                raise JobStorageTransactionError(error["message"])
            if error["type"] == "JobError":
                raise JobError(error["message"])
        resp.raise_for_status()

    @asynccontextmanager
    async def try_create_job(self, job: JobRecord) -> AsyncIterator[JobRecord]:
        assert self._client
        url = self._base_url / "try_create_job"
        yield job
        async with self._client.post(url, json=job.to_primitive()) as resp:
            await self._check_for_error(resp)

    async def set_job(self, job: JobRecord) -> None:
        assert self._client
        url = self._base_url / "set_job"
        async with self._client.post(url, json=job.to_primitive()) as resp:
            await self._check_for_error(resp)

    async def get_job(self, job_id: str) -> JobRecord:
        assert self._client
        url = self._base_url / "get_job"
        params = {"job_id": job_id}
        async with self._client.get(url, params=params) as resp:
            await self._check_for_error(resp)
            payload = await resp.json()
            return JobRecord.from_primitive(payload)

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        assert self._client
        url = self._base_url / "try_update_job"
        params = {"job_id": job_id}
        async with self._client.get(url, params=params) as resp:
            await self._check_for_error(resp)
            payload = await resp.json()
            job = JobRecord.from_primitive(payload)
            version = resp.headers["X-JOB-VERSION"]
        yield job
        async with self._client.post(
            url, json=job.to_primitive(), headers={"X-JOB-VERSION": version}
        ) as resp:
            await self._check_for_error(resp)

    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None
    ) -> AsyncIterator[JobRecord]:
        assert self._client
        url = self._base_url / "iter_all_jobs"
        params = {}
        if job_filter:
            params["job_filter"] = json.dumps(job_filter.to_primitive())
        if reverse:
            params["reverse"] = "True"
        if limit is not None:
            params["limit"] = str(limit)
        async with self._client.get(url=url, params=params) as resp:
            resp.raise_for_status()
            async for line in resp.content:
                payload = json.loads(line)
                yield JobRecord.from_primitive(payload)

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        assert self._client
        url = self._base_url / "get_jobs_by_ids"
        params = {
            "job_ids": json.dumps(list(job_ids)),
        }
        if job_filter is not None:
            params["job_filter"] = json.dumps(job_filter.to_primitive())
        async with self._client.get(url=url, params=params) as resp:
            await self._check_for_error(resp)
            payload = await resp.json()
            return [JobRecord.from_primitive(item) for item in payload]

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        assert self._client
        url = self._base_url / "get_jobs_for_deletion"
        params = {"delay_s": str(delay.total_seconds())}
        async with self._client.get(url=url, params=params) as resp:
            await self._check_for_error(resp)
            payload = await resp.json()
            return [JobRecord.from_primitive(item) for item in payload]

    async def get_tags(self, owner: str) -> List[str]:
        assert self._client
        url = self._base_url / "get_tags"
        params = {"owner": owner}
        async with self._client.get(url=url, params=params) as resp:
            await self._check_for_error(resp)
            return await resp.json()
