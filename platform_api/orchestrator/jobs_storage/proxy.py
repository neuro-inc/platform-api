import asyncio
import dataclasses
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
)

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.jobs_storage import JobFilter

from .base import JobsStorage


logger = logging.getLogger(__name__)


class ProxyJobStorage(JobsStorage):
    """Proxy JobStorage for passing calls to real storages

    Always has single primary JobStorage and, in addition to ie, can have
    any number for secondary storages. Calls are passed to all storages,
    but result only returned from primary one.

    Main goal of this class to allow smooth transition from one storage
    engine to another while new storage is still in development.
    """

    _primary_storage: JobsStorage
    _secondary_storages: List[JobsStorage]

    def __init__(
        self, primary_storage: JobsStorage, secondary_storages: Iterable[JobsStorage]
    ):
        self._primary_storage = primary_storage
        self._secondary_storages = list(secondary_storages)

    async def _run_secondary(self, coro: Coroutine[Any, Any, Any], method: str) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Secondary jobs storage failed to run {method}")

    async def _pass_through_call(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Any:
        for secondary in self._secondary_storages:
            coro = getattr(secondary, method_name)(*args, **kwargs)
            asyncio.create_task(self._run_secondary(coro, method_name))
        return await getattr(self._primary_storage, method_name)(*args, **kwargs)

    def _pass_through_aiter(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[Any]:
        async def iter_secondary(secondary_storage: JobsStorage) -> None:
            aiter = getattr(secondary_storage, method_name)(*args, **kwargs)
            async for _ in aiter:
                pass

        for secondary in self._secondary_storages:
            asyncio.create_task(
                self._run_secondary(iter_secondary(secondary), method_name)
            )
        return getattr(self._primary_storage, method_name)(*args, **kwargs)

    @asynccontextmanager
    async def _pass_through_job_context_manager(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[JobRecord]:
        primary_manager = getattr(self._primary_storage, method_name)(*args, **kwargs)
        async with primary_manager as primary_job, AsyncExitStack() as stack:
            secondary_managers = []
            for secondary in self._secondary_storages:
                try:
                    manager = getattr(secondary, method_name)(*args, **kwargs)
                    secondary_job = await manager.__aenter__()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Secondary jobs storage failed to enter context "
                        f"manager {method_name}"
                    )
                else:
                    stack.push_async_exit(secondary_job)
                    secondary_managers.append((manager, secondary_job))
            yield primary_job
            for manager, secondary_job in secondary_managers:
                # Sync up secondary job with primary
                for field in dataclasses.fields(secondary_job):
                    setattr(secondary_job, field.name, getattr(primary_job, field.name))
                try:
                    await stack.aclose()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Secondary jobs storage failed to exit context"
                        f" manager {method_name}"
                    )

    async def set_job(self, job: JobRecord) -> None:
        return await self._pass_through_call("set_job", job)

    async def get_job(self, job_id: str) -> JobRecord:
        return await self._pass_through_call("get_job", job_id)

    def try_create_job(self, job: JobRecord) -> AsyncContextManager[JobRecord]:
        return self._pass_through_job_context_manager("try_create_job", job)

    def try_update_job(self, job_id: str) -> AsyncContextManager[JobRecord]:
        return self._pass_through_job_context_manager("try_update_job", job_id)

    def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        return self._pass_through_aiter(
            "iter_all_jobs", job_filter, reverse=reverse, limit=limit
        )

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        return await self._pass_through_call("get_jobs_by_ids", job_ids, job_filter)

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        return await self._pass_through_call("get_jobs_for_deletion", delay=delay)

    async def get_tags(self, owner: str) -> List[str]:
        return await self._pass_through_call("get_tags", owner)

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        return await self._pass_through_call(
            "get_aggregated_run_time_by_clusters", job_filter
        )
