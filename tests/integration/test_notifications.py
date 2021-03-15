from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Set

import aiohttp.web
import pytest

from .api import ApiConfig, JobsClient
from .auth import _User
from .notifications import NotificationsServer


class TestJobTransition:
    @pytest.fixture
    async def run_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client_factory: Callable[[_User], JobsClient],
    ) -> AsyncIterator[Callable[[_User, Dict[str, Any], bool, bool], Awaitable[str]]]:
        cleanup_pairs = []

        async def _impl(
            user: _User,
            job_request: Dict[str, Any],
            wait_for_start: bool = True,
            do_kill: bool = False,
        ) -> str:
            url = api.jobs_base_url
            headers = user.headers
            jobs_client = jobs_client_factory(user)
            async with client.post(url, headers=headers, json=job_request) as resp:
                assert resp.status == aiohttp.web.HTTPAccepted.status_code, str(
                    job_request
                )
                data = await resp.json()
                job_id = data["id"]
                if wait_for_start:
                    await jobs_client.long_polling_by_job_id(job_id, "running")
                    if do_kill:
                        await jobs_client.delete_job(job_id)
                        await jobs_client.long_polling_by_job_id(job_id, "cancelled")
                else:
                    cleanup_pairs.append((jobs_client, job_id))
            return job_id

        yield _impl

        if not api.runner.closed:
            for jobs_client, job_id in cleanup_pairs:
                await jobs_client.delete_job(job_id=job_id, assert_success=False)

    @pytest.mark.asyncio
    async def test_not_sent_job_creating_failed(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[str], Dict[str, Any]],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
    ) -> None:
        user = await regular_user_factory()
        url = api.jobs_base_url
        job_request = job_request_factory("not_existing_cluster")
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()

        for (slug, payload) in mock_notifications_server.requests:
            if slug == "job-transition":
                raise AssertionError("Unexpected JobTransition sent")

    @pytest.mark.asyncio
    async def test_succeeded_job_workflow(
        self,
        api: ApiConfig,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
        run_job: Callable[..., Awaitable[str]],
    ) -> None:
        user = await regular_user_factory()
        jobs_client = jobs_client_factory(user)
        job_request = job_request_factory()
        job_request["container"]["command"] = "sleep 15m"

        job_id = await run_job(user, job_request, do_kill=False)
        await jobs_client.delete_job(job_id)
        await api.runner.close()

        states: Set[str] = set()
        for (slug, payload) in mock_notifications_server.requests:
            if slug != "job-transition":
                raise AssertionError(f"Unexpected Notification: {slug} : {payload}")

            if payload["status"] != "pending":
                assert payload["status"] not in states
            states.add(payload["status"])

            if payload["status"] == "pending":
                assert (
                    payload.get("prev_status") is None
                    or payload["prev_status"] == "pending"
                )
            elif payload["status"] == "running":
                assert payload["prev_status"] == "pending"
            elif payload["status"] == "cancelled":
                assert payload["prev_status"] == "running"
            else:
                raise AssertionError(f"Unexpected JobTransition payload: {payload}")
        assert states == {"pending", "running", "cancelled"}

    @pytest.mark.asyncio
    async def test_failed_job_workflow(
        self,
        api: ApiConfig,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
        run_job: Callable[..., Awaitable[str]],
    ) -> None:
        user = await regular_user_factory()
        jobs_client = jobs_client_factory(user)
        job_request = job_request_factory()
        job_request["container"]["command"] = "failed-command"

        job_id = await run_job(user, job_request, wait_for_start=False)
        await jobs_client.long_polling_by_job_id(job_id, "failed")
        await api.runner.close()

        states: Set[str] = set()
        for (slug, payload) in mock_notifications_server.requests:
            if slug != "job-transition":
                raise AssertionError(f"Unexpected Notification: {slug} : {payload}")

            if payload["status"] != "pending":
                assert payload["status"] not in states
            states.add(payload["status"])

            if payload["status"] == "pending":
                assert (
                    payload.get("prev_status") is None
                    or payload["prev_status"] == "pending"
                )
            elif payload["status"] == "failed":
                assert payload["prev_status"] == "pending"
            else:
                raise AssertionError(f"Unexpected JobTransition payload: {payload}")
        assert states == {"pending", "failed"}
