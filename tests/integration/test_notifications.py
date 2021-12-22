from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import Any
from unittest import mock

import aiohttp.web
import pytest
from neuro_admin_client import Balance, Quota
from neuro_notifications_client import CreditsWillRunOutSoon, JobCannotStartNoCredits

from platform_api.config import Config

from .api import ApiConfig, JobsClient
from .auth import UserFactory, _User
from .notifications import NotificationsServer


class TestCannotStartJobNoCredits:
    @pytest.mark.asyncio
    async def test_not_sent_has_credits(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: UserFactory,
        mock_notifications_server: NotificationsServer,
        test_cluster_name: str,
    ) -> None:
        user = await regular_user_factory(
            clusters=[(test_cluster_name, Balance(credits=Decimal("100")), Quota())]
        )
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()
        for (slug, request) in mock_notifications_server.requests:
            if slug == JobCannotStartNoCredits.slug():
                raise AssertionError("Unexpected JobCannotStartQuotaReached sent")

    @pytest.mark.asyncio
    async def test_sent_if_no_credits(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: UserFactory,
        mock_notifications_server: NotificationsServer,
        test_cluster_name: str,
    ) -> None:
        user = await regular_user_factory(
            clusters=[(test_cluster_name, Balance(credits=Decimal("0")), Quota())]
        )
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()
        assert (
            JobCannotStartNoCredits.slug(),
            {
                "user_id": user.name,
                "cluster_name": user.cluster_name,
            },
        ) in mock_notifications_server.requests


class TestJobTransition:
    @pytest.fixture
    async def run_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client_factory: Callable[[_User], JobsClient],
    ) -> AsyncIterator[Callable[[_User, dict[str, Any], bool, bool], Awaitable[str]]]:
        cleanup_pairs = []

        async def _impl(
            user: _User,
            job_request: dict[str, Any],
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
        job_request_factory: Callable[[str], dict[str, Any]],
        regular_user_factory: UserFactory,
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
        job_request_factory: Callable[[], dict[str, Any]],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_factory: UserFactory,
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

        states: set[str] = set()
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
        job_request_factory: Callable[[], dict[str, Any]],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_factory: UserFactory,
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

        states: set[str] = set()
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


class TestCreditsWillRunOutSoon:
    @pytest.mark.asyncio
    async def test_sent_if_credits_less_then_threshold(
        self,
        config: Config,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], dict[str, Any]],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_factory: UserFactory,
        mock_notifications_server: NotificationsServer,
        test_cluster_name: str,
    ) -> None:
        threshold = config.job_policy_enforcer.credit_notification_threshold

        user = await regular_user_factory(
            clusters=[(test_cluster_name, Balance(credits=threshold / 2), Quota())]
        )

        jobs_client = jobs_client_factory(user)
        job_request = job_request_factory()
        job_request["container"]["command"] = "sleep 5s"  # Let job run for some time
        job_data = await jobs_client.create_job(job_request)
        await jobs_client.long_polling_by_job_id(job_data["id"], "succeeded")

        # Notification will be sent in graceful app shutdown
        await api.runner.close()

        assert (
            CreditsWillRunOutSoon.slug(),
            {
                "user_id": user.name,
                "cluster_name": user.cluster_name,
                "credits": mock.ANY,
            },
        ) in mock_notifications_server.requests
