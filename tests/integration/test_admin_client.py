from decimal import Decimal

import pytest

from platform_api.admin_client import AdminClient

from .admin import AdminChargeRequest, AdminServer


class TestAdminClient:
    @pytest.mark.asyncio
    async def test_patch_user_credits(self, mock_admin_server: AdminServer) -> None:
        cluster_name = "test-cluster"
        username = "username"
        amount = Decimal("20.11")
        key = "key"
        async with AdminClient(base_url=mock_admin_server.url) as client:
            await client.change_user_credits(cluster_name, username, amount, key)

        assert len(mock_admin_server.requests) == 1
        assert mock_admin_server.requests[0] == AdminChargeRequest(
            key,
            cluster_name,
            username,
            amount,
        )
