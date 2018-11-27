from typing import Any
from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest

from platform_api.config import GCPTokenStoreConfig
from platform_api.utils.google_api_auth import GCPTokenStore


class MockResponse:
    def __init__(self, code: int, payload: Any) -> None:
        self._code = code
        self._payload = payload

    async def json(self) -> Any:
        return self._payload

    @property
    def status(self) -> int:
        return self._code


class TestGCPStackDriverConf:
    """
    Test cases to ensure stack driver logging storage properly picked.
    """

    private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIBzAIBAAJhAL+pBqnLy6A7x0FWpDOKKO242q4sSjN37NazvdBCy3/TcySghY49
yV1KbEkx/xDhUZQDlrUOcUiUvUkynjVRnY6MzPP1LzOZ4TUkSOQZeSF/7zGzpDMp
QDq9PozliAVydQIDAQABAmEAu7cU66DK4zkpQvlVAZXD2HFA3W5LjnVa5j5NHnkN
TzkOgUUnCdXCLzkBLf9lzmafCSSHQ0QOobEl+zjB6o8cj+gPh7vBHa8lTO6+eV+o
fMJ/xYadU0EYPcApQQAZhKcBAjEA60eYaWwmIm0jxNayMlWALVCR5kfVLfTyV6Wy
algD16UbUXQvjXDm7Zu2E51LFwgRAjEA0IoGyIimN/F86V0IuuN0aHm7dgeMG/Ia
Obw/wP5wiGUnub5R0G7Wur5O/W69JMglAjEAyjVVKz3UpH/SXwj6S8IqEEgPrK+N
6xp68ZMp/LW6T6rKCL5nZBNllU7fNIpaD+hRAjBdG9Ntg72bKsIXu4cjUlzuLLPr
PfFrpMvazVc1xyTdcTSsdPI4etR66m+ALgpbOtUCMQChuZwmmaKcgYY7+tRvKoFG
gUNwOdUjaxFd6quGJh8TAhRwgnHIOqjQ7Bau9SnZ3JE=
-----END RSA PRIVATE KEY-----"""
    config = GCPTokenStoreConfig(
        private_key, iss="test-service@neuromation.io", token_expiration=10
    )

    @classmethod
    def mocked_async_context_manager(cls, return_value=None):
        class ContextManager:
            def __init__(self, *args, **kwargs):
                self._args = args

            async def __aenter__(self):
                return return_value

            async def __aexit__(self, *args):
                pass

        return Mock(wraps=ContextManager)

    @pytest.mark.asyncio
    async def test_gcp_token(self) -> None:
        def wrapper_async_call(mock):
            async def internal(*args, **kwargs):
                return mock()

            return internal

        store = GCPTokenStore(
            GCPTokenStoreConfig(
                private_key="private-key",
                iss="test-service@neuromation.io",
                token_expiration=10,
            )
        )

        with mock.patch("time.time") as mock_time:
            mock_time.return_value = -2
            magic_mock = MagicMock()
            store._request = wrapper_async_call(magic_mock)
            await store.get_token()
            assert magic_mock.call_count == 1

            mock_time.return_value = 4
            await store.get_token()
            assert magic_mock.call_count == 1

            mock_time.return_value = 5
            await store.get_token()
            assert magic_mock.call_count == 1

            mock_time.return_value = 30
            await store.get_token()
            assert magic_mock.call_count == 2

        await store.close()

    @pytest.mark.asyncio
    async def test_gcp_token_http(self) -> None:

        store = GCPTokenStore(TestGCPStackDriverConf.config)

        with mock.patch(
            "aiohttp.ClientSession.request",
            new=TestGCPStackDriverConf.mocked_async_context_manager(
                MockResponse(400, None)
            ),
        ):
            with mock.patch("time.time") as mock_time:
                mock_time.return_value = 1000
                assert await store.get_token() is None

        await store.close()

    @pytest.mark.asyncio
    async def test_gcp_token_valid_response(self) -> None:
        store = GCPTokenStore(TestGCPStackDriverConf.config)

        with mock.patch(
            "aiohttp.ClientSession.post",
            new=TestGCPStackDriverConf.mocked_async_context_manager(
                MockResponse(200, {"access_token": "token_test_value"})
            ),
        ):
            with mock.patch("time.time") as mock_time:
                mock_time.return_value = 1020
                assert await store.get_token() == "token_test_value"

        await store.close()
