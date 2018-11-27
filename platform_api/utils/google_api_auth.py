from math import ceil
from typing import Optional

import aiohttp
from jose import constants, jwt
from yarl import URL

from platform_api.config import GCPTokenStoreConfig


class GCPTokenStore:
    def __init__(self, gcp_store_config: GCPTokenStoreConfig) -> None:

        self._token_generation_time = -1
        self._token_generated: Optional[str] = None

        self._gcp_store_config = gcp_store_config

        self._client = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._client and not self._client.closed:
            await self._client.close()

    @classmethod
    def _create_signed_jwt_key(
        cls, key: str, iss: str, auth_url: str, issue_time: int, exp_time: int
    ) -> str:
        return jwt.encode(
            headers={"alg": "RS256", "typ": "JWT"},
            claims={
                "iss": iss,
                "scope": "https://www.googleapis.com/auth/logging.read",
                "aud": auth_url,
                "iat": issue_time,
                "exp": exp_time,
            },
            key=key,
            algorithm=constants.ALGORITHMS.RS256,
        )

    async def _request(self, url: URL, current_time: int) -> Optional[str]:
        issue_time = current_time
        exp_time = current_time + self._gcp_store_config.token_expiration
        self._signed_jwt = GCPTokenStore._create_signed_jwt_key(
            self._gcp_store_config.private_key,
            self._gcp_store_config.iss,
            str(self._gcp_store_config.general_auth_url),
            issue_time,
            exp_time,
        )
        async with self._client.post(url, data=self._signed_jwt) as response:
            if 400 <= response.status:
                # TODO log here into the error log, and raise proper exception
                return None
            payload = await response.json()
            return payload["access_token"]

    async def get_token(self) -> Optional[str]:
        import time

        now = int(ceil(time.time()))
        token_age = now - self._token_generation_time
        if (not self._token_generated) or (
            token_age > self._gcp_store_config.token_expiration
        ):
            # token would expire a bit earlier
            token_value = await self._request(
                self._gcp_store_config.general_auth_url, now
            )
            if token_value:
                self._token_generation_time = now
                self._token_generated = token_value
        return self._token_generated
