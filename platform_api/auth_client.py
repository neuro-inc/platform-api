from typing import Optional

from neuro_auth_client import AuthClient as BaseAuthClient


class AuthClient(BaseAuthClient):
    async def get_user_token(self, name: str, token: Optional[str] = None) -> str:
        path = self._get_user_path(name) + "/token"
        headers = self._generate_headers(token)
        async with self._request("POST", path, headers=headers) as resp:
            payload = await resp.json()
            return payload["access_token"]
