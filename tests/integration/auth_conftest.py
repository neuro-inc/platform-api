from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Sequence

from aiohttp.hdrs import AUTHORIZATION
from neuro_auth_client import AuthClient, Permission

from platform_api.user import User


class _AuthClient(AuthClient):
    async def grant_user_permissions(
        self, name: str, permissions: Sequence[Permission], token: Optional[str] = None
    ) -> None:
        assert permissions, "No permissions passed"
        path = "/api/v1/users/{name}/permissions".format(name=name)
        headers = self._generate_headers(token)
        payload: List[Dict[str, str]] = [asdict(p) for p in permissions]
        await self._request("POST", path, headers=headers, json=payload)


@dataclass(frozen=True)
class _User(User):
    @property
    def headers(self) -> Dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}
