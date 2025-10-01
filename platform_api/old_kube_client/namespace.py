import dataclasses
from typing import Any, Optional

from platform_api.old_kube_client.client import KubeClient


@dataclasses.dataclass
class Label:
    name: str
    value: str


@dataclasses.dataclass
class Namespace:
    uid: str
    name: str
    labels: list[Label]

    @property
    def labels_as_dict(self) -> dict[str, str]:
        return {label.name: label.value for label in self.labels}

    @classmethod
    def from_kube(cls, kube_response: dict[str, Any]) -> "Namespace":
        return cls(
            uid=kube_response["metadata"]["uid"],
            name=kube_response["metadata"]["name"],
            labels=[
                Label(name=name, value=value)
                for name, value in kube_response["metadata"]["labels"].items()
            ],
        )


class NamespaceApi:
    """
    Namespace APIs wrapper
    """

    def __init__(self, kube_client: KubeClient):
        self._kube = kube_client

    async def get_namespaces(self) -> list[Namespace]:
        namespaces = await self._kube.get(self._kube.namespaces_url)
        return [Namespace.from_kube(spec) for spec in namespaces["items"]]

    async def get_namespace(self, name: str) -> Namespace:
        url = self._kube.generate_namespace_url(name)
        response = await self._kube.get(url=url)
        return Namespace.from_kube(response)

    async def create_namespace(
        self, name: str, *, labels: Optional[dict[str, str]] = None
    ) -> Namespace:
        payload: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": name},
        }
        if labels:
            payload["metadata"]["labels"] = labels
        response = await self._kube.post(url=self._kube.namespaces_url, json=payload)
        return Namespace.from_kube(response)

    async def delete_namespace(self, name: str) -> dict[str, Any]:
        url = self._kube.generate_namespace_url(name)
        return await self._kube.delete(url)
