import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from .base import Telemetry
from .job import JobStats
from .kube_client import KubeClient


class KubeTelemetry(Telemetry):
    def __init__(
        self,
        kube_client: KubeClient,
        namespace_name: str,
        pod_name: str,
        container_name: str,
    ) -> None:
        self._kube_client = kube_client

        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name

    async def get_latest_stats(self) -> Optional[JobStats]:
        pod_stats = await self._kube_client.get_pod_container_stats(
            self._pod_name, self._container_name
        )
        if not pod_stats:
            return None

        return JobStats(
            cpu=pod_stats.cpu,
            memory=pod_stats.memory,
            gpu_duty_cycle=pod_stats.gpu_duty_cycle,
            gpu_memory=pod_stats.gpu_memory,
        )
