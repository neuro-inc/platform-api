from abc import ABC, abstractmethod
import logging


logger = logging.getLogger(__file__)


class StatusService(ABC):
    @abstractmethod
    async def set(self, status_id: str):
        pass

    @abstractmethod
    async def get(self, status_id: str) -> dict:
        pass


class InMemoryStatusService(StatusService):
    def __init__(self):
        self._statuses = {}

    async def set(self, status_id: str):
        if status_id in self._statuses:
            logger.info(f"update status_id {status_id}")
        else:
            logger.info(f"add status_id {status_id}")
        self._statuses[status_id] = 'test_status'

    async def get(self, status_id: str) -> dict:
        status_info = self._statuses.get(status_id)
        return status_info
