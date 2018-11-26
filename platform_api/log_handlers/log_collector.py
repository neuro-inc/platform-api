from typing import List


class LogEntry:
    entry: str
    time: str


class LogStorage:
    """
    Instantiable base implementation of log storage.
    Base does nothing but always returns empty results.
    """
    async def get_logs(self) -> List[LogEntry]:
        return List[LogEntry]()

    async def __aenter__(self) -> "LogStorage":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass
