from dataclasses import dataclass

from decouple import config as decouple_config


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080

    @classmethod
    def from_environ(cls) -> 'ServerConfig':
        port = decouple_config('API_PORT', cast=int, default=cls.port)
        return cls(port=port)  # type: ignore


@dataclass(frozen=True)
class Config:
    server: ServerConfig

    @classmethod
    def from_environ(cls) -> 'Config':
        server = ServerConfig.from_environ()
        return cls(server=server)  # type: ignore