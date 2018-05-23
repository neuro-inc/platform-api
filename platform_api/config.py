from dataclasses import dataclass

from decouple import config as decouple_config

from .orchestrator import KubeConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    orchestrator_config: KubeConfig
