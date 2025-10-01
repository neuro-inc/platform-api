from dataclasses import dataclass, field
from enum import Enum

NAMESPACE_DEFAULT = "default"


class KubeClientAuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: str | None = field(repr=False, default=None)
    cert_authority_path: str | None = None
    auth_type: KubeClientAuthType = KubeClientAuthType.NONE
    auth_cert_path: str | None = None
    auth_cert_key_path: str | None = None
    token: str | None = field(repr=False, default=None)
    token_path: str | None = None
    namespace: str = NAMESPACE_DEFAULT
    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_watch_timeout_s: int = 1800
    client_conn_pool_size: int = 100
