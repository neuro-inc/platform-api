from typing import Dict, List

from platform_api.config import GCPTokenStore, GCPTokenStoreConfig
from platform_api.log_handlers.log_collector import LogEntry, LogStorage


class GCPStackDriverLogStorage(LogStorage):
    """
    Next env variables are used to configure the instance:
    NP_LOG_STORE_GCP_SERVICE_KEY - private key of a service account
    NP_LOG_STORE_GCP_SERVICE_EMAIL - email address of a service account
    NP_LOG_STORE_GCP_SERVICE_TOKEN_EXPIRATION - token expiration in seconds,
                  less than 3600
    """

    def __init__(self, env_vars: Dict[str, str]) -> None:
        self._private_key = env_vars["NP_LOG_STORE_GCP_SERVICE_KEY"]
        self._iss = env_vars["NP_LOG_STORE_GCP_SERVICE_EMAIL"]
        self._expiration = int(
            env_vars.get(
                "NP_LOG_STORE_GCP_SERVICE_TOKEN_EXPIRATION",
                GCPTokenStoreConfig.token_expiration,
            )
        )

    async def get_logs(self) -> List[LogEntry]:
        return List[LogEntry]()

    async def __aenter__(self) -> "LogStorage":
        config = GCPTokenStoreConfig(
            private_key=self._private_key,
            iss=self._iss,
            token_expiration=self._expiration,
        )
        self._gcp_token_store = GCPTokenStore(config)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._gcp_token_store:
            await self._gcp_token_store.close()
