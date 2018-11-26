from typing import Dict

from platform_api.config import LogStorageConfig
from platform_api.log_handlers.gcp_stackdriver_logging import GCPStackDriverLogStorage
from platform_api.log_handlers.log_collector import LogStorage


class LogFactory:
    @classmethod
    def get_logger(cls, log_store_conf: LogStorageConfig) -> LogStorage:
        """
        Uses NP_LOG_STORE_IMPL to identify real class to be used.
        In case class not found, it would fallback to default impl
        which does nothing
        :param log_env_vars:
        :return: LogStorage
        """
        log_env_vars: Dict[str, str] = log_store_conf.env_vars
        log_name = log_env_vars.get('NP_LOG_STORE_IMPL', None)
        if log_name == "gcp_stackdriver":
            return GCPStackDriverLogStorage(log_env_vars)
        return LogStorage()
