from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestratorConfig:
    jobs_domain_name: str
