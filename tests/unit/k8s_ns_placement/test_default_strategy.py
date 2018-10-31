from platform_api.orchestrator.kube_orchestrator import SingleNamespaceStrategy


def test_default_strategy() -> None:
    strategy = SingleNamespaceStrategy("test-namespace")
    assert strategy.provide_namespace(None) == "test-namespace"
    assert strategy.provide_namespace("my-user") == "test-namespace"
