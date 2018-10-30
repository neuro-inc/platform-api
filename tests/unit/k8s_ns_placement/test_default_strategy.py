from platform_api.orchestrator.kube_orchestrator import SingleNamespacePlacementStrategy


def test_default_strategy() -> None:
    strategy = SingleNamespacePlacementStrategy("test-namespace")
    assert strategy.provide_namespace(None) == "test-namespace"
    assert strategy.provide_namespace("my-user") == "test-namespace"
