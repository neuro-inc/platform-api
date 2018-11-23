import pytest

from platform_api.orchestrator.kube_client import (
    NodeSelectorOperator,
    NodeSelectorRequirement,
)


class TestNodeSelectorRequirement:
    def test_blank_key(self):
        with pytest.raises(ValueError, match="blank key"):
            NodeSelectorRequirement("", operator=NodeSelectorOperator.EXISTS)

    def test_non_empty_values_with_exists(self):
        with pytest.raises(ValueError, match="values must be empty"):
            NodeSelectorRequirement(
                "key", operator=NodeSelectorOperator.EXISTS, values=["value"]
            )

    def test_create_exists(self):
        req = NodeSelectorRequirement.create_exists("testkey")
        assert req == NodeSelectorRequirement(
            key="testkey", operator=NodeSelectorOperator.EXISTS
        )
        assert req.to_primitive() == {
            "key": "testkey",
            "operator": "Exists",
        }

    def test_create_does_not_exist(self):
        req = NodeSelectorRequirement.create_does_not_exist("testkey")
        assert req == NodeSelectorRequirement(
            key="testkey", operator=NodeSelectorOperator.DOES_NOT_EXIST
        )
        assert req.to_primitive() == {
            "key": "testkey",
            "operator": "DoesNotExist",
        }
