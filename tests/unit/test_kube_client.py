import pytest

from platform_api.orchestrator.kube_client import (
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorOperator,
    NodeSelectorRequirement,
    NodeSelectorTerm,
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
        assert req.to_primitive() == {"key": "testkey", "operator": "Exists"}

    def test_create_does_not_exist(self):
        req = NodeSelectorRequirement.create_does_not_exist("testkey")
        assert req == NodeSelectorRequirement(
            key="testkey", operator=NodeSelectorOperator.DOES_NOT_EXIST
        )
        assert req.to_primitive() == {"key": "testkey", "operator": "DoesNotExist"}


class TestNodeSelectorTerm:
    def test_empty(self):
        with pytest.raises(ValueError, match="no expressions"):
            NodeSelectorTerm([])


class TestNodeAffinity:
    def test_empty(self):
        with pytest.raises(ValueError, match="no terms"):
            NodeAffinity()

    def test_to_primitive(self):
        node_affinity = NodeAffinity(
            required=[
                NodeSelectorTerm([NodeSelectorRequirement.create_exists("testkey")])
            ],
            preferred=[
                NodePreferredSchedulingTerm(
                    NodeSelectorTerm(
                        [NodeSelectorRequirement.create_does_not_exist("anotherkey")]
                    )
                )
            ],
        )
        assert node_affinity.to_primitive() == {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "testkey", "operator": "Exists"}]}
                ]
            },
            "preferredDuringSchedulingIgnoredDuringExecution": [
                {
                    "preference": {
                        "matchExpressions": [
                            {"key": "anotherkey", "operator": "DoesNotExist"}
                        ]
                    },
                    "weight": 100,
                }
            ],
        }
