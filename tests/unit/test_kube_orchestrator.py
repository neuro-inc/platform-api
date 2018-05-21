import pytest

from platform_api.orchestrator.kube_orchestrator import PodDescriptor


class TestPodDescriptor:
    def test_to_primitive(self):
        pod = PodDescriptor(name='testname', image='testimage')
        assert pod.name == 'testname'
        assert pod.image == 'testimage'
        assert pod.to_primitive() == {
            'kind': 'Pod1',
            'apiVersion': 'v1',
            'metadata': {
                'name': 'testname',
            },
            'spec': {
                'containers': [{
                    'name': 'testname',
                    'image': 'testimage'
                }]
            }
        }
