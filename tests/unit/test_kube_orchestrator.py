from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.kube_orchestrator import (
    PodDescriptor, PodStatus,
)


class TestPodDescriptor:
    def test_to_primitive(self):
        pod = PodDescriptor(name='testname', image='testimage')
        assert pod.name == 'testname'
        assert pod.image == 'testimage'
        assert pod.to_primitive() == {
            'kind': 'Pod',
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


class TestPodStatus:
    def test_from_primitive(self):
        payload = {
            'kind': 'Pod',
            'status': {
                'phase': 'Running',
                'containerStatuses': [{
                    'ready': True,
                }]
            },
        }
        status = PodStatus.from_primitive(payload)
        assert status.status == JobStatus.SUCCEEDED
