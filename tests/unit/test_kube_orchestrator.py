import pytest

from platform_api.orchestrator.job_request import (
    Container, ContainerVolume,
    JobRequest, JobStatus, JobError,
)
from platform_api.orchestrator.kube_orchestrator import (
    Volume, VolumeMount,
    PodDescriptor, PodStatus,
)


class TestVolume:
    def test_to_primitive(self):
        volume = Volume('testvolume', host_path='/tmp')
        assert volume.to_primitive() == {
            'name': 'testvolume',
            'hostPath': {
                'path': '/tmp',
                'type': 'Directory',
            },
        }


class TestVolumeMount:
    def test_from_container_volume(self):
        volume = Volume(name='testvolume', host_path='/tmp')
        container_volume = ContainerVolume(
            src_path='/src', dst_path='/dst', read_only=True)
        mount = VolumeMount.from_container_volume(volume, container_volume)
        assert mount.volume == volume
        assert mount.mount_path == '/dst'
        assert mount.sub_path == '/src'
        assert mount.read_only

    def test_to_primitive(self):
        volume = Volume(name='testvolume', host_path='/tmp')
        mount = VolumeMount(
            volume=volume, mount_path='/dst', sub_path='/src', read_only=True)
        assert mount.to_primitive() == {
            'name': 'testvolume',
            'mountPath': '/dst',
            'subPath': '/src',
            'readOnly': True,
        }


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
                    'image': 'testimage',
                    'volumeMounts': [],
                }],
                'volumes': [],
                'restartPolicy': 'Never',
            }
        }

    def test_from_job_request(self):
        container = Container(
            image='testimage', command='testcommand 123',
            volumes=[ContainerVolume(src_path='/src', dst_path='/dst')])
        volume = Volume(name='testvolume', host_path='/tmp')
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(volume, job_request)
        assert pod.name == job_request.job_id
        assert pod.image == 'testimage'
        assert pod.args == ['testcommand', '123']
        assert pod.volume_mounts == [
            VolumeMount(volume=volume, mount_path='/dst', sub_path='/src')
        ]
        assert pod.volumes == [volume]


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

    def test_from_primitive_failure(self):
        payload = {
            'kind': 'Status',
            'code': 409,
        }
        with pytest.raises(JobError, match='already exist'):
            PodStatus.from_primitive(payload)

    def test_from_primitive_unknown_kind(self):
        payload = {
            'kind': 'Unknown',
        }
        with pytest.raises(ValueError, match='unknown kind: Unknown'):
            PodStatus.from_primitive(payload)
