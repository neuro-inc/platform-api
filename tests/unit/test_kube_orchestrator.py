from pathlib import PurePath

import pytest

from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume,
    JobRequest, JobStatus, JobError,
)
from platform_api.orchestrator.kube_orchestrator import (
    Volume, VolumeMount,
    PodDescriptor, PodStatus, Resources,
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

    def test_create_mount(self):
        volume = Volume('testvolume', host_path='/host')
        container_volume = ContainerVolume(
            src_path=PurePath('/host/path/to/dir'),
            dst_path=PurePath('/container/path/to/dir'))
        mount = volume.create_mount(container_volume)
        assert mount.volume == volume
        assert mount.mount_path == PurePath('/container/path/to/dir')
        assert mount.sub_path == PurePath('path/to/dir')
        assert not mount.read_only


class TestVolumeMount:
    def test_to_primitive(self):
        volume = Volume(name='testvolume', host_path=PurePath('/tmp'))
        mount = VolumeMount(
            volume=volume, mount_path=PurePath('/dst'),
            sub_path=PurePath('/src'), read_only=True)
        assert mount.to_primitive() == {
            'name': 'testvolume',
            'mountPath': '/dst',
            'subPath': '/src',
            'readOnly': True,
        }


class TestPodDescriptor:
    def test_to_primitive(self):
        pod = PodDescriptor(
            name='testname', image='testimage', env={'TESTVAR': 'testvalue'},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
        )
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
                    'env': [{'name': 'TESTVAR', 'value': 'testvalue'}],
                    'volumeMounts': [],
                    'resources': {
                        'limits': {
                            'cpu': '500m',
                            'memory': '1024Mi',
                            'nvidia.com/gpu': 1,
                        },
                    },
                }],
                'volumes': [],
                'restartPolicy': 'Never',
            }
        }

    def test_from_job_request(self):
        container = Container(
            image='testimage', command='testcommand 123',
            env={'TESTVAR': 'testvalue'},
            volumes=[ContainerVolume(
                src_path=PurePath('/tmp/src'), dst_path=PurePath('/dst'))],
            resources=ContainerResources(cpu=1, memory_mb=128, gpu=1))
        volume = Volume(name='testvolume', host_path='/tmp')
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(volume, job_request)
        assert pod.name == job_request.job_id
        assert pod.image == 'testimage'
        assert pod.args == ['testcommand', '123']
        assert pod.env == {'TESTVAR': 'testvalue'}
        assert pod.env_list == [{'name': 'TESTVAR', 'value': 'testvalue'}]
        assert pod.volume_mounts == [
            VolumeMount(
                volume=volume, mount_path=PurePath('/dst'),
                sub_path=PurePath('src'))
        ]
        assert pod.volumes == [volume]
        assert pod.resources == Resources(cpu=1, memory=128, gpu=1)


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


class TestResources:
    def test_to_primitive(self):
        resources = Resources(cpu=0.5, memory=1024)  # type: ignore
        assert resources.to_primitive() == {
            'limits': {
                'cpu': '500m',
                'memory': '1024Mi',
            },
        }

    def test_to_primitive_gpu(self):
        resources = Resources(cpu=0.5, memory=1024, gpu=2)  # type: ignore
        assert resources.to_primitive() == {
            'limits': {
                'cpu': '500m',
                'memory': '1024Mi',
                'nvidia.com/gpu': 2,
            },
        }
