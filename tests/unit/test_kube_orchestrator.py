from pathlib import PurePath
from unittest import mock

import aiohttp
import pytest

from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume, JobError, JobRequest,
    JobStatus
)
from platform_api.orchestrator.kube_orchestrator import (
    ContainerStatus, HostVolume, Ingress, IngressRule, JobStatusItemFactory,
    NfsVolume, PodDescriptor, PodStatus, Resources, Service,
    SharedMemoryVolume, VolumeMount
)
from platform_api.orchestrator.logs import FilteredStreamWrapper


class TestVolume:
    @pytest.mark.parametrize('volume', (
        HostVolume('testvolume', path='/host'),  # type: ignore
        NfsVolume(
            'testvolume', server='1.2.3.4', path='/host'),  # type: ignore
    ))
    def test_create_mount(self, volume):
        container_volume = ContainerVolume(
            src_path=PurePath('/host/path/to/dir'),
            dst_path=PurePath('/container/path/to/dir'))
        mount = volume.create_mount(container_volume)
        assert mount.volume == volume
        assert mount.mount_path == PurePath('/container/path/to/dir')
        assert mount.sub_path == PurePath('path/to/dir')
        assert not mount.read_only


class TestHostVolume:
    def test_to_primitive(self):
        volume = HostVolume('testvolume', path='/tmp')
        assert volume.to_primitive() == {
            'name': 'testvolume',
            'hostPath': {
                'path': '/tmp',
                'type': 'Directory',
            },
        }


class TestNfsVolume:
    def test_to_primitive(self):
        volume = NfsVolume(
            'testvolume', server='1.2.3.4', path=PurePath('/tmp'))
        assert volume.to_primitive() == {
            'name': 'testvolume',
            'nfs': {
                'server': '1.2.3.4',
                'path': '/tmp',
            },
        }


class TestVolumeMount:
    def test_to_primitive(self):
        volume = HostVolume(name='testvolume', path=PurePath('/tmp'))
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
            port=1234,
        )
        assert pod.name == 'testname'
        assert pod.image == 'testimage'
        assert pod.to_primitive() == {
            'kind': 'Pod',
            'apiVersion': 'v1',
            'metadata': {
                'name': 'testname',
                'labels': {
                    'job': 'testname',
                },
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
                    'ports': [{'containerPort': 1234}],
                    'readinessProbe': {
                        'httpGet': {'port': 1234, 'path': '/'},
                        'initialDelaySeconds': 1,
                        'periodSeconds': 1,
                    },
                    'terminationMessagePolicy': 'FallbackToLogsOnError',
                }],
                'volumes': [],
                'restartPolicy': 'Never',
            }
        }

    def test_to_primitive_with_dev_shm(self):
        dev_shm = SharedMemoryVolume(name='dshm', path=None)
        container_volume = ContainerVolume(dst_path=PurePath('/dev/shm'),
                                           src_path=None)
        pod = PodDescriptor(
            name='testname', image='testimage', env={'TESTVAR': 'testvalue'},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
            port=1234,
            volumes=[dev_shm],
            volume_mounts=[dev_shm.create_mount(container_volume)]
        )
        assert pod.name == 'testname'
        assert pod.image == 'testimage'
        assert pod.to_primitive() == {
            'kind': 'Pod',
            'apiVersion': 'v1',
            'metadata': {
                'name': 'testname',
                'labels': {
                    'job': 'testname',
                },
            },
            'spec': {
                'containers': [{
                    'name': 'testname',
                    'image': 'testimage',
                    'env': [{'name': 'TESTVAR', 'value': 'testvalue'}],
                    'volumeMounts': [{
                        'name': 'dshm',
                        'mountPath': '/dev/shm',
                        'readOnly': False,
                        'subPath': '.',
                    }],
                    'resources': {
                        'limits': {
                            'cpu': '500m',
                            'memory': '1024Mi',
                            'nvidia.com/gpu': 1,
                        },
                    },
                    'ports': [{'containerPort': 1234}],
                    'readinessProbe': {
                        'httpGet': {'port': 1234, 'path': '/'},
                        'initialDelaySeconds': 1,
                        'periodSeconds': 1,
                    },
                    'terminationMessagePolicy': 'FallbackToLogsOnError',
                }],
                'volumes': [{
                    'name': 'dshm',
                    'emptyDir': {
                        'medium': 'Memory',
                    }
                }],
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
        volume = HostVolume(name='testvolume', path='/tmp')
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

    def test_from_primitive(self):
        payload = {
            'kind': 'Pod',
            'metadata': {'name': 'testname'},
            'spec': {'containers': [{
                'name': 'testname',
                'image': 'testimage',
            }]},
            'status': {
                'phase': 'Running',
            },
        }
        pod = PodDescriptor.from_primitive(payload)
        assert pod.name == 'testname'
        assert pod.image == 'testimage'
        assert pod.status.phase == 'Running'

    def test_from_primitive_failure(self):
        payload = {
            'kind': 'Status',
            'code': 409,
        }
        with pytest.raises(JobError, match='already exist'):
            PodDescriptor.from_primitive(payload)

    def test_from_primitive_unknown_kind(self):
        payload = {
            'kind': 'Unknown',
        }
        with pytest.raises(ValueError, match='unknown kind: Unknown'):
            PodDescriptor.from_primitive(payload)


class TestJobStatusItemFactory:
    @pytest.mark.parametrize('phase, expected_status', (
        ('Succeeded', JobStatus.SUCCEEDED),
        ('Failed', JobStatus.FAILED),
        ('Unknown', JobStatus.FAILED),
        ('Running', JobStatus.RUNNING),
        ('NewPhase', JobStatus.PENDING),
    ))
    def test_status(self, phase, expected_status):
        payload = {'phase': phase}
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(expected_status)

    def test_status_pending(self):
        payload = {'phase': 'Pending'}
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(JobStatus.PENDING)

    def test_status_pending_creating(self):
        payload = {
            'phase': 'Pending', 'containerStatuses': [{
                'state': {'waiting': {'reason': 'ContainerCreating'}},
            }]
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(JobStatus.PENDING)

    def test_status_pending_failure(self):
        payload = {
            'phase': 'Pending', 'containerStatuses': [{
                'state': {'waiting': {'reason': 'SomeWeirdReason'}},
            }]
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.FAILED, reason='SomeWeirdReason')

    def test_status_failure(self):
        payload = {
            'phase': 'Failed', 'containerStatuses': [{
                'state': {'terminated': {
                    'reason': 'Error', 'message': 'Failed!\n', 'exitCode': 123,
                }},
            }]
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.FAILED, reason='Error',
            description='Failed!\n\nExit code: 123')


class TestPodStatus:
    def test_from_primitive(self):
        payload = {
            'phase': 'Running',
            'containerStatuses': [{
                'ready': True,
            }]
        }
        status = PodStatus.from_primitive(payload)
        assert status.phase == 'Running'


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

    def test_from_container_resources(self):
        container_resources = ContainerResources(  # type: ignore
            cpu=1, memory_mb=128, gpu=1)
        resources = Resources.from_container_resources(container_resources)
        assert resources == Resources(cpu=1, memory=128, gpu=1)


class TestIngressRule:
    def test_from_primitive_host(self):
        rule = IngressRule.from_primitive({
            'host': 'testhost',
        })
        assert rule == IngressRule(host='testhost')

    def test_from_primitive_no_paths(self):
        rule = IngressRule.from_primitive({
            'host': 'testhost',
            'http': {'paths': []},
        })
        assert rule == IngressRule(host='testhost')

    def test_from_primitive_no_backend(self):
        rule = IngressRule.from_primitive({
            'host': 'testhost',
            'http': {'paths': [{}]},
        })
        assert rule == IngressRule(host='testhost')

    def test_from_primitive_no_service(self):
        rule = IngressRule.from_primitive({
            'host': 'testhost',
            'http': {'paths': [{'backend': {}}]},
        })
        assert rule == IngressRule(host='testhost')

    def test_from_primitive(self):
        rule = IngressRule.from_primitive({
            'host': 'testhost',
            'http': {'paths': [{'backend': {
                'serviceName': 'testname',
                'servicePort': 1234,
            }}]},
        })
        assert rule == IngressRule(
            host='testhost', service_name='testname', service_port=1234)

    def test_to_primitive_no_service(self):
        rule = IngressRule(host='testhost')
        assert rule.to_primitive() == {
            'host': 'testhost',
        }

    def test_to_primitive(self):
        rule = IngressRule(
            host='testhost', service_name='testname', service_port=1234)
        assert rule.to_primitive() == {
            'host': 'testhost',
            'http': {'paths': [{
                'backend': {
                    'serviceName': 'testname',
                    'servicePort': 1234,
                }
            }]},
        }

    def test_from_service(self):
        service = Service(name='testname', target_port=1234)
        rule = IngressRule.from_service(
            domain_name='testdomain', service=service)
        assert rule == IngressRule(
            host='testname.testdomain', service_name='testname',
            service_port=80)


class TestIngress:
    def test_from_primitive_no_rules(self):
        ingress = Ingress.from_primitive({
            'kind': 'Ingress',
            'metadata': {'name': 'testingress'},
            'spec': {'rules': []}
        })
        assert ingress == Ingress(name='testingress', rules=[])

    def test_from_primitive(self):
        ingress = Ingress.from_primitive({
            'kind': 'Ingress',
            'metadata': {'name': 'testingress'},
            'spec': {'rules': [{
                'host': 'testhost',
            }]}
        })
        assert ingress == Ingress(
            name='testingress', rules=[IngressRule(host='testhost')])

    def test_find_rule_index_by_host(self):
        ingress = Ingress(name='testingress', rules=[
            IngressRule(host='host1'),
            IngressRule(host='host2'),
            IngressRule(host='host3')])
        assert ingress.find_rule_index_by_host('host1') == 0
        assert ingress.find_rule_index_by_host('host2') == 1
        assert ingress.find_rule_index_by_host('host4') == -1

    def test_to_primitive_no_rules(self):
        ingress = Ingress(name='testingress')
        assert ingress.to_primitive() == {
            'metadata': {'name': 'testingress'},
            'spec': {'rules': [None]},
        }

    def test_to_primitive(self):
        ingress = Ingress(name='testingress', rules=[
            IngressRule(
                host='host1', service_name='testservice', service_port=1234)
        ])
        assert ingress.to_primitive() == {
            'metadata': {'name': 'testingress'},
            'spec': {'rules': [{
                'host': 'host1',
                'http': {'paths': [{
                    'backend': {
                        'serviceName': 'testservice',
                        'servicePort': 1234
                    }
                }]},
            }]}
        }


class TestService:
    @pytest.fixture
    def service_payload(self):
        return {
            'metadata': {'name': 'testservice'},
            'spec': {
                'type': 'NodePort',
                'ports': [{'port': 80, 'targetPort': 8080}],
                'selector': {'job': 'testservice'},
            },
        }

    def test_to_primitive(self, service_payload):
        service = Service(name='testservice', target_port=8080)
        assert service.to_primitive() == service_payload

    def test_from_primitive(self, service_payload):
        service = Service.from_primitive(service_payload)
        assert service == Service(name='testservice', target_port=8080)

    def test_create_for_pod(self):
        pod = PodDescriptor(name='testpod', image='testimage', port=1234)
        service = Service.create_for_pod(pod)
        assert service == Service(name='testpod', target_port=1234)


class TestContainerStatus:
    def test_no_state(self):
        payload = {'state': {}}
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert status.reason is None
        assert status.message is None

    @pytest.mark.parametrize('payload', (
        None, {}, {'state': {}}, {'state': {'waiting': {}}},
        {'state': {'waiting': {'reason': 'ContainerCreating'}}},
    ))
    def test_is_waiting_creating(self, payload):
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert status.is_creating
        assert not status.is_terminated

        with pytest.raises(AssertionError):
            status.exit_code

    @pytest.mark.parametrize('payload', (
        {'state': {'waiting': {'reason': 'NOT CREATING'}}},
    ))
    def test_is_waiting_not_creating(self, payload):
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert not status.is_creating
        assert not status.is_terminated
        assert status.reason == 'NOT CREATING'
        assert status.message is None

    @pytest.mark.parametrize('payload', (
        {'state': {'running': {}}},
        {'state': {'terminated': {}}},
    ))
    def test_is_not_waiting(self, payload):
        status = ContainerStatus(payload)
        assert not status.is_waiting
        assert not status.is_creating

    def test_is_terminated(self):
        payload = {'state': {'terminated': {
            'reason': 'Error',
            'message': 'Failed!',
            'exitCode': 123,
        }}}
        status = ContainerStatus(payload)
        assert status.is_terminated
        assert status.reason == 'Error'
        assert status.message == 'Failed!'
        assert status.exit_code == 123


class TestFilteredStreamWrapper:
    @pytest.mark.asyncio
    async def test_read_eof(self):
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_read_two_lines_eof(self):
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b'line1\n')
        reader.feed_data(b'line2')
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b'line1\n'
        chunk = await stream.read()
        assert chunk == b'line2'

    @pytest.mark.asyncio
    async def test_half_line(self):
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b'line1\n')
        reader.feed_data(b'line2\n')
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read(size=2)
        assert chunk == b'li'
        chunk = await stream.read(size=2)
        assert chunk == b'ne'

        reader.feed_data(b'line3')
        reader.feed_eof()

        chunk = await stream.read(size=2)
        assert chunk == b'1\n'
        chunk = await stream.read()
        assert chunk == b'line2\n'
        chunk = await stream.read()
        assert chunk == b'line3'

    @pytest.mark.asyncio
    async def test_filtered_single_rpc_error(self):
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b'line1\n')
        reader.feed_data(b'rpc error: code = whatever')
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b'line1\n'
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_filtered_two_rpc_errors(self):
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b'line1\n')
        reader.feed_data(b'rpc error: code = whatever\n')
        reader.feed_data(b'rpc error: code = again\n')
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b'line1\n'
        chunk = await stream.read()
        assert chunk == b'rpc error: code = whatever\n'
        chunk = await stream.read()
        assert not chunk
