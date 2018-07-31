import dataclasses
from datetime import datetime, timezone
from pathlib import PurePath
from unittest import mock

import pytest

from platform_api.config import StorageConfig
from platform_api.handlers.models_handler import ModelRequest
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume, ContainerVolumeFactory,
    JobRequest, JobStatus
)


class TestContainer:
    def test_command_list_empty(self):
        container = Container(
            image='testimage',
            resources=ContainerResources(cpu=1, memory_mb=128))
        assert container.command_list == []

    def test_command_list(self):
        container = Container(
            image='testimage', command='bash -c date',
            resources=ContainerResources(cpu=1, memory_mb=128))
        assert container.command_list == ['bash', '-c', 'date']


class TestContainerVolumeFactory:
    def test_invalid_storage_uri_scheme(self):
        uri = 'invalid://path'
        with pytest.raises(ValueError, match='Invalid URI scheme'):
            ContainerVolumeFactory(
                uri, src_mount_path=PurePath('/'),
                dst_mount_path=PurePath('/'))

    @pytest.mark.parametrize('uri', (
        'storage:///',
        'storage://',))
    def test_invalid_storage_uri_path(self, uri):
        volume = ContainerVolumeFactory(
            uri,
            src_mount_path=PurePath('/host'),
            dst_mount_path=PurePath('/container')
        ).create()
        assert volume.src_path == PurePath('/host')
        assert volume.dst_path == PurePath('/container')
        assert not volume.read_only

    @pytest.mark.parametrize('uri', (
        'storage:///path/to/dir',
        'storage:///path/to//dir',
        'storage:///path/to/./dir',
        'storage://path/to/dir',))
    def test_create(self, uri):
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath('/host'),
            dst_mount_path=PurePath('/container'),
            read_only=True,
        )
        assert volume.src_path == PurePath('/host/path/to/dir')
        assert volume.dst_path == PurePath('/container/path/to/dir')
        assert volume.read_only

    @pytest.mark.parametrize('uri', (
        'storage:///../to/dir',
        'storage://path/../dir',))
    def test_create_invalid_path(self, uri):
        uri = 'storage:///../outside/file'
        with pytest.raises(ValueError, match='Invalid path'):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath('/host'),
                dst_mount_path=PurePath('/container')
            ).create()

    def test_create_without_extending_dst_mount_path(self):
        uri = 'storage:///path/to/dir'
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath('/host'),
            dst_mount_path=PurePath('/container'),
            read_only=True,
            extend_dst_mount_path=False,
        )
        assert volume.src_path == PurePath('/host/path/to/dir')
        assert volume.dst_path == PurePath('/container')
        assert volume.read_only

    def test_relative_dst_mount_path(self):
        uri = 'storage:///path/to/dir'
        with pytest.raises(ValueError, match='Mount path must be absolute'):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath('/host'),
                dst_mount_path=PurePath('container')
            )

    def test_dots_dst_mount_path(self):
        uri = 'storage:///path/to/dir'
        with pytest.raises(ValueError, match='Invalid path'):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath('/host'),
                dst_mount_path=PurePath('/container/../path')
            )


class TestModelRequest:
    def test_to_container(self):
        storage_config = StorageConfig(  # type: ignore
            host_mount_path=PurePath('/tmp'),
        )
        payload = {
            'container': {
                'image': 'testimage',
                'command': 'testcommand',
                'env': {'TESTVAR': 'testvalue'},
                'resources': {
                    'cpu': 0.1,
                    'memory_mb': 128,
                    'gpu': 1,
                },
                'http': {
                    'port': 80,
                },
            },
            'dataset_storage_uri': 'storage://path/to/dir',
            'result_storage_uri': 'storage://path/to/another/dir',
        }
        request = ModelRequest(
            payload, storage_config=storage_config, env_prefix='NP')
        assert request.to_container() == Container(
            image='testimage',
            command='testcommand',
            env={
                'TESTVAR': 'testvalue',
                'NP_DATASET_PATH': '/var/storage/path/to/dir',
                'NP_RESULT_PATH': '/var/storage/path/to/another/dir',
            },
            volumes=[
                ContainerVolume(
                    src_path=PurePath('/tmp/path/to/dir'),
                    dst_path=PurePath('/var/storage/path/to/dir'),
                    read_only=True),
                ContainerVolume(
                    src_path=PurePath('/tmp/path/to/another/dir'),
                    dst_path=PurePath('/var/storage/path/to/another/dir'),
                    read_only=False)
            ],
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1),
            port=80,
            health_check_path='/',
        )


@pytest.fixture
def job_request_payload():
    return {
        'job_id': 'testjob',
        'container': {
            'image': 'testimage',
            'resources': {'cpu': 1, 'memory_mb': 128, 'gpu': None},
            'command': None,
            'env': {'testvar': 'testval'},
            'volumes': [{
                'src_path': '/src/path',
                'dst_path': '/dst/path',
                'read_only': False,
            }],
            'port': None,
            'health_check_path': '/',
        },
    }


class TestJob:
    @pytest.fixture
    def job_request(self):
        container = Container(
            image='testimage',
            resources=ContainerResources(cpu=1, memory_mb=128),
            port=1234,
        )
        return JobRequest(job_id='testjob', container=container)

    def test_http_url(self, mock_orchestrator, job_request):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request)
        assert job.http_url == 'http://testjob.jobs'

    def test_should_be_deleted_pending(self, mock_orchestrator, job_request):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request)
        assert not job.finished_at
        assert not job.should_be_deleted

    def test_should_be_deleted_finished(self, mock_orchestrator, job_request):
        config = dataclasses.replace(
            mock_orchestrator.config, job_deletion_delay_s=0)
        job = Job(orchestrator_config=config, job_request=job_request)
        job.status = JobStatus.FAILED
        assert job.finished_at
        assert job.should_be_deleted

    def test_to_primitive(self, mock_orchestrator, job_request):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request)
        job.status = JobStatus.FAILED
        job.is_deleted = True
        assert job.to_primitive() == {
            'id': job.id,
            'request': mock.ANY,
            'status': 'failed',
            'is_deleted': True,
            'finished_at': job.finished_at.isoformat(),
        }

    def test_from_primitive(self, mock_orchestrator, job_request_payload):
        payload = {
            'id': 'testjob',
            'request': job_request_payload,
            'status': 'succeeded',
            'is_deleted': True,
            'finished_at': datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator, payload)
        assert job.id == 'testjob'
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_deleted
        assert job.finished_at


class TestJobRequest:
    def test_to_primitive(self, job_request_payload):
        container = Container(
            image='testimage',
            env={'testvar': 'testval'},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[ContainerVolume(
                src_path=PurePath('/src/path'),
                dst_path=PurePath('/dst/path'))])
        request = JobRequest(job_id='testjob', container=container)
        assert request.to_primitive() == job_request_payload

    def test_from_primitive(self, job_request_payload):
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == 'testjob'
        expected_container = Container(
            image='testimage',
            env={'testvar': 'testval'},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[ContainerVolume(
                src_path=PurePath('/src/path'),
                dst_path=PurePath('/dst/path'))])
        assert request.container == expected_container
