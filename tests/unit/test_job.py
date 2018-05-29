from pathlib import PurePath

import pytest

from platform_api.config import StorageConfig
from platform_api.handlers.models_handler import ModelRequest
from platform_api.orchestrator.job_request import (
    Container, ContainerVolume, ContainerVolumeFactory)


class TestContainer:
    def test_command_list_empty(self):
        container = Container(image='testimage')
        assert container.command_list == []

    def test_command_list(self):
        container = Container(image='testimage', command='bash -c date')
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
        with pytest.raises(ValueError, match='Invalid URI path'):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath('/host'),
                dst_mount_path=PurePath('/container')
            ).create()


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
            },
            'dataset_storage_uri': 'storage://path/to/dir',
            'result_storage_uri': 'storage://path/to/another/dir',
        }
        request = ModelRequest(payload, storage_config=storage_config)
        assert request.to_container() == Container(
            image='testimage',
            command='testcommand',
            env={
                'TESTVAR': 'testvalue',
                'DATASET_PATH': '/var/storage/path/to/dir',
                'RESULT_PATH': '/var/storage/path/to/another/dir',
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
            ]
        )
