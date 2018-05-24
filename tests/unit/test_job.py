from pathlib import PurePath

import pytest

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
        assert volume.src_path == '/host'
        assert volume.dst_path == '/container'
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
        assert volume.src_path == '/host/path/to/dir'
        assert volume.dst_path == '/container/path/to/dir'
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
