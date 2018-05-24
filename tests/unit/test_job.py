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
        with pytest.raises(ValueError, match='Invalid scheme'):
            ContainerVolumeFactory(
                uri, src_mount_path=PurePath('/'),
                dst_mount_path=PurePath('/'))

    def test_invalid_storage_uri_path(self):
        uri = 'storage://'
        with pytest.raises(ValueError, match='Empty path'):
            ContainerVolumeFactory(
                uri, src_mount_path=PurePath('/'),
                dst_mount_path=PurePath('/'))

    def test_create(self):
        uri = 'storage://path/to/dir'
        volume = ContainerVolumeFactory(
            uri,
            src_mount_path=PurePath('/host'),
            dst_mount_path=PurePath('/container')
        ).create()
        assert volume.src_path == '/host/path/to/dir'
        assert volume.dst_path == '/container/path/to/dir'
        assert not volume.read_only
