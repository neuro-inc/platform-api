from pathlib import Path

import pytest

from platform_api.utils.file import read_file


def test_read_file(tmp_path: Path) -> None:
    tmp_file = str((tmp_path / "temp.txt").resolve())
    text = "test text"
    with open(tmp_file, "w") as f:
        f.write(text)

    data = read_file(tmp_file)
    assert isinstance(data, str)
    assert data == text


def test_read_file_not_found(tmp_path: Path) -> None:
    non_existing_file = str((tmp_path / "temp.txt").resolve())
    with pytest.raises(FileNotFoundError):
        read_file(non_existing_file)
