from pathlib import Path
from uuid import uuid4

from platform_api.utils.file import read_file_binary


def test_read_file_binary(tmp_path: Path) -> None:
    tmp_file = str(tmp_path / f"temp-file-{uuid4()}.txt")
    text = "test text"
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write(text)

    data = read_file_binary(tmp_file)
    assert isinstance(data, bytes)
    assert data == text.encode("utf-8")


def test_read_file_binary_fails(tmp_path: Path) -> None:
    non_existing_file = str(tmp_path / f"temp-file-{uuid4()}.txt")
    data = read_file_binary(non_existing_file)
    assert data is None
