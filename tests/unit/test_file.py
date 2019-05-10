from pathlib import Path

from platform_api.utils.file import read_certificate_file


def test_read_certificate_file(tmp_path: Path) -> None:
    tmp_file = str((tmp_path / "temp-ca.crt").resolve())
    text = "test text"
    with open(tmp_file, "w") as f:
        f.write(text)

    data = read_certificate_file(tmp_file)
    assert isinstance(data, str)
    assert data == text


def test_read_certificate_file_not_found(tmp_path: Path) -> None:
    non_existing_file = str((tmp_path / "temp-ca.crt").resolve())
    data = read_certificate_file(non_existing_file)
    assert data is None
