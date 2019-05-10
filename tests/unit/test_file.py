from pathlib import Path

from platform_api.utils.file import read_certificate_file


def test_read_certificate_file(tmp_path: Path) -> None:
    tmp_file = str(tmp_path / "temp-ca.crt")
    text = "test text"
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write(text)

    data = read_certificate_file(tmp_file)
    assert isinstance(data, bytes)
    assert data == text.encode("utf-8")


def test_read_certificate_file(tmp_path: Path) -> None:
    non_existing_file = str(tmp_path / "temp-ca.crt")
    data = read_certificate_file(non_existing_file)
    assert data is None
