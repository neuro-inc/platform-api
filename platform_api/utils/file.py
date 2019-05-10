import os


def read_file_binary(file: str) -> bytes:
    try:
        with open(file, "rb") as f:
            return f.read()
    except IOError as e:
        raise IOError(f"Could not read CA file '{os.path.abspath(file)}': {e}") from e
