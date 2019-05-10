import os


def read_certificate_file(file: str) -> str:
    try:
        with open(file, "r") as f:
            return f.read()
    except IOError as e:
        raise IOError(f"Could not read CA file '{os.path.abspath(file)}': {e}") from e
