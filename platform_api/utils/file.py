import logging
import os


logger = logging.getLogger(__name__)


def read_certificate_file(file: str) -> str:
    try:
        with open(file, "r") as f:
            return f.read()
    except (IOError, FileNotFoundError) as e:
        raise RuntimeError(
            f"Could not read CA file '{os.path.abspath(file)}': {e}"
        ) from e
