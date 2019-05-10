import logging
import os
from typing import Optional


logger = logging.getLogger(__name__)


def read_certificate_file(file: str) -> Optional[str]:
    try:
        with open(file, "r") as f:
            return f.read()
    except (IOError, FileNotFoundError) as e:
        logger.warning(f"Could not read CA file '{os.path.abspath(file)}': {e}")
        return None
