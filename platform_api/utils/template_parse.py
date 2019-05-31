import re
import string
from typing import Callable, Dict


def create_template_parser(template: str) -> Callable[[str], Dict[str, str]]:
    fields = []
    chunks = []
    for literal_text, field_name, *_ in string.Formatter().parse(template):
        chunks.append(re.escape(literal_text))
        if field_name is not None:
            fields.append(field_name)
            chunks.append("(.*)")
    p = re.compile("".join(chunks))

    def parse(s):
        m = p.fullmatch(s)
        if not m:
            raise ValueError(f"Value doesn't match template {template!r}")
        return dict(zip(fields, m.groups()))

    return parse
