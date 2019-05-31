import pytest

from platform_api.utils.template_parse import create_template_parser


def test_template_parse() -> None:
    template = "{job_name}-{job_owner}.jobs-dev.neu.ro"
    parse = create_template_parser(template)
    assert parse("name-owner.jobs-dev.neu.ro") == {
        "job_name": "name",
        "job_owner": "owner",
    }
    assert parse("name-job-owner.jobs-dev.neu.ro") == {
        "job_name": "name-job",
        "job_owner": "owner",
    }
    with pytest.raises(ValueError):
        parse("nameowner.jobs-dev.neu.ro")
    with pytest.raises(ValueError):
        parse("name-owner.example.org")
