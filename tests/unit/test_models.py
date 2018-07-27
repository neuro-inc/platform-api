import pytest

from platform_api.handlers.models_handler import (
    create_model_response_validator
)


class TestModelResponseValidator:
    def test_empty(self):
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match='is required'):
            validator.check({})

    def test_failure(self):
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match="doesn't match any variant"):
            validator.check({
                'job_id': 'testjob',
                'status': 'INVALID',
            })

    def test_success(self):
        validator = create_model_response_validator()
        assert validator.check({
            'job_id': 'testjob',
            'status': 'pending',
            'http_url': 'http://testjob',
        })
