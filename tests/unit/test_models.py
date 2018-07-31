import pytest

from platform_api.handlers.models_handler import (
    create_model_response_validator
)
from platform_api.handlers.validators import create_container_request_validator


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self):
        return {
            'image': 'testimage',
            'resources': {
                'cpu': 0.1,
                'memory_mb': 16,
            },
            'volumes': [{
                'src_storage_uri': 'storage:///',
                'dst_path': '/var/storage',
            }],
        }

    def test_allowed_volumes(self, payload):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload)
        assert result['volumes'][0]['read_only']

    def test_disallowed_volumes(self, payload):
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match='volumes is not allowed key'):
            validator.check(payload)


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
