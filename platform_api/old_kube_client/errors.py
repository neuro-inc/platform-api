class KubeClientException(Exception):
    """Base kube client error"""


class ResourceNotFound(KubeClientException):
    pass


class ResourceInvalid(KubeClientException):
    pass


class ResourceExists(KubeClientException):
    pass


class ResourceBadRequest(KubeClientException):
    pass


class ResourceGone(KubeClientException):
    pass


class KubeClientUnauthorized(KubeClientException):
    pass
