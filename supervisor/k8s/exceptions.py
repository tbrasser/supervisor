"""Kubernetes-specific exceptions for Supervisor.

The Kubernetes exceptions deliberately subclass their Docker counterparts so
that the large existing body of ``except DockerError`` / ``except
DockerNotFound`` handling in backend-agnostic components (apps, plugins,
Home Assistant Core management) keeps working when the Kubernetes backend is
active. This mirrors the fact that :class:`~supervisor.k8s.interface.K8sInterface`
mirrors the public API of :class:`~supervisor.docker.interface.DockerInterface`.
"""

from ..exceptions import DockerError, DockerJobError, DockerNotFound


class K8sError(DockerError):
    """Base exception for all Kubernetes backend errors."""


class K8sAPIError(K8sError):
    """An unexpected error was returned by the Kubernetes API server."""


class K8sNotFound(K8sError, DockerNotFound):
    """A requested Kubernetes resource does not exist."""


class K8sTimeoutError(K8sError):
    """A Kubernetes API operation timed out."""


class K8sJobError(K8sError, DockerJobError):
    """A Supervisor job targeting a Kubernetes resource failed."""
