"""Kubernetes-specific exceptions for Supervisor."""

from ..exceptions import HassioError


class K8sError(HassioError):
    """Base exception for all Kubernetes backend errors."""


class K8sAPIError(K8sError):
    """An unexpected error was returned by the Kubernetes API server."""


class K8sNotFound(K8sError):
    """A requested Kubernetes resource does not exist."""


class K8sTimeoutError(K8sError):
    """A Kubernetes API operation timed out."""


class K8sJobError(K8sError):
    """A Supervisor job targeting a Kubernetes resource failed."""
