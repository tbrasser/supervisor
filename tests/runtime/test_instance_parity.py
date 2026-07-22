"""Test that both container backends satisfy the runtime instance protocols."""

import pytest

from supervisor.docker.app import DockerApp
from supervisor.docker.homeassistant import DockerHomeAssistant
from supervisor.docker.interface import DockerInterface
from supervisor.k8s.app import K8sApp
from supervisor.k8s.homeassistant import K8sHomeAssistant
from supervisor.k8s.interface import K8sInterface
from supervisor.runtime.interface import (
    AppInstance,
    HomeAssistantInstance,
    WorkloadInstance,
)


@pytest.mark.parametrize(
    ("protocol", "implementations"),
    [
        (WorkloadInstance, (DockerInterface, K8sInterface)),
        (AppInstance, (DockerApp, K8sApp)),
        (HomeAssistantInstance, (DockerHomeAssistant, K8sHomeAssistant)),
    ],
    ids=["workload", "app", "homeassistant"],
)
def test_backend_implements_protocol(protocol, implementations):
    """Test each backend class provides every member of its protocol."""
    for implementation in implementations:
        missing = sorted(
            member
            for member in protocol.__protocol_attrs__
            if not hasattr(implementation, member)
        )
        assert not missing, (
            f"{implementation.__name__} is missing members required by "
            f"{protocol.__name__}: {', '.join(missing)}"
        )
