"""Kubernetes workload definition for HA Multicast plugin."""

import logging

from awesomeversion import AwesomeVersion

from ..docker.const import ENV_TIME
from ..docker.multicast import MULTICAST_DOCKER_NAME
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .exceptions import K8sJobError
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

MULTICAST_K8S_NAME: str = MULTICAST_DOCKER_NAME


class K8sMulticast(K8sInterface):
    """Kubernetes Supervisor wrapper for HA Multicast plugin."""

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return MULTICAST_K8S_NAME

    @property
    def image(self) -> str:
        """Return name of HA Multicast image."""
        return self.sys_plugins.multicast.image

    @property
    def version(self) -> AwesomeVersion | None:
        """Return version of HA Multicast image."""
        return self.sys_plugins.multicast.version

    @Job(
        name="k8s_multicast_run",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Run Kubernetes workload for Multicast plugin."""
        version = self.sys_plugins.multicast.version

        # Multicast requires host networking for mDNS/IGMP to work correctly.
        await self._run(
            image=self.image,
            tag=str(version),
            env={ENV_TIME: self.sys_timezone},
            extra_pod_spec={"hostNetwork": True},
        )
        _LOGGER.info(
            "Starting Multicast %s with version %s in Kubernetes",
            self.image,
            version,
        )
