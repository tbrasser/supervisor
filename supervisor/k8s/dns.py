"""Kubernetes workload definition for HA DNS plugin."""

import logging
from typing import Any

from ..const import DNS_DOCKER_NAME
from ..docker.const import ENV_TIME
from ..exceptions import DockerJobError
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

DNS_K8S_NAME: str = DNS_DOCKER_NAME


class K8sDns(K8sInterface):
    """Kubernetes Supervisor wrapper for HA DNS plugin."""

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return DNS_K8S_NAME

    @property
    def image(self) -> str:
        """Return name of HA DNS image."""
        return self.sys_plugins.dns.image

    @property
    def version(self) -> str | None:
        """Return version of HA DNS image."""
        return self.sys_plugins.dns.version

    @Job(
        name="k8s_dns_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Run Kubernetes workload for DNS plugin."""
        version = self.sys_plugins.dns.version

        # Mount the DNS config directory so CoreDNS can read its configuration.
        volume_mounts: list[dict[str, Any]] = [
            {
                "name": "dns-config",
                "mountPath": "/config",
                "readOnly": False,
            },
        ]
        volumes: list[dict[str, Any]] = [
            {
                "name": "dns-config",
                "hostPath": {
                    "path": self.sys_config.path_extern_dns.as_posix(),
                    "type": "DirectoryOrCreate",
                },
            },
        ]

        await self._run(
            image=self.image,
            tag=str(version),
            env={ENV_TIME: self.sys_timezone},
            mounts=volume_mounts,
            extra_pod_spec={"volumes": volumes},
        )
        _LOGGER.info(
            "Starting DNS %s with version %s in Kubernetes",
            self.image,
            version,
        )
