"""Kubernetes workload definition for HA Observer plugin."""

import logging
from typing import Any

from ..const import OBSERVER_DOCKER_NAME, OBSERVER_PORT
from ..docker.const import ENV_TIME, ENV_TOKEN
from ..exceptions import DockerJobError
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

OBSERVER_K8S_NAME: str = OBSERVER_DOCKER_NAME


class K8sObserver(K8sInterface):
    """Kubernetes Supervisor wrapper for HA Observer plugin."""

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return OBSERVER_K8S_NAME

    @property
    def image(self) -> str:
        """Return name of HA Observer image."""
        return self.sys_plugins.observer.image

    @property
    def version(self) -> str | None:
        """Return version of HA Observer image."""
        return self.sys_plugins.observer.version

    @Job(
        name="k8s_observer_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Run Kubernetes workload for Observer plugin."""
        version = self.sys_plugins.observer.version

        ports: list[dict[str, Any]] = [
            {
                "containerPort": 80,
                "hostPort": OBSERVER_PORT,
                "protocol": "TCP",
            },
        ]

        await self._run(
            image=self.image,
            tag=str(version),
            env={
                ENV_TIME: self.sys_timezone,
                ENV_TOKEN: self.sys_plugins.observer.supervisor_token,
            },
            ports=ports,
        )
        _LOGGER.info(
            "Starting Observer %s with version %s in Kubernetes",
            self.image,
            version,
        )
