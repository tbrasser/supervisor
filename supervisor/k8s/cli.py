"""Kubernetes workload definition for HA Cli."""

import logging

from awesomeversion import AwesomeVersion

from ..const import CLI_DOCKER_NAME
from ..docker.const import ENV_TIME, ENV_TOKEN
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .exceptions import K8sJobError
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

CLI_K8S_NAME: str = CLI_DOCKER_NAME


class K8sCli(K8sInterface):
    """Kubernetes Supervisor wrapper for HA cli."""

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return CLI_K8S_NAME

    @property
    def image(self) -> str:
        """Return name of HA cli image."""
        return self.sys_plugins.cli.image

    @property
    def version(self) -> AwesomeVersion | None:
        """Return version of HA cli image."""
        return self.sys_plugins.cli.version

    @Job(
        name="k8s_cli_run",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Run Kubernetes workload."""
        version = self.sys_plugins.cli.version

        await self._run(
            image=self.image,
            tag=str(version),
            command=["/init"],
            env={
                ENV_TIME: self.sys_timezone,
                ENV_TOKEN: self.sys_plugins.cli.supervisor_token,
            },
        )
        _LOGGER.info(
            "Starting CLI %s with version %s in Kubernetes",
            self.image,
            version,
        )
