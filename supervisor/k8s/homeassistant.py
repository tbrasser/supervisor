"""Kubernetes workload definition for Home Assistant Core.

This is the Kubernetes equivalent of
:class:`supervisor.docker.homeassistant.DockerHomeAssistant`.

It manages a single-replica ``homeassistant`` Deployment inside the
``home-assistant`` namespace using the same image and environment variables as
the Docker variant so that Home Assistant Core runs identically under both
backends.
"""

from __future__ import annotations

import logging
from typing import Any

from awesomeversion import AwesomeVersion

from ..docker.const import (
    ENV_CORE_API_SOCKET,
    ENV_DUPLICATE_LOG_FILE,
    ENV_TIME,
    ENV_TOKEN,
    ENV_TOKEN_OLD,
    PATH_MEDIA,
    PATH_PUBLIC_CONFIG,
    PATH_SHARE,
    PATH_SSL,
)
from ..docker.manager import CommandReturn
from ..exceptions import DockerJobError
from ..homeassistant.const import LANDINGPAGE
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

# Kubernetes workload / Deployment name for Home Assistant Core.
HASS_K8S_NAME: str = "homeassistant"

# Environment variable injected when a restore job is active.
ENV_RESTORE_JOB_ID = "SUPERVISOR_RESTORE_JOB_ID"

# Termination grace period mirrors the Docker backend default.
_TERMINATION_GRACE_PERIOD = 260


class K8sHomeAssistant(K8sInterface):
    """Kubernetes Supervisor workload for Home Assistant Core."""

    @property
    def name(self) -> str:
        """Return the Kubernetes workload name."""
        return HASS_K8S_NAME

    @property
    def timeout(self) -> int:
        """Return the graceful shutdown timeout in seconds."""
        return _TERMINATION_GRACE_PERIOD

    @property
    def image(self) -> str:
        """Return the Home Assistant container image repository."""
        return self.sys_homeassistant.image

    @property
    def version(self) -> AwesomeVersion | None:
        """Return the current Home Assistant version."""
        return self.sys_homeassistant.version

    # ------------------------------------------------------------------
    # Volume mounts
    # ------------------------------------------------------------------

    def _volume_mounts(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Build Kubernetes volumeMounts and volumes lists.

        Returns a ``(volumeMounts, volumes)`` tuple so they can be injected
        into the Pod spec separately.
        """
        volume_mounts: list[dict[str, Any]] = [
            {
                "name": "config",
                "mountPath": PATH_PUBLIC_CONFIG.as_posix(),
                "readOnly": False,
            },
        ]
        volumes: list[dict[str, Any]] = [
            {
                "name": "config",
                "hostPath": {
                    "path": self.sys_config.path_extern_homeassistant.as_posix(),
                    "type": "DirectoryOrCreate",
                },
            },
        ]

        if self.sys_homeassistant.version != LANDINGPAGE:
            volume_mounts += [
                {
                    "name": "ssl",
                    "mountPath": PATH_SSL.as_posix(),
                    "readOnly": True,
                },
                {
                    "name": "share",
                    "mountPath": PATH_SHARE.as_posix(),
                    "readOnly": False,
                },
                {
                    "name": "media",
                    "mountPath": PATH_MEDIA.as_posix(),
                    "readOnly": False,
                },
            ]
            volumes += [
                {
                    "name": "ssl",
                    "hostPath": {
                        "path": self.sys_config.path_extern_ssl.as_posix(),
                        "type": "DirectoryOrCreate",
                    },
                },
                {
                    "name": "share",
                    "hostPath": {
                        "path": self.sys_config.path_extern_share.as_posix(),
                        "type": "DirectoryOrCreate",
                    },
                },
                {
                    "name": "media",
                    "hostPath": {
                        "path": self.sys_config.path_extern_media.as_posix(),
                        "type": "DirectoryOrCreate",
                    },
                },
            ]

        if self.sys_homeassistant.api.supports_unix_socket:
            volume_mounts.append(
                {
                    "name": "supervisor-run",
                    "mountPath": "/run/supervisor",
                    "readOnly": False,
                }
            )
            volumes.append(
                {
                    "name": "supervisor-run",
                    "hostPath": {
                        "path": "/run/supervisor",
                        "type": "DirectoryOrCreate",
                    },
                }
            )

        return volume_mounts, volumes

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    @Job(
        name="k8s_home_assistant_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self, *, restore_job_id: str | None = None) -> None:
        """Apply the Home Assistant Deployment to the cluster."""
        version = self.sys_homeassistant.version
        if not version:
            raise ValueError("Home Assistant version is not set, cannot run!")

        environment: dict[str, str | None] = {
            ENV_TIME: self.sys_timezone,
            ENV_TOKEN: self.sys_homeassistant.supervisor_token,
            ENV_TOKEN_OLD: self.sys_homeassistant.supervisor_token,
        }
        if restore_job_id:
            environment[ENV_RESTORE_JOB_ID] = restore_job_id
        if self.sys_homeassistant.api.supports_unix_socket:
            environment[ENV_CORE_API_SOCKET] = "/run/supervisor/core.sock"
        if self.sys_homeassistant.duplicate_log_file:
            environment[ENV_DUPLICATE_LOG_FILE] = "1"

        volume_mounts, volumes = self._volume_mounts()

        # Home Assistant Core needs elevated privileges for hardware access.
        security_context: dict[str, Any] = {}
        if version != LANDINGPAGE:
            security_context = {
                "privileged": True,
            }

        extra_spec: dict[str, Any] = {
            "hostNetwork": True,
            "volumes": volumes,
            "terminationGracePeriodSeconds": _TERMINATION_GRACE_PERIOD,
        }

        await self._run(
            image=self.image,
            tag=str(version),
            env=environment,
            mounts=volume_mounts,
            security_context=security_context,
            extra_pod_spec=extra_spec,
        )
        _LOGGER.info(
            "Started Home Assistant %s with version %s in Kubernetes",
            self.image,
            version,
        )

    # ------------------------------------------------------------------
    # Execute command (run a one-shot Job for migration / check tasks)
    # ------------------------------------------------------------------

    @Job(
        name="k8s_home_assistant_execute_command",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def execute_command(self, command: list[str]) -> CommandReturn:
        """Run *command* as a Kubernetes Job using the Home Assistant image."""
        return await self.k8s.run_command(
            self.image,
            command=command,
            tag=str(self.sys_homeassistant.version or "latest"),
        )

    # ------------------------------------------------------------------
    # Initialisation check
    # ------------------------------------------------------------------

    async def is_initialize(self) -> bool:
        """Return True if the Home Assistant Deployment exists and is ready."""
        if not self.sys_homeassistant.version:
            return False
        deployment = await self.k8s.get_deployment(self.name)
        if deployment is None:
            return False
        ready = deployment.get("status", {}).get("readyReplicas", 0) or 0
        return ready > 0
