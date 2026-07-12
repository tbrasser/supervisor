"""Kubernetes workload definition for HA Audio plugin."""

import logging
from typing import Any

from ..const import AUDIO_DOCKER_NAME
from ..docker.const import ENV_TIME, PATH_PRIVATE_DATA
from ..exceptions import DockerJobError
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .interface import K8sInterface

_LOGGER: logging.Logger = logging.getLogger(__name__)

AUDIO_K8S_NAME: str = AUDIO_DOCKER_NAME


class K8sAudio(K8sInterface):
    """Kubernetes Supervisor wrapper for HA Audio plugin."""

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return AUDIO_K8S_NAME

    @property
    def image(self) -> str:
        """Return name of HA Audio image."""
        return self.sys_plugins.audio.image

    @property
    def version(self) -> str | None:
        """Return version of HA Audio image."""
        return self.sys_plugins.audio.version

    @Job(
        name="k8s_audio_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Run Kubernetes workload for Audio plugin."""
        version = self.sys_plugins.audio.version

        volume_mounts: list[dict[str, Any]] = [
            {
                "name": "audio-data",
                "mountPath": PATH_PRIVATE_DATA.as_posix(),
                "readOnly": False,
            },
        ]
        volumes: list[dict[str, Any]] = [
            {
                "name": "audio-data",
                "hostPath": {
                    "path": self.sys_config.path_extern_audio.as_posix(),
                    "type": "DirectoryOrCreate",
                },
            },
        ]

        # Audio requires elevated privileges for hardware / PulseAudio access.
        security_context: dict[str, Any] = {
            "privileged": True,
        }

        await self._run(
            image=self.image,
            tag=str(version),
            env={ENV_TIME: self.sys_timezone},
            mounts=volume_mounts,
            security_context=security_context,
            extra_pod_spec={"volumes": volumes},
        )
        _LOGGER.info(
            "Starting Audio %s with version %s in Kubernetes",
            self.image,
            version,
        )
