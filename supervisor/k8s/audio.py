"""Kubernetes Audio: no-op for Wyoming / Snapcast environments.

Version 1 – any K8s cluster, single namespace
  Host-level hardware access (USB audio, Bluetooth, ALSA) is not available
  in a generic Kubernetes environment.  Audio is handled entirely through
  network-based protocols:

  - **Wyoming** – voice-assistant pipelines (STT / TTS / wake-word detection)
    over the Wyoming protocol, using network-connected satellites.
  - **Snapcast / sendspin** – multi-room audio streaming over the network.

  No PulseAudio plugin container is deployed.

Version 2 – haos-kairos (future)
  Kairos / K3s with per-app namespaces and host resource management will
  restore direct hardware audio access via privileged DaemonSets or K8s
  device plugins, at which point this class can be promoted to a real
  Deployment.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable

from awesomeversion import AwesomeVersion

from ..const import AUDIO_DOCKER_NAME
from ..docker.const import ContainerState
from ..exceptions import DockerJobError
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .interface import K8sInterface
from .stats import K8sStats

_LOGGER: logging.Logger = logging.getLogger(__name__)

AUDIO_K8S_NAME: str = AUDIO_DOCKER_NAME


class K8sAudio(K8sInterface):
    """K8s Audio: no-op — audio handled by Wyoming / Snapcast externally (V1).

    No PulseAudio container is deployed.  Voice pipelines use Wyoming and
    multi-room audio uses Snapcast / sendspin over the network instead.
    """

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return AUDIO_K8S_NAME

    @property
    def image(self) -> str | None:
        """No container image – audio is handled by external services."""
        return None

    # ------------------------------------------------------------------
    # Lifecycle (all no-ops)
    # ------------------------------------------------------------------

    @Job(
        name="k8s_audio_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """No-op: audio is handled by Wyoming / Snapcast (V1)."""
        _LOGGER.info(
            "K8s Audio (V1): PulseAudio plugin skipped — "
            "audio handled externally via Wyoming / Snapcast"
        )

    @Job(
        name="k8s_audio_stop",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def stop(self, remove: bool = True) -> None:
        """No-op: nothing to stop."""

    @Job(
        name="k8s_audio_start",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def start(self) -> Awaitable[None]:
        """No-op: audio services are external."""
        return self.run()

    @Job(
        name="k8s_audio_restart",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def restart(self) -> None:
        """No-op: nothing to restart."""

    @Job(
        name="k8s_audio_install",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def install(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """No-op: no container to install."""

    @Job(
        name="k8s_audio_update",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def update(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """No-op: no container image to update."""

    # ------------------------------------------------------------------
    # State queries
    # ------------------------------------------------------------------

    async def is_running(self) -> bool:
        """Return True – audio services are external and always available."""
        return True

    async def current_state(self) -> ContainerState:
        """Return RUNNING – audio is handled by external services."""
        return ContainerState.RUNNING

    async def exists(self) -> bool:
        """Return True – external audio services always exist."""
        return True

    # ------------------------------------------------------------------
    # Stats / logs
    # ------------------------------------------------------------------

    async def stats(self) -> K8sStats:
        """No container – return empty stats."""
        return K8sStats({})

    async def logs(self) -> list[str]:
        """No container – return empty log list."""
        return []
