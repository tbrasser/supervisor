"""Kubernetes workload definition for HA Observer plugin.

Version 1 – any K8s cluster, single namespace
  No observer *container* is deployed.  Instead a Kubernetes Service is
  created that selects the ``homeassistant`` Deployment's Pods and exposes
  HA Core on the observer port.  Kubernetes handles the routing natively,
  making the proxy container redundant.

Version 2 – haos-kairos (future)
  Could be extended with a proper Ingress resource and TLS termination
  instead of the simple ClusterIP Service created here.
"""

from __future__ import annotations

from collections.abc import Awaitable
import contextlib
import logging
from typing import Any

from awesomeversion import AwesomeVersion

from ..const import OBSERVER_DOCKER_NAME, OBSERVER_PORT
from ..docker.const import ContainerState
from ..exceptions import DockerJobError
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .const import LABEL_APP
from .homeassistant import HASS_K8S_NAME
from .interface import K8sInterface
from .stats import K8sStats

_LOGGER: logging.Logger = logging.getLogger(__name__)

OBSERVER_K8S_NAME: str = OBSERVER_DOCKER_NAME


class K8sObserver(K8sInterface):
    """K8s Observer: routes traffic to HA Core via a Kubernetes Service.

    No observer container is deployed.  The Service selects the HA Core
    Deployment's Pods so Kubernetes acts as the proxy.
    """

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return OBSERVER_K8S_NAME

    @property
    def image(self) -> str | None:
        """No container image – observer is a K8s Service, not a Deployment."""
        return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @Job(
        name="k8s_observer_run",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Create (or update) the K8s Service that exposes HA Core."""
        ports: list[dict[str, Any]] = [
            {
                "port": OBSERVER_PORT,
                "targetPort": 8123,
                "protocol": "TCP",
            }
        ]
        await self.k8s.apply_service(
            self.name,
            ports,
            selector={LABEL_APP: HASS_K8S_NAME},
        )
        _LOGGER.info(
            "K8s Observer: HA Core exposed via Kubernetes Service on port %d",
            OBSERVER_PORT,
        )

    @Job(
        name="k8s_observer_stop",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def stop(self, remove: bool = True) -> None:
        """Remove the observer Service (no Deployment to scale)."""
        with contextlib.suppress(Exception):
            await self.k8s.delete_service(self.name)

    @Job(
        name="k8s_observer_start",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def start(self) -> Awaitable[None]:
        """Re-apply the observer Service."""
        return self.run()

    @Job(
        name="k8s_observer_restart",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def restart(self) -> None:
        """Re-apply the Service (idempotent; no container to restart)."""
        await self.run()

    @Job(
        name="k8s_observer_install",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def install(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """Install: create the K8s Service."""
        await self.run()

    @Job(
        name="k8s_observer_update",
        on_condition=DockerJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def update(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """Update: no container image to update, re-apply Service for idempotency."""
        await self.run()

    # ------------------------------------------------------------------
    # State queries
    # ------------------------------------------------------------------

    async def is_running(self) -> bool:
        """Return True if the observer Service exists."""
        return await self.k8s.get_service(self.name) is not None

    async def current_state(self) -> ContainerState:
        """Return RUNNING when the Service is present, else STOPPED."""
        if await self.is_running():
            return ContainerState.RUNNING
        return ContainerState.STOPPED

    async def exists(self) -> bool:
        """Return True if the observer Service exists."""
        return await self.k8s.get_service(self.name) is not None

    # ------------------------------------------------------------------
    # Stats / logs
    # ------------------------------------------------------------------

    async def stats(self) -> K8sStats:
        """No container – return empty stats."""
        return K8sStats({})

    async def logs(self) -> list[str]:
        """No container – return empty log list."""
        return []
