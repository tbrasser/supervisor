"""Kubernetes DNS: delegates to Kubernetes native CoreDNS.

Version 1 – any K8s cluster, single namespace
  DNS resolution within the ``home-assistant`` namespace is handled
  transparently by the cluster's built-in CoreDNS.  Services are
  reachable at ``<name>.<namespace>.svc.cluster.local`` without any
  additional plugin container.  No HA DNS container is deployed.

Version 2 – haos-kairos (future)
  Custom CoreDNS ConfigMap patches can be applied here to propagate
  host-level mDNS / LLMNR entries into the cluster-wide DNS.
"""

from __future__ import annotations

from collections.abc import Awaitable
import logging

from awesomeversion import AwesomeVersion

from ..const import DNS_DOCKER_NAME
from ..docker.const import ContainerState
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .exceptions import K8sJobError
from .interface import K8sInterface
from .stats import K8sStats

_LOGGER: logging.Logger = logging.getLogger(__name__)

DNS_K8S_NAME: str = DNS_DOCKER_NAME


class K8sDns(K8sInterface):
    """K8s DNS: no-op — Kubernetes CoreDNS handles all DNS natively (V1).

    No DNS plugin container is deployed.  Internal service discovery and
    external DNS forwarding are both provided by the cluster's CoreDNS.
    """

    @property
    def name(self) -> str:
        """Return name of Kubernetes workload."""
        return DNS_K8S_NAME

    @property
    def image(self) -> str | None:
        """No container image – DNS is handled by Kubernetes CoreDNS."""
        return None

    # ------------------------------------------------------------------
    # Lifecycle (all no-ops)
    # ------------------------------------------------------------------

    @Job(
        name="k8s_dns_run",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """No-op: cluster CoreDNS is always available."""
        _LOGGER.info(
            "K8s DNS (V1): cluster-internal DNS handled by Kubernetes CoreDNS; "
            "no plugin container deployed"
        )

    @Job(
        name="k8s_dns_stop",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def stop(self, remove: bool = True) -> None:
        """No-op: nothing to stop."""

    @Job(
        name="k8s_dns_start",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def start(self) -> Awaitable[None]:
        """No-op: DNS is always running."""
        return self.run()

    @Job(
        name="k8s_dns_restart",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def restart(self) -> None:
        """No-op: nothing to restart."""

    @Job(
        name="k8s_dns_install",
        on_condition=K8sJobError,
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
        name="k8s_dns_update",
        on_condition=K8sJobError,
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
        """Return True – Kubernetes CoreDNS is always available."""
        return True

    async def current_state(self) -> ContainerState:
        """Return RUNNING – Kubernetes CoreDNS is always available."""
        return ContainerState.RUNNING

    async def exists(self) -> bool:
        """Return True – Kubernetes DNS always exists."""
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
