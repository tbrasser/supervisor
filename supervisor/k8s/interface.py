"""Abstract Kubernetes workload interface for Supervisor.

This is the Kubernetes equivalent of
:class:`supervisor.docker.interface.DockerInterface`.  Each managed component
(Home Assistant Core, add-ons, plugins) subclasses :class:`K8sInterface` and
overrides :meth:`run` to provide the component-specific Deployment parameters.

The interface deliberately mirrors the public API of :class:`DockerInterface`
so that higher-level components in Supervisor can be made backend-agnostic with
minimal changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable
import contextlib
import logging

from awesomeversion import AwesomeVersion

from ..coresys import CoreSys
from ..docker.const import ContainerState
from ..docker.manager import CommandReturn, ExecReturn
from ..jobs.const import JOB_GROUP_DOCKER_INTERFACE, JobConcurrency
from ..jobs.decorator import Job
from ..jobs.job_group import JobGroup
from .exceptions import K8sError, K8sJobError, K8sNotFound
from .manager import K8sAPI
from .stats import K8sStats

_LOGGER: logging.Logger = logging.getLogger(__name__)


class K8sInterface(JobGroup, ABC):
    """Abstract Kubernetes workload interface.

    Subclasses must implement :meth:`run` to apply the appropriate Deployment
    for the component they manage.
    """

    def __init__(self, coresys: CoreSys) -> None:
        """Initialize K8s workload interface."""
        super().__init__(
            coresys,
            JOB_GROUP_DOCKER_INTERFACE.format(name=self.name or "unknown"),
            self.name,
        )
        self.coresys: CoreSys = coresys

    # ------------------------------------------------------------------
    # Properties to override
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the workload name (used as Deployment / Service name)."""

    @property
    def timeout(self) -> int:
        """Return the graceful shutdown timeout in seconds."""
        return 10

    @property
    def image(self) -> str | None:
        """Return the container image repository (without tag)."""
        return None

    @property
    def version(self) -> AwesomeVersion | None:
        """Return the current image version from the Deployment annotation."""
        # Synchronous read is not possible here; version is cached by callers.
        return None

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def k8s(self) -> K8sAPI:
        """Return K8s manager."""
        # The k8s manager is stored on coresys just like the docker manager.
        # During the transition period it may live under coresys.k8s.
        return self.coresys.k8s  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Core lifecycle methods (mirror DockerInterface API)
    # ------------------------------------------------------------------

    @Job(
        name="k8s_interface_run",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Apply / start the Deployment for this workload.

        Subclasses must override this method and call
        :meth:`_run` with the appropriate parameters.
        """
        raise NotImplementedError

    async def _run(
        self,
        *,
        image: str | None = None,
        tag: str = "latest",
        **kwargs,
    ) -> None:
        """Apply the Deployment and wait until at least one Pod is scheduled.

        Parameters
        ----------
        image:
            Container image repository.  Falls back to :attr:`image`.
        tag:
            Image tag.  Falls back to ``str(self.version)`` then ``"latest"``.
        **kwargs:
            Additional keyword arguments forwarded to
            :meth:`~K8sAPI.apply_deployment`.

        """
        use_image = image or self.image
        if not use_image:
            raise ValueError(f"Cannot determine image to use for '{self.name}'!")

        use_tag = tag
        if use_tag == "latest" and self.version:
            use_tag = str(self.version)

        if await self.is_running():
            return

        # Remove any existing stopped workload first.
        await self.stop(remove=False)

        await self.k8s.apply_deployment(self.name, use_image, use_tag, **kwargs)
        _LOGGER.info(
            "Applied Deployment '%s' with image %s:%s", self.name, use_image, use_tag
        )

    @Job(
        name="k8s_interface_stop",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def stop(self, remove: bool = True) -> None:
        """Stop (and optionally delete) the Deployment for this workload.

        When *remove* is ``False`` the Deployment is scaled to 0 instead of
        being deleted, which preserves the spec for a future :meth:`start`.
        """
        if remove:
            try:
                await self.k8s.delete_deployment(self.name)
                await self.k8s.delete_service(self.name)
            except K8sNotFound:
                pass
        else:
            with contextlib.suppress(K8sNotFound):
                await self.k8s.scale_deployment(self.name, replicas=0)

    @Job(
        name="k8s_interface_start",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def start(self) -> Awaitable[None]:
        """Scale the Deployment back to 1 replica."""
        return self.k8s.scale_deployment(self.name, replicas=1)

    @Job(
        name="k8s_interface_restart",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def restart(self) -> Awaitable[None]:
        """Trigger a rollout restart of the Deployment."""
        return self.k8s.restart_deployment(self.name)

    @Job(
        name="k8s_interface_remove",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def remove(self) -> None:
        """Delete the Deployment and its associated Service."""
        with contextlib.suppress(K8sNotFound):
            await self.k8s.delete_deployment(self.name)
        with contextlib.suppress(K8sNotFound):
            await self.k8s.delete_service(self.name)

    @Job(
        name="k8s_interface_update",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def update(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """Update the Deployment to a new image version.

        Stops the current workload and re-applies with the new image tag.
        The *latest* parameter is accepted for API compatibility but ignored
        (Kubernetes image tags are always explicit).
        """
        use_image = image or self.image
        _LOGGER.info(
            "Updating workload '%s' from %s to %s:%s",
            self.name,
            self.version,
            use_image,
            version,
        )
        await self.k8s.apply_deployment(self.name, use_image or "", str(version))

    @Job(
        name="k8s_interface_install",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def install(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """Install (first-time deploy) a workload.

        In the Kubernetes backend "installing" means applying the Deployment
        so Kubernetes can pull and start the image.  There is no separate image
        pull step.
        """
        use_image = image or self.image
        if not use_image:
            raise ValueError(f"Cannot install '{self.name}' without an image!")
        _LOGGER.info(
            "Installing workload '%s' with image %s:%s", self.name, use_image, version
        )
        await self.k8s.apply_deployment(self.name, use_image, str(version))

    # ------------------------------------------------------------------
    # State queries
    # ------------------------------------------------------------------

    async def exists(self) -> bool:
        """Return ``True`` if the Deployment exists in the cluster."""
        return await self.k8s.get_deployment(self.name) is not None

    async def is_running(self) -> bool:
        """Return ``True`` if at least one Pod is in the Running phase."""
        pods = await self.k8s.get_pods_for_deployment(self.name)
        return any(p.get("status", {}).get("phase") == "Running" for p in pods)

    async def is_failed(self) -> bool:
        """Return ``True`` if the workload is in a failed state."""
        return await self.current_state() == ContainerState.FAILED

    async def current_state(self) -> ContainerState:
        """Return the current :class:`~supervisor.docker.const.ContainerState`."""
        pods = await self.k8s.get_pods_for_deployment(self.name)
        if not pods:
            return ContainerState.UNKNOWN

        # Use the phase of the most recently created Pod.
        pods.sort(
            key=lambda p: p.get("metadata", {}).get("creationTimestamp") or "",
            reverse=True,
        )
        phase = pods[0].get("status", {}).get("phase", "Unknown")
        if phase == "Running":
            conditions = pods[0].get("status", {}).get("conditions", [])
            ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            )
            return ContainerState.HEALTHY if ready else ContainerState.RUNNING
        if phase == "Succeeded":
            return ContainerState.STOPPED
        if phase == "Failed":
            return ContainerState.FAILED
        return ContainerState.UNKNOWN

    # ------------------------------------------------------------------
    # Logs / stats / exec
    # ------------------------------------------------------------------

    async def logs(self) -> list[str]:
        """Return the last 100 log lines for this workload."""
        try:
            return await self.k8s.pod_logs(self.name)
        except K8sError:
            return []

    async def stats(self) -> K8sStats:
        """Return resource usage stats for this workload."""
        return await self.k8s.pod_stats(self.name)

    @Job(
        name="k8s_interface_execute_command",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def execute_command(self, command: list[str]) -> CommandReturn:
        """Execute *command* inside the running Pod and return its output."""
        raise NotImplementedError

    @Job(
        name="k8s_interface_run_inside",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    def run_inside(self, command: str) -> Awaitable[ExecReturn]:
        """Execute a command inside the running Pod."""
        return self.k8s.pod_exec(self.name, command)

    # ------------------------------------------------------------------
    # Cleanup (no-op on k8s – images are managed by the registry)
    # ------------------------------------------------------------------

    @Job(name="k8s_interface_cleanup", concurrency=JobConcurrency.GROUP_QUEUE)
    async def cleanup(
        self,
        old_image: str | None = None,
        image: str | None = None,
        version: AwesomeVersion | None = None,
    ) -> None:
        """No-op on Kubernetes: image lifecycle is managed by the registry.

        Accepted for API compatibility with :class:`~supervisor.docker.interface.DockerInterface`.
        """
