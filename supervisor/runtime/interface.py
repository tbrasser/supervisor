"""Backend-neutral workload instance protocols and backend selection.

These protocols capture the exact surface that Supervisor's high-level
components use on their ``instance`` attribute, independent of whether the
workload is backed by Docker (:class:`~supervisor.docker.interface.DockerInterface`)
or Kubernetes (:class:`~supervisor.k8s.interface.K8sInterface`).

Typing components against these protocols (instead of backend unions) makes
drift between the backends a static type error, and the protocols double as
the source of truth for backend parity tests.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ipaddress import IPv4Address
    from pathlib import Path

    from awesomeversion import AwesomeVersion

    from ..const import CpuArch
    from ..coresys import CoreSys
    from ..docker.const import ContainerState
    from ..docker.manager import CommandReturn, ExecReturn
    from ..jobs import SupervisorJob
    from .stats import ContainerStats


class WorkloadInstance(Protocol):
    """Backend-neutral contract for a managed container workload.

    This is the common surface used by plugins and other simple managed
    components.  Both ``DockerInterface`` and ``K8sInterface`` (and their
    subclasses) satisfy this protocol.
    """

    @property
    def name(self) -> str:
        """Return the workload name."""

    @property
    def image(self) -> str | None:
        """Return the container image repository (without tag)."""

    @property
    def version(self) -> AwesomeVersion | None:
        """Return the current workload version."""

    @property
    def arch(self) -> str | None:
        """Return the CPU architecture of the workload image."""

    @property
    def timeout(self) -> int:
        """Return the graceful shutdown timeout in seconds."""

    @property
    def attached(self) -> bool:
        """Return True if this interface is attached to a workload."""

    @property
    def in_progress(self) -> bool:
        """Return True if a task is in progress."""

    @property
    def healthcheck(self) -> dict[str, Any] | None:
        """Return the image healthcheck configuration, if any."""

    @property
    def active_job(self) -> SupervisorJob | None:
        """Return the job group's currently active job, if any."""

    @property
    def supports_build(self) -> bool:
        """Return True if the backend can build images locally."""

    @property
    def supports_stdin(self) -> bool:
        """Return True if the backend can attach to container stdin."""

    async def run(self) -> None:
        """Run the workload."""

    async def stop(self, remove_container: bool = True) -> None:
        """Stop the workload."""

    def start(self) -> Awaitable[None]:
        """Start the workload."""

    def restart(self) -> Awaitable[None]:
        """Restart the workload."""

    async def remove(self, *, remove_image: bool = True) -> None:
        """Remove the workload."""

    async def install(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
        arch: CpuArch | None = None,
    ) -> None:
        """Install (pull or deploy) the workload image."""

    async def update(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
    ) -> None:
        """Update the workload to a new version."""

    async def attach(
        self, version: AwesomeVersion, *, skip_state_event_if_down: bool = False
    ) -> None:
        """Attach to an existing workload."""

    async def check_image(
        self,
        version: AwesomeVersion,
        expected_image: str,
        expected_cpu_arch: CpuArch | None = None,
    ) -> None:
        """Check the workload uses the expected image."""

    async def exists(self) -> bool:
        """Return True if the workload exists."""

    async def is_running(self) -> bool:
        """Return True if the workload is running."""

    async def is_failed(self) -> bool:
        """Return True if the workload is in a failed state."""

    async def current_state(self) -> ContainerState:
        """Return the current workload state."""

    async def logs(self) -> list[str]:
        """Return the workload logs."""

    async def stats(self) -> ContainerStats:
        """Return resource usage stats for the workload."""

    async def cleanup(
        self,
        old_image: str | None = None,
        image: str | None = None,
        version: AwesomeVersion | None = None,
    ) -> None:
        """Clean up old workload images."""

    async def get_latest_version(self) -> AwesomeVersion:
        """Return the latest locally available version of the workload."""


class HomeAssistantInstance(WorkloadInstance, Protocol):
    """Workload contract for the Home Assistant Core container."""

    @property
    def meta_config(self) -> dict[str, Any]:
        """Return the workload container config metadata."""

    @property
    def machine(self) -> str | None:
        """Return the machine type of the Home Assistant image, if known."""

    @property
    def ip_address(self) -> IPv4Address:
        """Return the IP address Home Assistant is reachable on."""

    async def run(self, *, restore_job_id: str | None = None) -> None:
        """Run the Home Assistant workload."""

    async def execute_command(self, command: list[str]) -> CommandReturn:
        """Execute a command in a one-off container of the Core image."""

    async def is_initialize(self) -> bool:
        """Return True if the Home Assistant workload is initialized."""


class AppInstance(WorkloadInstance, Protocol):
    """Workload contract for app containers."""

    @property
    def ip_address(self) -> IPv4Address:
        """Return the IP address the app is reachable on."""

    async def update(
        self,
        version: AwesomeVersion,
        image: str | None = None,
        latest: bool = False,
        arch: CpuArch | None = None,
    ) -> None:
        """Update the app to a new version."""

    def run_inside(self, command: str) -> Awaitable[ExecReturn]:
        """Execute a command inside the running app."""

    async def write_stdin(self, data: bytes) -> None:
        """Write data to the app's stdin."""

    async def export_image(self, tar_file: Path) -> None:
        """Export the app image to a tar file."""

    async def import_image(self, tar_file: Path) -> None:
        """Import the app image from a tar file."""


def create_instance[DockerT, K8sT](
    coresys: CoreSys,
    docker_type: Callable[..., DockerT],
    k8s_type: Callable[..., K8sT],
    *args: Any,
) -> DockerT | K8sT:
    """Return the workload instance for the active container backend.

    Instantiates *k8s_type* when the Kubernetes backend is active
    (``coresys.k8s`` is set), *docker_type* otherwise.  Extra positional
    arguments are passed to the constructor after *coresys*.
    """
    if coresys.k8s:
        return k8s_type(coresys, *args)
    return docker_type(coresys, *args)
