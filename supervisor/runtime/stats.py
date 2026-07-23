"""Backend-neutral container stats interface.

Both container backends produce resource usage data in their own native
format (Docker stats API, Kubernetes Metrics API) and expose it through the
structural :class:`ContainerStats` protocol defined here, so consumers don't
need to know which backend produced the data.

The Docker backend keeps its own upstream implementation
(:class:`supervisor.docker.stats.DockerStats`); it satisfies this protocol
structurally without inheriting from it.
"""

from typing import Protocol


class ContainerStats(Protocol):
    """Structural interface for normalized container resource usage stats."""

    @property
    def cpu_percent(self) -> float:
        """Return CPU percent (fraction of one core × 100)."""

    @property
    def memory_usage(self) -> int:
        """Return memory usage in bytes."""

    @property
    def memory_limit(self) -> int:
        """Return memory limit in bytes (0 if unknown)."""

    @property
    def memory_percent(self) -> float:
        """Return memory usage in percent."""

    @property
    def network_rx(self) -> int:
        """Return network receive bytes (0 if not available)."""

    @property
    def network_tx(self) -> int:
        """Return network transmit bytes (0 if not available)."""

    @property
    def blk_read(self) -> int:
        """Return block IO read bytes (0 if not available)."""

    @property
    def blk_write(self) -> int:
        """Return block IO write bytes (0 if not available)."""
