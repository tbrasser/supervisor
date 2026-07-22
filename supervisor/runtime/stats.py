"""Backend-neutral container stats representation.

Both container backends produce resource usage data in their own native
format (Docker stats API, Kubernetes Metrics API).  They normalize it into
the :class:`ContainerStats` interface defined here so consumers don't need
to know which backend produced the data.
"""


class ContainerStats:
    """Hold normalized container resource usage stats.

    Subclasses populate the protected attributes from backend-specific data
    in their ``__init__``.
    """

    _cpu: float = 0.0
    _memory_usage: int = 0
    _memory_limit: int = 0
    _memory_percent: float = 0.0
    _network_rx: int = 0
    _network_tx: int = 0
    _blk_read: int = 0
    _blk_write: int = 0

    @property
    def cpu_percent(self) -> float:
        """Return CPU percent (fraction of one core × 100)."""
        return round(self._cpu, 2)

    @property
    def memory_usage(self) -> int:
        """Return memory usage in bytes."""
        return self._memory_usage

    @property
    def memory_limit(self) -> int:
        """Return memory limit in bytes (0 if unknown)."""
        return self._memory_limit

    @property
    def memory_percent(self) -> float:
        """Return memory usage in percent."""
        return round(self._memory_percent, 2)

    @property
    def network_rx(self) -> int:
        """Return network receive bytes (0 if not available)."""
        return self._network_rx

    @property
    def network_tx(self) -> int:
        """Return network transmit bytes (0 if not available)."""
        return self._network_tx

    @property
    def blk_read(self) -> int:
        """Return block IO read bytes (0 if not available)."""
        return self._blk_read

    @property
    def blk_write(self) -> int:
        """Return block IO write bytes (0 if not available)."""
        return self._blk_write
