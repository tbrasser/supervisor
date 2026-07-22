"""Calc and represent docker stats data."""

from contextlib import suppress

from ..runtime.stats import ContainerStats


class DockerStats(ContainerStats):
    """Hold stats data from container inside."""

    def __init__(self, stats):
        """Initialize Docker stats."""
        self._cpu = 0.0
        self._network_rx = 0
        self._network_tx = 0
        self._blk_read = 0
        self._blk_write = 0

        try:
            # cgroupv1 & Docker > 19.03
            if "total_inactive_file" in stats["memory_stats"]["stats"]:
                cache = stats["memory_stats"]["stats"]["total_inactive_file"]

            # Docker <= 19.03
            elif "cache" in stats["memory_stats"]["stats"]:
                cache = stats["memory_stats"]["stats"]["cache"]

            # cgroupv2
            else:
                cache = stats["memory_stats"]["stats"]["inactive_file"]

            self._memory_usage = stats["memory_stats"]["usage"] - cache
            self._memory_limit = stats["memory_stats"]["limit"]
        except KeyError:
            self._memory_usage = 0
            self._memory_limit = 0

        # Calculate percent usage
        if self._memory_limit != 0:
            self._memory_percent = self._memory_usage / self._memory_limit * 100.0
        else:
            self._memory_percent = 0

        with suppress(KeyError):
            self._calc_cpu_percent(stats)

        with suppress(KeyError):
            self._calc_network(stats["networks"])

        with suppress(KeyError, TypeError):
            self._calc_block_io(stats["blkio_stats"])

    def _calc_cpu_percent(self, stats):
        """Calculate CPU percent."""
        cpu_delta = (
            stats["cpu_stats"]["cpu_usage"]["total_usage"]
            - stats["precpu_stats"]["cpu_usage"]["total_usage"]
        )
        system_delta = (
            stats["cpu_stats"]["system_cpu_usage"]
            - stats["precpu_stats"]["system_cpu_usage"]
        )

        if system_delta > 0.0 and cpu_delta > 0.0:
            self._cpu = (cpu_delta / system_delta) * 100.0
        else:
            self._cpu = 0.0

    def _calc_network(self, networks):
        """Calculate Network IO stats."""
        for _, stats in networks.items():
            self._network_rx += stats["rx_bytes"]
            self._network_tx += stats["tx_bytes"]

    def _calc_block_io(self, blkio):
        """Calculate block IO stats."""
        for stats in blkio["io_service_bytes_recursive"]:
            if stats["op"] == "Read":
                self._blk_read += stats["value"]
            elif stats["op"] == "Write":
                self._blk_write += stats["value"]
