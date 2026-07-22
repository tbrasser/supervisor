"""Kubernetes pod resource/stats representation.

The Kubernetes Metrics API returns resource usage in the following format:
  - CPU: milli-cores string (e.g. "125m") or nano-cores string (e.g. "125000000n")
  - Memory: bytes with SI suffix (e.g. "64Mi", "1Gi")

This module parses those values and exposes them with the same interface as
:class:`supervisor.docker.stats.DockerStats` so the rest of Supervisor can
consume resource data from either backend without modification.
"""

from __future__ import annotations

import re

from ..runtime.stats import ContainerStats

_CPU_MILLI_RE = re.compile(r"^(\d+)m$")
_CPU_NANO_RE = re.compile(r"^(\d+)n$")
_MEM_SUFFIX: dict[str, int] = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "E": 1000**6,
}


def _parse_cpu(value: str) -> float:
    """Return CPU usage as a fraction of one full core (0.0–N.0)."""
    if m := _CPU_MILLI_RE.match(value):
        return int(m.group(1)) / 1000.0
    if m := _CPU_NANO_RE.match(value):
        return int(m.group(1)) / 1_000_000_000.0
    # Plain integer → cores
    try:
        return float(value)
    except ValueError:
        return 0.0


def _parse_memory(value: str) -> int:
    """Return memory usage in bytes."""
    for suffix, multiplier in _MEM_SUFFIX.items():
        if value.endswith(suffix):
            try:
                return int(value[: -len(suffix)]) * multiplier
            except ValueError:
                return 0
    try:
        return int(value)
    except ValueError:
        return 0


class K8sStats(ContainerStats):
    """Hold stats data sourced from the Kubernetes Metrics API.

    Exposes the same properties as :class:`~supervisor.docker.stats.DockerStats`
    so callers don't need to know which backend produced the data.

    The *metrics* dict is a single container entry from the Pod metrics
    response, e.g.:

    .. code-block:: python

        {
            "name": "homeassistant",
            "usage": {
                "cpu": "125m",
                "memory": "256Mi",
            },
        }

    The *limits* dict is the optional resource limits spec from the container
    definition:

    .. code-block:: python

        {
            "cpu": "1",
            "memory": "512Mi",
        }
    """

    def __init__(
        self,
        metrics: dict,
        limits: dict | None = None,
    ) -> None:
        """Initialize K8s stats from Metrics API data."""
        usage = metrics.get("usage", {})

        # CPU
        cpu_cores = _parse_cpu(usage.get("cpu", "0"))
        # Convert to percent of a single core (matches Docker's per-core percent)
        self._cpu = cpu_cores * 100.0

        # Memory
        self._memory_usage = _parse_memory(usage.get("memory", "0"))

        if limits:
            self._memory_limit = _parse_memory(limits.get("memory", "0"))
        else:
            self._memory_limit = 0

        if self._memory_limit:
            self._memory_percent = self._memory_usage / self._memory_limit * 100.0
        else:
            self._memory_percent = 0.0

        # Network and block I/O are not available from the Metrics API.
        self._network_rx = 0
        self._network_tx = 0
        self._blk_read = 0
        self._blk_write = 0
