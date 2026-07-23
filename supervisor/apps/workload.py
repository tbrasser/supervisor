"""Backend-neutral workload composition for apps.

Both container backends compose the same app folder mappings and environment
variables; only the native format differs (Docker bind mounts vs Kubernetes
hostPath volumes).  The mapping logic is owned by the Docker backend
(:class:`~supervisor.docker.app.DockerApp`) to stay close to upstream; this
module delegates to it and translates the result into a backend-neutral spec
that other backends convert into their own mount/env format.

Backend-specific extras (hardware, audio, D-Bus, journald mounts) remain in
the Docker backend; apps requiring them are rejected on Kubernetes.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import TYPE_CHECKING

from ..docker.app import DockerApp
from ..docker.const import PropagationMode

if TYPE_CHECKING:
    from .app import App

_NAME_SANITIZE_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True, frozen=True)
class WorkloadMount:
    """Backend-neutral bind mount of a host folder into an app workload."""

    name: str
    source: str
    target: str
    read_only: bool
    propagation: PropagationMode | None = None


def _mount_name(target: str) -> str:
    """Derive a DNS-1123 compatible volume name from the mount target."""
    name = _NAME_SANITIZE_RE.sub("-", target.lower()).strip("-")
    return name[:63].strip("-") or "root"


def workload_folder_mounts(app: App) -> list[WorkloadMount]:
    """Resolve the app folder mappings into backend-neutral mounts.

    Delegates to :attr:`DockerApp.folder_mounts` so the mapping logic lives
    in the Docker module only.
    """
    mounts: list[WorkloadMount] = []
    seen: set[str] = set()
    for mount in DockerApp(app.coresys, app).folder_mounts:
        name = base_name = _mount_name(mount.target)
        counter = 1
        while name in seen:
            counter += 1
            name = f"{base_name[:59]}-{counter}"
        seen.add(name)
        mounts.append(
            WorkloadMount(
                name=name,
                source=mount.source,
                target=mount.target,
                read_only=mount.read_only,
                propagation=mount.bind_options.propagation
                if mount.bind_options
                else None,
            )
        )
    return mounts


def workload_environment(app: App) -> dict[str, str | int | None]:
    """Return the backend-neutral environment for an app workload.

    Delegates to :attr:`DockerApp.environment` so the environment logic
    lives in the Docker module only.
    """
    return DockerApp(app.coresys, app).environment
