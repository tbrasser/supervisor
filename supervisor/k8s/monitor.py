"""Kubernetes workload monitor for Supervisor.

This module watches the Kubernetes Pod event stream inside the Supervisor-managed
namespace and converts Pod phase / condition changes into the same
:class:`~supervisor.docker.monitor.ContainerStateEvent` events that the
rest of Supervisor already listens on.  This makes it possible for higher-level
components (add-ons, Home Assistant, plugins) to react to container-lifecycle
events without knowing whether the underlying runtime is Docker or Kubernetes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
from time import time
from typing import Any

from kubernetes_asyncio import client, watch

from ..const import BusEvent
from ..coresys import CoreSys, CoreSysAttributes
from ..docker.const import ContainerState
from ..docker.monitor import ContainerStateEvent
from ..exceptions import HassioError
from ..utils.sentry import async_capture_exception, capture_exception
from .const import K8S_NAMESPACE, LABEL_MANAGED

_LOGGER: logging.Logger = logging.getLogger(__name__)

STOP_MONITOR_TIMEOUT = 10.0


@dataclass(slots=True, frozen=True)
class K8sEventCallbackTask:
    """Kubernetes event paired with the asyncio task spawned for it."""

    data: ContainerStateEvent
    task: asyncio.Task


def _pod_phase_to_container_state(
    pod: dict[str, Any],
) -> tuple[ContainerState | None, int | None]:
    """Map a Pod status object to a :class:`ContainerState` and optional exit code.

    Returns ``(None, None)`` when the phase does not map to a meaningful state
    change (e.g. ``Pending`` or ``Unknown``).
    """
    status: dict[str, Any] = pod.get("status", {})
    phase: str = status.get("phase", "Unknown")

    if phase == "Running":
        # Check liveness/readiness conditions for health state.
        conditions: list[dict[str, Any]] = status.get("conditions", [])
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in conditions
        )
        return (ContainerState.HEALTHY if ready else ContainerState.RUNNING), None

    if phase == "Succeeded":
        return ContainerState.STOPPED, None

    if phase == "Failed":
        # Try to extract the exit code from the first container status.
        exit_code: int | None = None
        for cs in status.get("containerStatuses", []):
            terminated = cs.get("state", {}).get("terminated", {})
            if terminated:
                exit_code = terminated.get("exitCode")
                break
        return ContainerState.FAILED, exit_code

    return None, None


class K8sMonitor(CoreSysAttributes):
    """Monitor Kubernetes Pod events and translate them to Supervisor bus events."""

    def __init__(self, coresys: CoreSys, core_v1: client.CoreV1Api) -> None:
        """Initialize the Kubernetes monitor."""
        super().__init__()
        self.coresys = coresys
        self._core_v1 = core_v1
        self._monitor_task: asyncio.Task | None = None
        self._await_task: asyncio.Task | None = None
        self._event_tasks: asyncio.Queue[K8sEventCallbackTask | None]
        self._stop_event: asyncio.Event = asyncio.Event()

    async def load(self) -> None:
        """Start the Kubernetes Pod event monitor."""
        self._stop_event.clear()
        self._event_tasks = asyncio.Queue()
        self._monitor_task = self.sys_create_task(self._run(), eager_start=True)
        self._await_task = self.sys_create_task(
            self._await_event_tasks(), eager_start=True
        )
        _LOGGER.info("Started Kubernetes Pod monitor for namespace '%s'", K8S_NAMESPACE)

    async def unload(self) -> None:
        """Stop the Kubernetes Pod event monitor."""
        self._stop_event.set()

        tasks = [task for task in (self._monitor_task, self._await_task) if task]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=STOP_MONITOR_TIMEOUT)
            if pending:
                _LOGGER.warning(
                    "Timeout stopping Kubernetes monitor, cancelling %d pending task(s)",
                    len(pending),
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            self._event_tasks.shutdown(immediate=True)
            self._monitor_task = None
            self._await_task = None

        _LOGGER.info("Stopped Kubernetes Pod monitor")

    async def _run(self) -> None:
        """Watch Pod events and dispatch state-change bus events."""
        watcher = watch.Watch()
        label_selector = f"{LABEL_MANAGED}=true"

        try:
            async with watcher.stream(
                self._core_v1.list_namespaced_pod,
                namespace=K8S_NAMESPACE,
                label_selector=label_selector,
            ) as stream:
                async for event in stream:
                    if self._stop_event.is_set():
                        break
                    try:
                        await self._handle_pod_event(event)
                    except Exception as err:  # pylint: disable=broad-exception-caught
                        await async_capture_exception(err)
                        _LOGGER.error(
                            "Could not process Kubernetes Pod event, state may be inaccurate: %s",
                            err,
                        )
        except Exception as err:  # pylint: disable=broad-exception-caught
            if not self._stop_event.is_set():
                await async_capture_exception(err)
                _LOGGER.error(
                    "Kubernetes Pod monitor crashed, state information will be inaccurate: %s",
                    err,
                )
        finally:
            await self._event_tasks.put(None)

    async def _handle_pod_event(self, event: dict[str, Any]) -> None:
        """Process a single Pod watch event."""
        event_type: str = event.get("type", "")
        pod: dict[str, Any] = event.get("object", {})

        if event_type not in ("ADDED", "MODIFIED"):
            return

        # Derive the workload name from the pod's app label.
        labels: dict[str, str] = pod.get("metadata", {}).get("labels", {})
        pod_name: str = pod.get("metadata", {}).get("name", "")
        pod_uid: str = pod.get("metadata", {}).get("uid", pod_name)
        app_name: str = labels.get("supervisor.home-assistant.io/app", "")

        if not app_name:
            return

        container_state, exit_code = _pod_phase_to_container_state(pod)
        if container_state is None:
            return

        state_event = ContainerStateEvent(
            name=app_name,
            state=container_state,
            id=pod_uid,
            time=int(time()),
            exit_code=exit_code,
        )
        tasks = self.sys_bus.fire_event(BusEvent.CONTAINER_STATE_CHANGE, state_event)
        await asyncio.gather(
            *[
                self._event_tasks.put(K8sEventCallbackTask(state_event, task))
                for task in tasks
            ]
        )

    async def _await_event_tasks(self) -> None:
        """Await event callback tasks and log unhandled errors."""
        while (event := await self._event_tasks.get()) is not None:
            try:
                await event.task
            except HassioError:
                pass
            except Exception as err:  # pylint: disable=broad-exception-caught
                capture_exception(err)
                _LOGGER.error(
                    "Error processing Kubernetes Pod state event: %s %s %s",
                    event.task.get_name(),
                    event.data,
                    err,
                )
