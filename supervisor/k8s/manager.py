"""Kubernetes API manager for Supervisor.

This is the Kubernetes equivalent of :class:`supervisor.docker.manager.DockerAPI`.
It provides the core primitives used by :class:`K8sInterface` subclasses (and
indirectly by the rest of Supervisor) to manage workloads that run as Kubernetes
Deployments inside the ``home-assistant`` namespace.

Design decisions
----------------
* **Single namespace** – all Supervisor-managed resources live in the
  ``home-assistant`` namespace so that RBAC rules can be scoped narrowly.
* **Deployment per workload** – each Supervisor component (Home Assistant Core,
  add-ons, plugins) is represented as a single-replica Deployment.  Scaling to 0
  replicas effectively "stops" the workload; scaling back to 1 "starts" it.
* **No local image storage** – Kubernetes pulls images from a registry on behalf
  of each Pod, so the Docker-side concepts of ``pull_image``, ``remove_image``,
  ``cleanup_old_images``, ``import_image`` and ``export_image`` do not apply here.
  Stubs are provided where callers might expect them.
* **One-shot commands** → Kubernetes Jobs (created, awaited, deleted in-place).
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from typing import Any, Self
from uuid import uuid4

from kubernetes_asyncio import client, config as k8s_config
from kubernetes_asyncio.client import ApiClient
from kubernetes_asyncio.stream import WsApiClient

from ..coresys import CoreSys, CoreSysAttributes
from ..docker.manager import CommandReturn, ExecReturn
from .const import (
    DEFAULT_TERMINATION_GRACE_PERIOD,
    K8S_NAMESPACE,
    LABEL_APP,
    LABEL_MANAGED,
)
from .exceptions import K8sAPIError, K8sNotFound, K8sTimeoutError
from .monitor import K8sMonitor
from .stats import K8sStats

_LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class K8sInfo:
    """Basic information about the connected Kubernetes cluster."""

    server_version: str
    platform: str = "kubernetes"


class K8sAPI(CoreSysAttributes):
    """Kubernetes Supervisor wrapper.

    Manages all Supervisor-owned Kubernetes resources inside the
    ``home-assistant`` namespace.
    """

    def __init__(self, coresys: CoreSys) -> None:
        """Initialize K8s manager."""
        self.coresys = coresys
        self._api_client: ApiClient | None = None
        self._core_v1: client.CoreV1Api | None = None
        self._apps_v1: client.AppsV1Api | None = None
        self._batch_v1: client.BatchV1Api | None = None
        self._info: K8sInfo | None = None
        self._monitor: K8sMonitor | None = None

    async def post_init(self) -> Self:
        """Connect to the Kubernetes API server and set up internal objects.

        In-cluster configuration is used when running inside a Pod; falls back
        to the kubeconfig file when running outside the cluster (dev mode).
        """
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            _LOGGER.debug(
                "Not running inside a cluster, loading kubeconfig for Kubernetes backend"
            )
            await k8s_config.load_kube_config()

        self._api_client = ApiClient()
        self._core_v1 = client.CoreV1Api(self._api_client)
        self._apps_v1 = client.AppsV1Api(self._api_client)
        self._batch_v1 = client.BatchV1Api(self._api_client)

        # Ensure the home-assistant namespace exists.
        await self._ensure_namespace()

        # Fetch cluster version for diagnostics.
        version_api = client.VersionApi(self._api_client)
        try:
            ver = await version_api.get_code()
            self._info = K8sInfo(server_version=ver.git_version)
            _LOGGER.info("Connected to Kubernetes %s", self._info.server_version)
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOGGER.warning("Could not fetch Kubernetes server version: %s", err)
            self._info = K8sInfo(server_version="unknown")

        self._monitor = K8sMonitor(self.coresys, self._core_v1)
        return self

    @property
    def core_v1(self) -> client.CoreV1Api:
        """Return CoreV1 API client."""
        if not self._core_v1:
            raise RuntimeError("Kubernetes CoreV1 API not initialized!")
        return self._core_v1

    @property
    def apps_v1(self) -> client.AppsV1Api:
        """Return AppsV1 API client."""
        if not self._apps_v1:
            raise RuntimeError("Kubernetes AppsV1 API not initialized!")
        return self._apps_v1

    @property
    def batch_v1(self) -> client.BatchV1Api:
        """Return BatchV1 API client."""
        if not self._batch_v1:
            raise RuntimeError("Kubernetes BatchV1 API not initialized!")
        return self._batch_v1

    @property
    def info(self) -> K8sInfo:
        """Return Kubernetes cluster info."""
        if not self._info:
            raise RuntimeError("Kubernetes info not initialized!")
        return self._info

    @property
    def monitor(self) -> K8sMonitor:
        """Return the Pod event monitor."""
        if not self._monitor:
            raise RuntimeError("Kubernetes monitor not initialized!")
        return self._monitor

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def load(self) -> None:
        """Start the Kubernetes event monitor."""
        await self.monitor.load()

    async def unload(self) -> None:
        """Stop the Kubernetes event monitor and close the API client."""
        await self.monitor.unload()
        if self._api_client:
            await self._api_client.close()
            self._api_client = None

    # ------------------------------------------------------------------
    # Namespace helpers
    # ------------------------------------------------------------------

    async def _ensure_namespace(self) -> None:
        """Create the home-assistant namespace if it does not already exist."""
        try:
            await self.core_v1.read_namespace(K8S_NAMESPACE)
        except client.ApiException as err:
            if err.status == 404:
                _LOGGER.info("Creating Kubernetes namespace '%s'", K8S_NAMESPACE)
                namespace = client.V1Namespace(
                    api_version="v1",
                    kind="Namespace",
                    metadata=client.V1ObjectMeta(
                        name=K8S_NAMESPACE,
                        labels={LABEL_MANAGED: "true"},
                    ),
                )
                try:
                    await self.core_v1.create_namespace(namespace)
                except client.ApiException as create_err:
                    raise K8sAPIError(
                        f"Failed to create namespace '{K8S_NAMESPACE}': {create_err}"
                    ) from create_err
            else:
                raise K8sAPIError(
                    f"Failed to check namespace '{K8S_NAMESPACE}': {err}"
                ) from err

    # ------------------------------------------------------------------
    # Deployment management
    # ------------------------------------------------------------------

    def _deployment_manifest(
        self,
        name: str,
        image: str,
        tag: str,
        *,
        env: dict[str, str | None] | None = None,
        mounts: list[dict[str, Any]] | None = None,
        ports: list[dict[str, Any]] | None = None,
        security_context: dict[str, Any] | None = None,
        resource_limits: dict[str, Any] | None = None,
        termination_grace_period: int = DEFAULT_TERMINATION_GRACE_PERIOD,
        command: list[str] | None = None,
        args: list[str] | None = None,
        extra_pod_spec: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a Kubernetes Deployment manifest dict.

        Parameters
        ----------
        name:
            The workload name – used as the Deployment name, Pod selector
            label value, and container name.
        image:
            Container image repository (without tag).
        tag:
            Image tag.
        env:
            Environment variables.  A ``None`` value means "export the key
            without a value", matching Docker's behaviour.
        mounts:
            List of Kubernetes volume mount dicts with keys ``name``,
            ``mountPath``, and optionally ``readOnly``.  Each entry must have a
            matching entry in ``volumes``.
        ports:
            List of Kubernetes container port dicts.
        security_context:
            Pod-level security context overrides.
        resource_limits:
            Kubernetes resource requirements dict, e.g.
            ``{"requests": {"memory": "64Mi"}, "limits": {"memory": "128Mi"}}``.
        termination_grace_period:
            Seconds to wait after SIGTERM before SIGKILL.
        command:
            Override container entrypoint.
        args:
            Override container CMD.
        extra_pod_spec:
            Additional fields merged into the Pod spec (e.g. ``hostNetwork``,
            ``volumes``).  These are applied after the standard fields so they
            can override defaults.

        """
        labels = {
            LABEL_MANAGED: "true",
            LABEL_APP: name,
        }

        env_list: list[dict[str, str]] = []
        for key, val in (env or {}).items():
            if val is None:
                env_list.append({"name": key})
            else:
                env_list.append({"name": key, "value": val})

        container: dict[str, Any] = {
            "name": name,
            "image": f"{image}:{tag}",
            "imagePullPolicy": "IfNotPresent",
        }
        if env_list:
            container["env"] = env_list
        if mounts:
            container["volumeMounts"] = mounts
        if ports:
            container["ports"] = ports
        if security_context:
            container["securityContext"] = security_context
        if resource_limits:
            container["resources"] = resource_limits
        if command:
            container["command"] = command
        if args:
            container["args"] = args

        pod_spec: dict[str, Any] = {
            "containers": [container],
            "restartPolicy": "Always",
            "terminationGracePeriodSeconds": termination_grace_period,
        }
        if extra_pod_spec:
            pod_spec.update(extra_pod_spec)

        manifest: dict[str, Any] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "namespace": K8S_NAMESPACE,
                "labels": labels,
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {LABEL_APP: name}},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": pod_spec,
                },
            },
        }
        return manifest

    async def apply_deployment(
        self,
        name: str,
        image: str,
        tag: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Create or update a Supervisor-managed Deployment.

        Uses server-side apply (``field_manager``) so that re-applying the same
        manifest is always safe.
        """
        manifest = self._deployment_manifest(name, image, tag, **kwargs)
        try:
            existing = await self.apps_v1.read_namespaced_deployment(
                name, K8S_NAMESPACE
            )
            # Update existing deployment.
            manifest["metadata"]["resourceVersion"] = existing.metadata.resource_version
            result = await self.apps_v1.replace_namespaced_deployment(
                name, K8S_NAMESPACE, manifest
            )
            _LOGGER.debug("Updated Deployment '%s'", name)
        except client.ApiException as err:
            if err.status == 404:
                result = await self.apps_v1.create_namespaced_deployment(
                    K8S_NAMESPACE, manifest
                )
                _LOGGER.debug("Created Deployment '%s'", name)
            else:
                raise K8sAPIError(
                    f"Failed to apply Deployment '{name}': {err}"
                ) from err
        return result.to_dict()

    async def scale_deployment(self, name: str, replicas: int) -> None:
        """Scale a Deployment to *replicas* replicas (0 = stopped, 1 = running)."""
        patch = {"spec": {"replicas": replicas}}
        try:
            await self.apps_v1.patch_namespaced_deployment_scale(
                name, K8S_NAMESPACE, patch
            )
            _LOGGER.debug("Scaled Deployment '%s' to %d replicas", name, replicas)
        except client.ApiException as err:
            if err.status == 404:
                raise K8sNotFound(
                    f"Deployment '{name}' not found for scaling", _LOGGER.error
                ) from err
            raise K8sAPIError(
                f"Failed to scale Deployment '{name}': {err}", _LOGGER.error
            ) from err

    async def delete_deployment(self, name: str) -> None:
        """Delete a Deployment (and its Pods) by name."""
        try:
            await self.apps_v1.delete_namespaced_deployment(
                name,
                K8S_NAMESPACE,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )
            _LOGGER.debug("Deleted Deployment '%s'", name)
        except client.ApiException as err:
            if err.status == 404:
                raise K8sNotFound(
                    f"Deployment '{name}' not found for deletion", _LOGGER.warning
                ) from err
            raise K8sAPIError(
                f"Failed to delete Deployment '{name}': {err}", _LOGGER.error
            ) from err

    async def restart_deployment(self, name: str) -> None:
        """Trigger a rollout restart of a Deployment."""
        patch = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "supervisor.home-assistant.io/restartedAt": datetime.now(
                                UTC
                            ).isoformat()
                        }
                    }
                }
            }
        }
        try:
            await self.apps_v1.patch_namespaced_deployment(name, K8S_NAMESPACE, patch)
            _LOGGER.debug("Triggered rollout restart for Deployment '%s'", name)
        except client.ApiException as err:
            if err.status == 404:
                raise K8sNotFound(
                    f"Deployment '{name}' not found for restart", _LOGGER.warning
                ) from err
            raise K8sAPIError(
                f"Failed to restart Deployment '{name}': {err}", _LOGGER.error
            ) from err

    async def get_deployment(self, name: str) -> dict[str, Any] | None:
        """Return the Deployment manifest or ``None`` if it does not exist."""
        try:
            result = await self.apps_v1.read_namespaced_deployment(name, K8S_NAMESPACE)
            return result.to_dict()
        except client.ApiException as err:
            if err.status == 404:
                return None
            raise K8sAPIError(
                f"Failed to get Deployment '{name}': {err}", _LOGGER.error
            ) from err

    # ------------------------------------------------------------------
    # Pod helpers
    # ------------------------------------------------------------------

    async def get_pods_for_deployment(self, name: str) -> list[dict[str, Any]]:
        """Return all Pods owned by the named Deployment."""
        try:
            result = await self.core_v1.list_namespaced_pod(
                K8S_NAMESPACE,
                label_selector=f"{LABEL_APP}={name}",
            )
            return [p.to_dict() for p in result.items]
        except client.ApiException as err:
            raise K8sAPIError(
                f"Failed to list Pods for '{name}': {err}", _LOGGER.error
            ) from err

    async def pod_logs(self, name: str, tail: int = 100) -> list[str]:
        """Return the last *tail* lines of logs for the most recent Pod of *name*."""
        pods = await self.get_pods_for_deployment(name)
        if not pods:
            raise K8sNotFound(f"No Pods found for Deployment '{name}'", _LOGGER.warning)
        # Pick the most recently-created Pod.
        pods.sort(
            key=lambda p: p.get("metadata", {}).get("creationTimestamp") or "",
            reverse=True,
        )
        pod_name = pods[0]["metadata"]["name"]
        try:
            log_data: str = await self.core_v1.read_namespaced_pod_log(
                pod_name,
                K8S_NAMESPACE,
                tail_lines=tail,
                timestamps=False,
            )
            return log_data.splitlines()
        except client.ApiException as err:
            raise K8sAPIError(
                f"Failed to get logs for Pod '{pod_name}': {err}", _LOGGER.warning
            ) from err

    async def pod_exec(self, name: str, command: str) -> ExecReturn:
        """Execute *command* inside the running Pod for workload *name*.

        Streams stdout/stderr until the process exits and returns an
        :class:`~supervisor.docker.manager.ExecReturn`.
        """
        pods = await self.get_pods_for_deployment(name)
        running = [p for p in pods if p.get("status", {}).get("phase") == "Running"]
        if not running:
            raise K8sNotFound(f"No running Pod found for '{name}'", _LOGGER.warning)
        pod_name = running[0]["metadata"]["name"]
        container_name = name

        try:
            async with WsApiClient() as ws_client:
                core_v1_ws = client.CoreV1Api(api_client=ws_client)
                resp = await core_v1_ws.connect_get_namespaced_pod_exec(
                    pod_name,
                    K8S_NAMESPACE,
                    command=["sh", "-c", command],
                    container=container_name,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                )
        except client.ApiException as err:
            raise K8sAPIError(
                f"Failed to exec in Pod '{pod_name}': {err}", _LOGGER.error
            ) from err

        # resp is a websocket client; read all output.
        output = b""
        exit_code = 0
        if hasattr(resp, "read_all"):
            output = (await resp.read_all()).encode()
        elif hasattr(resp, "returncode"):
            exit_code = resp.returncode or 0

        return ExecReturn(exit_code=exit_code, output=output)

    # ------------------------------------------------------------------
    # Stats (Metrics API)
    # ------------------------------------------------------------------

    async def pod_stats(self, name: str) -> K8sStats:
        """Return resource usage for the active Pod of workload *name*.

        Reads from the Kubernetes Metrics API (metrics.k8s.io/v1beta1).  If the
        metrics-server is not installed the call will fail with :class:`K8sAPIError`.
        """
        custom_api = client.CustomObjectsApi(self._api_client)
        try:
            result = await custom_api.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=K8S_NAMESPACE,
                plural="pods",
                name=name,
            )
        except client.ApiException as err:
            if err.status == 404:
                raise K8sNotFound(
                    f"Metrics for Pod '{name}' not found", _LOGGER.warning
                ) from err
            raise K8sAPIError(
                f"Failed to get metrics for '{name}': {err}", _LOGGER.error
            ) from err

        containers: list[dict[str, Any]] = result.get("containers", [])
        metrics = containers[0] if containers else {}

        # Fetch resource limits from the Deployment spec.
        limits: dict[str, Any] | None = None
        deployment = await self.get_deployment(name)
        if deployment:
            try:
                container_specs = deployment["spec"]["template"]["spec"]["containers"]
                if container_specs:
                    limits = container_specs[0].get("resources", {}).get("limits")
            except KeyError, IndexError:
                pass

        return K8sStats(metrics, limits)

    # ------------------------------------------------------------------
    # One-shot command Jobs
    # ------------------------------------------------------------------

    async def run_command(
        self,
        image: str,
        command: list[str],
        tag: str = "latest",
        **kwargs: Any,
    ) -> CommandReturn:
        """Run *command* in a temporary Kubernetes Job and return its output.

        The Job is deleted after the command completes regardless of success or
        failure.
        """
        job_name = f"supervisor-cmd-{uuid4().hex[:8]}"

        labels = {LABEL_MANAGED: "true", LABEL_APP: job_name}
        job_manifest: dict[str, Any] = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": K8S_NAMESPACE,
                "labels": labels,
            },
            "spec": {
                "ttlSecondsAfterFinished": 0,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "cmd",
                                "image": f"{image}:{tag}",
                                "imagePullPolicy": "IfNotPresent",
                                "command": command,
                            }
                        ],
                    },
                },
            },
        }

        try:
            await self.batch_v1.create_namespaced_job(K8S_NAMESPACE, job_manifest)
        except client.ApiException as err:
            raise K8sAPIError(
                f"Failed to create Job '{job_name}': {err}", _LOGGER.error
            ) from err

        log_lines: list[str] = []
        exit_code = 0
        try:
            exit_code, log_lines = await self._await_job(job_name)
        finally:
            with contextlib.suppress(client.ApiException):
                await self.batch_v1.delete_namespaced_job(
                    job_name,
                    K8S_NAMESPACE,
                    body=client.V1DeleteOptions(propagation_policy="Foreground"),
                )

        return CommandReturn(exit_code=exit_code, log=log_lines)

    async def _await_job(
        self, job_name: str, poll_interval: float = 2.0, timeout: float = 300.0
    ) -> tuple[int, list[str]]:
        """Poll until the Job completes and return (exit_code, log_lines)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            if asyncio.get_event_loop().time() > deadline:
                raise K8sTimeoutError(
                    f"Timeout waiting for Job '{job_name}' to complete"
                )
            try:
                job = await self.batch_v1.read_namespaced_job(job_name, K8S_NAMESPACE)
            except client.ApiException as err:
                raise K8sAPIError(f"Failed to read Job '{job_name}': {err}") from err

            status = job.status
            if status.succeeded:
                break
            if status.failed:
                # Retrieve exit code from the Pod.
                pods = await self.core_v1.list_namespaced_pod(
                    K8S_NAMESPACE,
                    label_selector=f"{LABEL_APP}={job_name}",
                )
                exit_code = 1
                for pod in pods.items:
                    for cs in pod.status.container_statuses or []:
                        if cs.state.terminated:
                            exit_code = cs.state.terminated.exit_code or 1
                log_lines = await self._job_logs(job_name)
                return exit_code, log_lines

            await asyncio.sleep(poll_interval)

        log_lines = await self._job_logs(job_name)
        return 0, log_lines

    async def _job_logs(self, job_name: str) -> list[str]:
        """Retrieve logs from the first Pod of a Job."""
        try:
            pods = await self.core_v1.list_namespaced_pod(
                K8S_NAMESPACE,
                label_selector=f"{LABEL_APP}={job_name}",
            )
            if not pods.items:
                return []
            pod_name = pods.items[0].metadata.name
            log_data: str = await self.core_v1.read_namespaced_pod_log(
                pod_name, K8S_NAMESPACE
            )
            return log_data.splitlines()
        except client.ApiException:
            return []

    # ------------------------------------------------------------------
    # Service management
    # ------------------------------------------------------------------

    async def get_service(self, name: str) -> dict[str, Any] | None:
        """Return the Service manifest or ``None`` if it does not exist."""
        try:
            result = await self.core_v1.read_namespaced_service(name, K8S_NAMESPACE)
            return result.to_dict()
        except client.ApiException as err:
            if err.status == 404:
                return None
            raise K8sAPIError(
                f"Failed to get Service '{name}': {err}", _LOGGER.error
            ) from err

    async def get_service_cluster_ip(self, name: str) -> str | None:
        """Return the ClusterIP of a Service or ``None`` if unavailable."""
        service = await self.get_service(name)
        if not service:
            return None
        spec = service.get("spec") or {}
        # kubernetes_asyncio model to_dict() emits snake_case keys, while raw
        # manifests use camelCase - accept both.
        return spec.get("clusterIP") or spec.get("cluster_ip")

    async def apply_service(
        self,
        name: str,
        ports: list[dict[str, Any]],
        *,
        service_type: str = "ClusterIP",
        selector: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Create or update a Kubernetes Service for workload *name*.

        Parameters
        ----------
        name:
            Service name (same as the Deployment name).
        ports:
            List of ``{"port": int, "targetPort": int, "protocol": str}`` dicts.
        service_type:
            Kubernetes service type (``ClusterIP``, ``NodePort``, ``LoadBalancer``).
        selector:
            Pod label selector for the Service.  Defaults to
            ``{LABEL_APP: name}`` when ``None``, which is appropriate for
            services that front their own Deployment.  Pass an explicit
            selector when the Service should route to a *different*
            workload (e.g. the Observer Service routing to HA Core).

        """
        labels = {LABEL_MANAGED: "true", LABEL_APP: name}
        svc_manifest: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": name,
                "namespace": K8S_NAMESPACE,
                "labels": labels,
            },
            "spec": {
                "selector": selector or {LABEL_APP: name},
                "ports": ports,
                "type": service_type,
            },
        }
        try:
            existing = await self.core_v1.read_namespaced_service(name, K8S_NAMESPACE)
            svc_manifest["metadata"]["resourceVersion"] = (
                existing.metadata.resource_version
            )
            result = await self.core_v1.replace_namespaced_service(
                name, K8S_NAMESPACE, svc_manifest
            )
        except client.ApiException as err:
            if err.status == 404:
                result = await self.core_v1.create_namespaced_service(
                    K8S_NAMESPACE, svc_manifest
                )
            else:
                raise K8sAPIError(
                    f"Failed to apply Service '{name}': {err}", _LOGGER.error
                ) from err
        return result.to_dict()

    async def delete_service(self, name: str) -> None:
        """Delete a Service by name (ignores 404)."""
        try:
            await self.core_v1.delete_namespaced_service(name, K8S_NAMESPACE)
        except client.ApiException as err:
            if err.status != 404:
                raise K8sAPIError(
                    f"Failed to delete Service '{name}': {err}", _LOGGER.warning
                ) from err

    # ------------------------------------------------------------------
    # Repair / cleanup
    # ------------------------------------------------------------------

    async def repair(self) -> None:
        """Remove stale Supervisor-managed resources from the namespace.

        Deletes Deployments and Services whose Pods are not running and that
        have the Supervisor managed label but are no longer tracked by any
        active component.  This is a best-effort operation; errors are logged
        but not raised.
        """
        _LOGGER.info(
            "Pruning stale Supervisor-managed resources in namespace '%s'",
            K8S_NAMESPACE,
        )
        label_selector = f"{LABEL_MANAGED}=true"
        try:
            deployments = await self.apps_v1.list_namespaced_deployment(
                K8S_NAMESPACE, label_selector=label_selector
            )
            for deployment in deployments.items:
                dep_name = deployment.metadata.name
                replicas = deployment.status.ready_replicas or 0
                if replicas == 0:
                    _LOGGER.debug(
                        "Found stale Deployment '%s' with 0 ready replicas", dep_name
                    )
        except client.ApiException as err:
            _LOGGER.warning("Error listing Deployments during repair: %s", err)
