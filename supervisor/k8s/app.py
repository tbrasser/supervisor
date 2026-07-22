"""Kubernetes workload definition for Supervisor apps.

This is the Kubernetes equivalent of :class:`supervisor.docker.app.DockerApp`,
adapted from the ``KubernetesAddon`` backend of the
``shantur/homeassistant-k8s-supervisor`` fork.

Each installed app is represented as a single-replica Deployment plus a
ClusterIP Service inside the ``home-assistant`` namespace:

* The Deployment runs the app image with the same environment variables and
  (hostPath) volume mounts as the Docker backend, so apps behave identically
  under both backends.
* The Service exposes the app's declared ports and its ingress port inside
  the cluster.  The Service ClusterIP doubles as the app "IP address" used by
  Supervisor's ingress proxy and DNS handling.

Capabilities intentionally dropped from the fork:

* The shared-RWX-PVC storage model – this repository's Kubernetes backend
  uses hostPath volumes (matching the Home Assistant Core workload), so no
  PVC/port-registry/NGINX L4 proxy machinery is needed.
* Gateway API objects – ingress routing goes through Supervisor's built-in
  ingress proxy via the app Service ClusterIP.

Hard limitations (validated before start): host networking, device / GPIO /
USB / UART / video passthrough, full hardware access, host D-Bus, audio,
kernel modules, stdin attachment and local image builds are not supported on
the Kubernetes backend.
"""

from __future__ import annotations

from ipaddress import IPv4Address
import logging
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, cast

from awesomeversion import AwesomeVersion

from ..apps.const import MappingType
from ..coresys import CoreSys
from ..docker.const import (
    ENV_TIME,
    ENV_TOKEN,
    ENV_TOKEN_OLD,
    PATH_ALL_ADDON_CONFIGS,
    PATH_ALL_APP_CONFIGS,
    PATH_BACKUP,
    PATH_HOMEASSISTANT_CONFIG,
    PATH_HOMEASSISTANT_CONFIG_LEGACY,
    PATH_LOCAL_ADDONS,
    PATH_LOCAL_APPS,
    PATH_MEDIA,
    PATH_PRIVATE_DATA,
    PATH_PUBLIC_CONFIG,
    PATH_SHARE,
    PATH_SSL,
)
from ..exceptions import (
    AppNotSupportedError,
    AppNotSupportedWriteStdinError,
    CoreDNSError,
)
from ..jobs.const import JobConcurrency
from ..jobs.decorator import Job
from .exceptions import K8sJobError
from .interface import K8sInterface

if TYPE_CHECKING:
    from ..apps.app import App

_LOGGER: logging.Logger = logging.getLogger(__name__)

NO_ADDRESS = IPv4Address("0.0.0.0")

_RE_K8S_NAME = re.compile(r"[^a-z0-9-]+")


def _sanitize_k8s_name(name: str) -> str:
    """Sanitize a string into a valid Kubernetes resource name."""
    sanitized = _RE_K8S_NAME.sub("-", name.lower()).strip("-")
    return sanitized[:63].rstrip("-")


class K8sApp(K8sInterface):
    """Kubernetes Supervisor workload for an app."""

    def __init__(self, coresys: CoreSys, app: App) -> None:
        """Initialize K8s app wrapper."""
        self.app: App = app
        super().__init__(coresys)
        self._cluster_ip: IPv4Address = NO_ADDRESS

    @staticmethod
    def slug_to_name(slug: str) -> str:
        """Convert an app slug to the Kubernetes workload name."""
        return _sanitize_k8s_name(f"app-{slug}")

    @property
    def name(self) -> str:
        """Return the Kubernetes workload name."""
        return K8sApp.slug_to_name(self.app.slug)

    @property
    def image(self) -> str | None:
        """Return the app container image repository."""
        return self.app.image

    @property
    def version(self) -> AwesomeVersion:
        """Return the installed app version."""
        return self.app.version

    @property
    def timeout(self) -> int:
        """Return the graceful shutdown timeout in seconds."""
        return self.app.timeout

    @property
    def ip_address(self) -> IPv4Address:
        """Return the app Service ClusterIP (cached)."""
        return self._cluster_ip

    @property
    def environment(self) -> dict[str, str | int | None]:
        """Return environment for the app container."""
        app_env = cast(dict[str, str | int | None], self.app.environment or {})

        # Provide options for legacy apps
        if self.app.legacy:
            for key, value in self.app.options.items():
                if isinstance(value, (int, str)):
                    app_env[key] = value
                else:
                    _LOGGER.warning("Can not set nested option %s as environment", key)

        return {
            **app_env,
            ENV_TIME: self.sys_timezone,
            ENV_TOKEN: self.app.supervisor_token,
            ENV_TOKEN_OLD: self.app.supervisor_token,
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_supported(self) -> None:
        """Raise if the app requires capabilities not available on Kubernetes."""
        unsupported: list[str] = []
        if self.app.host_network:
            unsupported.append("host networking")
        if self.app.host_pid or self.app.host_uts:
            unsupported.append("host PID/UTS namespaces")
        if self.app.host_dbus:
            unsupported.append("host D-Bus access")
        if self.app.devices or self.app.static_devices:
            unsupported.append("device passthrough")
        if self.app.with_gpio or self.app.with_usb or self.app.with_uart:
            unsupported.append("GPIO/USB/UART access")
        if self.app.with_video:
            unsupported.append("video device access")
        if self.app.with_audio:
            unsupported.append("host audio")
        if self.app.with_kernel_modules:
            unsupported.append("kernel module access")
        if not self.app.protected and self.app.with_full_access:
            unsupported.append("full hardware access")
        if self.app.need_build:
            unsupported.append("local image builds")

        if unsupported:
            raise AppNotSupportedError(
                f"App {self.app.slug} is not supported on the Kubernetes "
                f"backend, it requires: {', '.join(unsupported)}",
                _LOGGER.error,
            )

    # ------------------------------------------------------------------
    # Volume mounts
    # ------------------------------------------------------------------

    def _folder_mount(
        self,
        name: str,
        source: str,
        target: str,
        read_only: bool,
        mounts: list[dict[str, Any]],
        volumes: list[dict[str, Any]],
    ) -> None:
        """Append a hostPath volume + volumeMount pair."""
        mounts.append({"name": name, "mountPath": target, "readOnly": read_only})
        volumes.append(
            {
                "name": name,
                "hostPath": {"path": source, "type": "DirectoryOrCreate"},
            }
        )

    def _volume_mounts(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Build Kubernetes volumeMounts and volumes for the app.

        Mirrors the folder mappings of :meth:`DockerApp.mounts`, using
        hostPath volumes like the other workloads of this backend.  Hardware,
        audio, D-Bus and journald mounts are excluded – apps requiring them
        are rejected by :meth:`_validate_supported`.
        """
        app_mapping = self.app.map_volumes
        mounts: list[dict[str, Any]] = []
        volumes: list[dict[str, Any]] = []

        target_data_path: str | None = None
        if MappingType.DATA in app_mapping:
            target_data_path = app_mapping[MappingType.DATA].path

        self._folder_mount(
            "data",
            self.app.path_extern_data.as_posix(),
            target_data_path or PATH_PRIVATE_DATA.as_posix(),
            False,
            mounts,
            volumes,
        )

        if MappingType.CONFIG in app_mapping:
            self._folder_mount(
                "config",
                self.sys_config.path_extern_homeassistant.as_posix(),
                app_mapping[MappingType.CONFIG].path
                or PATH_HOMEASSISTANT_CONFIG_LEGACY.as_posix(),
                app_mapping[MappingType.CONFIG].read_only,
                mounts,
                volumes,
            )
        else:
            if self.app.app_config_used:
                config_mapping_type = (
                    MappingType.APP_CONFIG
                    if MappingType.APP_CONFIG in app_mapping
                    else MappingType.ADDON_CONFIG
                )
                self._folder_mount(
                    "app-config",
                    self.app.path_extern_config.as_posix(),
                    app_mapping[config_mapping_type].path
                    or PATH_PUBLIC_CONFIG.as_posix(),
                    app_mapping[config_mapping_type].read_only,
                    mounts,
                    volumes,
                )

            if MappingType.HOMEASSISTANT_CONFIG in app_mapping:
                self._folder_mount(
                    "homeassistant-config",
                    self.sys_config.path_extern_homeassistant.as_posix(),
                    app_mapping[MappingType.HOMEASSISTANT_CONFIG].path
                    or PATH_HOMEASSISTANT_CONFIG.as_posix(),
                    app_mapping[MappingType.HOMEASSISTANT_CONFIG].read_only,
                    mounts,
                    volumes,
                )

        all_app_configs_mapping_type: MappingType | None = None
        if MappingType.ALL_APP_CONFIGS in app_mapping:
            all_app_configs_mapping_type = MappingType.ALL_APP_CONFIGS
        elif MappingType.ALL_ADDON_CONFIGS in app_mapping:
            all_app_configs_mapping_type = MappingType.ALL_ADDON_CONFIGS

        if all_app_configs_mapping_type:
            self._folder_mount(
                "all-app-configs",
                self.sys_config.path_extern_app_configs.as_posix(),
                app_mapping[all_app_configs_mapping_type].path
                or (
                    PATH_ALL_APP_CONFIGS.as_posix()
                    if all_app_configs_mapping_type == MappingType.ALL_APP_CONFIGS
                    else PATH_ALL_ADDON_CONFIGS.as_posix()
                ),
                app_mapping[all_app_configs_mapping_type].read_only,
                mounts,
                volumes,
            )

        if MappingType.SSL in app_mapping:
            self._folder_mount(
                "ssl",
                self.sys_config.path_extern_ssl.as_posix(),
                app_mapping[MappingType.SSL].path or PATH_SSL.as_posix(),
                app_mapping[MappingType.SSL].read_only,
                mounts,
                volumes,
            )

        apps_mapping_type: MappingType | None = None
        if MappingType.LOCAL_APPS in app_mapping:
            apps_mapping_type = MappingType.LOCAL_APPS
        elif MappingType.ADDONS in app_mapping:
            apps_mapping_type = MappingType.ADDONS

        if apps_mapping_type:
            self._folder_mount(
                "local-apps",
                self.sys_config.path_extern_apps_local.as_posix(),
                app_mapping[apps_mapping_type].path
                or (
                    PATH_LOCAL_APPS.as_posix()
                    if apps_mapping_type == MappingType.LOCAL_APPS
                    else PATH_LOCAL_ADDONS.as_posix()
                ),
                app_mapping[apps_mapping_type].read_only,
                mounts,
                volumes,
            )

        if MappingType.BACKUP in app_mapping:
            self._folder_mount(
                "backup",
                self.sys_config.path_extern_backup.as_posix(),
                app_mapping[MappingType.BACKUP].path or PATH_BACKUP.as_posix(),
                app_mapping[MappingType.BACKUP].read_only,
                mounts,
                volumes,
            )

        if MappingType.SHARE in app_mapping:
            self._folder_mount(
                "share",
                self.sys_config.path_extern_share.as_posix(),
                app_mapping[MappingType.SHARE].path or PATH_SHARE.as_posix(),
                app_mapping[MappingType.SHARE].read_only,
                mounts,
                volumes,
            )

        if MappingType.MEDIA in app_mapping:
            self._folder_mount(
                "media",
                self.sys_config.path_extern_media.as_posix(),
                app_mapping[MappingType.MEDIA].path or PATH_MEDIA.as_posix(),
                app_mapping[MappingType.MEDIA].read_only,
                mounts,
                volumes,
            )

        return mounts, volumes

    # ------------------------------------------------------------------
    # Ports / Service
    # ------------------------------------------------------------------

    def _service_ports(self) -> list[dict[str, Any]]:
        """Build the Service port list from declared and ingress ports."""
        ports: list[dict[str, Any]] = []
        seen: set[int] = set()

        if self.app.with_ingress and self.app.ingress_port:
            ports.append(
                {
                    "name": "ingress",
                    "port": self.app.ingress_port,
                    "targetPort": self.app.ingress_port,
                    "protocol": "TCP",
                }
            )
            seen.add(self.app.ingress_port)

        for container_port, host_port in (self.app.ports or {}).items():
            if not host_port:
                continue
            port_str, _, proto = str(container_port).partition("/")
            try:
                target_port = int(port_str)
            except ValueError:
                continue
            protocol = "UDP" if proto.lower() == "udp" else "TCP"
            if int(host_port) in seen:
                continue
            seen.add(int(host_port))
            ports.append(
                {
                    "name": _sanitize_k8s_name(f"p{target_port}-{protocol.lower()}"),
                    "port": int(host_port),
                    "targetPort": target_port,
                    "protocol": protocol,
                }
            )

        return ports

    async def _refresh_cluster_ip(self) -> None:
        """Refresh the cached Service ClusterIP."""
        cluster_ip = await self.k8s.get_service_cluster_ip(self.name)
        try:
            self._cluster_ip = IPv4Address(cluster_ip) if cluster_ip else NO_ADDRESS
        except ValueError:
            _LOGGER.warning(
                "Service '%s' returned invalid ClusterIP '%s'", self.name, cluster_ip
            )
            self._cluster_ip = NO_ADDRESS

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @Job(
        name="k8s_app_run",
        on_condition=K8sJobError,
        concurrency=JobConcurrency.GROUP_REJECT,
    )
    async def run(self) -> None:
        """Apply the app Deployment (and Service) to the cluster."""
        self._validate_supported()

        if not self.app.protected:
            _LOGGER.warning("%s running with disabled protected mode!", self.app.name)

        mounts, volumes = self._volume_mounts()
        extra_pod_spec: dict[str, Any] = {"volumes": volumes}

        # Re-apply the full Deployment on every start so a freshly generated
        # supervisor token always reaches the container (scale-up alone would
        # reuse the stale pod template).
        await self._run(
            tag=str(self.app.version),
            env=cast(dict[str, str | None], self.environment),
            mounts=mounts,
            extra_pod_spec=extra_pod_spec,
            termination_grace_period=self.timeout,
        )

        service_ports = self._service_ports()
        if service_ports:
            await self.k8s.apply_service(self.name, service_ports)
        await self._refresh_cluster_ip()

        _LOGGER.info(
            "Starting app %s with version %s on Kubernetes",
            self.image,
            self.version,
        )

        # Register the app hostname with the DNS plugin when available so
        # other workloads can resolve it like on the Docker backend.
        if self._cluster_ip != NO_ADDRESS:
            try:
                await self.sys_plugins.dns.add_host(
                    ipv4=self._cluster_ip, names=[self.app.hostname]
                )
            except CoreDNSError as err:
                _LOGGER.warning("Can't update DNS for %s: %s", self.name, err)

    async def attach(
        self, version: AwesomeVersion, *, skip_state_event_if_down: bool = False
    ) -> None:
        """Attach to an existing app Deployment and refresh the ClusterIP."""
        await super().attach(version, skip_state_event_if_down=skip_state_event_if_down)
        await self._refresh_cluster_ip()

    # ------------------------------------------------------------------
    # Unsupported Docker-specific operations
    # ------------------------------------------------------------------

    async def write_stdin(self, data: bytes) -> None:
        """Write to stdin is not supported on Kubernetes."""
        raise AppNotSupportedWriteStdinError(_LOGGER.error, app=self.app.slug)

    async def export_image(self, tar_file: Path) -> None:
        """Image export is not supported on Kubernetes (registry-managed)."""
        raise AppNotSupportedError(
            f"Cannot export image of app {self.app.slug}: images are managed "
            "by the registry on the Kubernetes backend",
            _LOGGER.error,
        )

    async def import_image(self, tar_file: Path) -> None:
        """Image import is not supported on Kubernetes (registry-managed)."""
        raise AppNotSupportedError(
            f"Cannot import image for app {self.app.slug}: images are managed "
            "by the registry on the Kubernetes backend",
            _LOGGER.error,
        )
