"""Backend-neutral workload composition for apps.

Both container backends compose the same app folder mappings and environment
variables; only the native format differs (Docker bind mounts vs Kubernetes
hostPath volumes).  This module resolves the app configuration into a neutral
spec that each backend translates into its own mount/env format.

Backend-specific extras (hardware, audio, D-Bus, journald mounts) remain in
the Docker backend; apps requiring them are rejected on Kubernetes.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import TYPE_CHECKING, cast

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
    PropagationMode,
)
from .const import MappingType

if TYPE_CHECKING:
    from .app import App

_LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class WorkloadMount:
    """Backend-neutral bind mount of a host folder into an app workload."""

    name: str
    source: str
    target: str
    read_only: bool
    propagation: PropagationMode | None = None


def workload_folder_mounts(app: App) -> list[WorkloadMount]:
    """Resolve the app folder mappings into backend-neutral mounts."""
    app_mapping = app.map_volumes
    mounts: list[WorkloadMount] = []

    target_data_path: str | None = None
    if MappingType.DATA in app_mapping:
        target_data_path = app_mapping[MappingType.DATA].path

    mounts.append(
        WorkloadMount(
            name="data",
            source=app.path_extern_data.as_posix(),
            target=target_data_path or PATH_PRIVATE_DATA.as_posix(),
            read_only=False,
        )
    )

    # setup config mappings
    if MappingType.CONFIG in app_mapping:
        mounts.append(
            WorkloadMount(
                name="config",
                source=app.sys_config.path_extern_homeassistant.as_posix(),
                target=app_mapping[MappingType.CONFIG].path
                or PATH_HOMEASSISTANT_CONFIG_LEGACY.as_posix(),
                read_only=app_mapping[MappingType.CONFIG].read_only,
            )
        )
    else:
        # Map app's public config folder if not using deprecated config option
        if app.app_config_used:
            config_mapping_type = (
                MappingType.APP_CONFIG
                if MappingType.APP_CONFIG in app_mapping
                else MappingType.ADDON_CONFIG
            )
            mounts.append(
                WorkloadMount(
                    name="app-config",
                    source=app.path_extern_config.as_posix(),
                    target=app_mapping[config_mapping_type].path
                    or PATH_PUBLIC_CONFIG.as_posix(),
                    read_only=app_mapping[config_mapping_type].read_only,
                )
            )

        # Map Home Assistant config in new way
        if MappingType.HOMEASSISTANT_CONFIG in app_mapping:
            mounts.append(
                WorkloadMount(
                    name="homeassistant-config",
                    source=app.sys_config.path_extern_homeassistant.as_posix(),
                    target=app_mapping[MappingType.HOMEASSISTANT_CONFIG].path
                    or PATH_HOMEASSISTANT_CONFIG.as_posix(),
                    read_only=app_mapping[MappingType.HOMEASSISTANT_CONFIG].read_only,
                )
            )

    all_app_configs_mapping_type: MappingType | None = None
    if MappingType.ALL_APP_CONFIGS in app_mapping:
        all_app_configs_mapping_type = MappingType.ALL_APP_CONFIGS
    elif MappingType.ALL_ADDON_CONFIGS in app_mapping:
        all_app_configs_mapping_type = MappingType.ALL_ADDON_CONFIGS

    if all_app_configs_mapping_type:
        mounts.append(
            WorkloadMount(
                name="all-app-configs",
                source=app.sys_config.path_extern_app_configs.as_posix(),
                target=app_mapping[all_app_configs_mapping_type].path
                or (
                    PATH_ALL_APP_CONFIGS.as_posix()
                    if all_app_configs_mapping_type == MappingType.ALL_APP_CONFIGS
                    else PATH_ALL_ADDON_CONFIGS.as_posix()
                ),
                read_only=app_mapping[all_app_configs_mapping_type].read_only,
            )
        )

    if MappingType.SSL in app_mapping:
        mounts.append(
            WorkloadMount(
                name="ssl",
                source=app.sys_config.path_extern_ssl.as_posix(),
                target=app_mapping[MappingType.SSL].path or PATH_SSL.as_posix(),
                read_only=app_mapping[MappingType.SSL].read_only,
            )
        )

    apps_mapping_type: MappingType | None = None
    if MappingType.LOCAL_APPS in app_mapping:
        apps_mapping_type = MappingType.LOCAL_APPS
    elif MappingType.ADDONS in app_mapping:
        apps_mapping_type = MappingType.ADDONS

    if apps_mapping_type:
        mounts.append(
            WorkloadMount(
                name="local-apps",
                source=app.sys_config.path_extern_apps_local.as_posix(),
                target=app_mapping[apps_mapping_type].path
                or (
                    PATH_LOCAL_APPS.as_posix()
                    if apps_mapping_type == MappingType.LOCAL_APPS
                    else PATH_LOCAL_ADDONS.as_posix()
                ),
                read_only=app_mapping[apps_mapping_type].read_only,
            )
        )

    if MappingType.BACKUP in app_mapping:
        mounts.append(
            WorkloadMount(
                name="backup",
                source=app.sys_config.path_extern_backup.as_posix(),
                target=app_mapping[MappingType.BACKUP].path or PATH_BACKUP.as_posix(),
                read_only=app_mapping[MappingType.BACKUP].read_only,
            )
        )

    if MappingType.SHARE in app_mapping:
        mounts.append(
            WorkloadMount(
                name="share",
                source=app.sys_config.path_extern_share.as_posix(),
                target=app_mapping[MappingType.SHARE].path or PATH_SHARE.as_posix(),
                read_only=app_mapping[MappingType.SHARE].read_only,
                propagation=PropagationMode.RSLAVE,
            )
        )

    if MappingType.MEDIA in app_mapping:
        mounts.append(
            WorkloadMount(
                name="media",
                source=app.sys_config.path_extern_media.as_posix(),
                target=app_mapping[MappingType.MEDIA].path or PATH_MEDIA.as_posix(),
                read_only=app_mapping[MappingType.MEDIA].read_only,
                propagation=PropagationMode.RSLAVE,
            )
        )

    return mounts


def workload_environment(app: App) -> dict[str, str | int | None]:
    """Return the backend-neutral environment for an app workload."""
    app_env = cast("dict[str, str | int | None]", app.environment or {})

    # Provide options for legacy apps
    if app.legacy:
        for key, value in app.options.items():
            if isinstance(value, (int, str)):
                app_env[key] = value
            else:
                _LOGGER.warning("Can not set nested option %s as environment", key)

    return {
        **app_env,
        ENV_TIME: app.sys_timezone,
        ENV_TOKEN: app.supervisor_token,
        ENV_TOKEN_OLD: app.supervisor_token,
    }
