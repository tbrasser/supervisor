"""Tests for Kubernetes plugin backends."""

from unittest.mock import AsyncMock

import pytest

from awesomeversion import AwesomeVersion

from supervisor.const import OBSERVER_PORT
from supervisor.coresys import CoreSys
from supervisor.docker.const import ContainerState
from supervisor.k8s.audio import K8sAudio
from supervisor.k8s.const import LABEL_APP
from supervisor.k8s.dns import K8sDns
from supervisor.k8s.homeassistant import HASS_K8S_NAME
from supervisor.k8s.observer import K8sObserver
from supervisor.plugins.audio import PluginAudio
from supervisor.plugins.dns import PluginDns
from supervisor.plugins.observer import PluginObserver


@pytest.fixture(name="k8s_api")
def fixture_k8s_api(coresys: CoreSys) -> AsyncMock:
    """Set up a mocked Kubernetes API manager."""
    coresys.k8s = AsyncMock()
    return coresys.k8s


def test_plugin_backends_switch_to_k8s(coresys: CoreSys, k8s_api: AsyncMock) -> None:
    """Ensure plugin wrappers use Kubernetes implementations when k8s is enabled."""
    assert isinstance(PluginAudio(coresys).instance, K8sAudio)
    assert isinstance(PluginDns(coresys).instance, K8sDns)
    assert isinstance(PluginObserver(coresys).instance, K8sObserver)


async def test_k8s_observer_routes_to_homeassistant(
    coresys: CoreSys, k8s_api: AsyncMock
) -> None:
    """Observer service should route to the Home Assistant deployment."""
    observer = K8sObserver(coresys)

    await observer.run()

    k8s_api.apply_service.assert_awaited_once_with(
        observer.name,
        [{"port": OBSERVER_PORT, "targetPort": 8123, "protocol": "TCP"}],
        selector={LABEL_APP: HASS_K8S_NAME},
    )


@pytest.mark.parametrize("plugin_type", [K8sAudio, K8sDns])
async def test_k8s_audio_dns_noop_behaviour(
    coresys: CoreSys, k8s_api: AsyncMock, plugin_type: type[K8sAudio | K8sDns]
) -> None:
    """Audio and DNS Kubernetes backends should act as always-available no-ops."""
    plugin = plugin_type(coresys)

    await plugin.run()
    await plugin.stop()
    await plugin.restart()
    await plugin.install(version=AwesomeVersion("2026.1.0"))
    await plugin.update(version=AwesomeVersion("2026.1.1"))

    assert plugin.image is None
    assert await plugin.exists() is True
    assert await plugin.is_running() is True
    assert await plugin.current_state() == ContainerState.RUNNING
    assert await plugin.logs() == []

    stats = await plugin.stats()
    assert stats.cpu_percent == 0.0
    assert stats.memory_usage == 0
