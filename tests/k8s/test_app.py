"""Tests for the Kubernetes app backend."""

from ipaddress import IPv4Address
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest

from supervisor.apps.app import App
from supervisor.coresys import CoreSys
from supervisor.docker.app import DockerApp
from supervisor.exceptions import AppNotSupportedError, AppNotSupportedWriteStdinError
from supervisor.k8s.app import K8sApp

from tests.const import TEST_ADDON_SLUG


@pytest.fixture(name="k8s_api")
def fixture_k8s_api(coresys: CoreSys) -> AsyncMock:
    """Set up a mocked Kubernetes API manager."""
    coresys.k8s = AsyncMock()
    return coresys.k8s


@pytest.fixture(name="k8s_app")
async def fixture_k8s_app(coresys: CoreSys, k8s_api: AsyncMock, test_repository) -> App:
    """Install the local_ssh app with the Kubernetes backend active."""
    store = coresys.apps.store[TEST_ADDON_SLUG]
    await coresys.apps.data.install(store)
    coresys.apps.data._data = coresys.apps.data._schema(  # pylint: disable=protected-access
        coresys.apps.data._data  # pylint: disable=protected-access
    )

    app = App(coresys, store.slug)
    coresys.apps.local[app.slug] = app
    return app


def test_app_backend_selection(coresys: CoreSys, k8s_app: App) -> None:
    """App instances use the Kubernetes backend when k8s is enabled."""
    assert isinstance(k8s_app.instance, K8sApp)


async def test_app_backend_docker_default(
    coresys: CoreSys, install_app_ssh: App
) -> None:
    """App instances use the Docker backend when k8s is not enabled."""
    assert isinstance(install_app_ssh.instance, DockerApp)


def test_slug_to_name_sanitization() -> None:
    """Workload names are valid Kubernetes resource names."""
    assert K8sApp.slug_to_name("local_ssh") == "app-local-ssh"
    assert K8sApp.slug_to_name("A_Weird.Slug!") == "app-a-weird-slug"
    assert len(K8sApp.slug_to_name("x" * 100)) <= 63


async def test_run_rejects_unsupported_capabilities(k8s_app: App) -> None:
    """Apps requiring host capabilities are rejected on Kubernetes."""
    # local_ssh declares host_dbus, audio and uart - all unsupported.
    with pytest.raises(AppNotSupportedError):
        await k8s_app.instance.run()


@pytest.mark.usefixtures("tmp_supervisor_data", "path_extern")
async def test_run_applies_deployment_and_service(
    coresys: CoreSys, k8s_api: AsyncMock, k8s_app: App
) -> None:
    """A supported app is applied as Deployment + Service."""
    instance = k8s_app.instance
    assert isinstance(instance, K8sApp)

    k8s_api.get_pods_for_deployment.return_value = []
    k8s_api.get_service_cluster_ip.return_value = "10.43.0.5"

    with (
        patch.object(App, "host_dbus", new=PropertyMock(return_value=False)),
        patch.object(App, "with_audio", new=PropertyMock(return_value=False)),
        patch.object(App, "with_uart", new=PropertyMock(return_value=False)),
        patch.object(App, "with_ingress", new=PropertyMock(return_value=False)),
        patch.object(App, "need_build", new=PropertyMock(return_value=False)),
        patch.object(App, "ports", new=PropertyMock(return_value={"22/tcp": 2222})),
        patch.object(
            type(coresys.plugins.dns), "add_host", new=AsyncMock()
        ) as add_host,
    ):
        await instance.run()

    k8s_api.apply_deployment.assert_awaited_once()
    name, image, tag = k8s_api.apply_deployment.await_args.args
    assert name == "app-local-ssh"
    assert image == k8s_app.image
    assert tag == str(k8s_app.version)

    kwargs = k8s_api.apply_deployment.await_args.kwargs
    env = kwargs["env"]
    assert "SUPERVISOR_TOKEN" in env
    mount_paths = {mount["mountPath"] for mount in kwargs["mounts"]}
    assert "/data" in mount_paths
    assert "/share" in mount_paths

    k8s_api.apply_service.assert_awaited_once()
    service_name, service_ports = k8s_api.apply_service.await_args.args
    assert service_name == "app-local-ssh"
    assert service_ports == [
        {"name": "p22-tcp", "port": 2222, "targetPort": 22, "protocol": "TCP"}
    ]

    assert instance.ip_address == IPv4Address("10.43.0.5")
    add_host.assert_awaited_once()


async def test_write_stdin_not_supported(k8s_app: App) -> None:
    """write_stdin raises on the Kubernetes backend."""
    with pytest.raises(AppNotSupportedWriteStdinError):
        await k8s_app.instance.write_stdin(b"test")


async def test_export_import_image_not_supported(k8s_app: App, tmp_path) -> None:
    """Image export/import raises on the Kubernetes backend."""
    with pytest.raises(AppNotSupportedError):
        await k8s_app.instance.export_image(tmp_path / "image.tar")
    with pytest.raises(AppNotSupportedError):
        await k8s_app.instance.import_image(tmp_path / "image.tar")
