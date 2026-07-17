"""Kubernetes backend constants."""

from __future__ import annotations

# Kubernetes namespace that Supervisor manages all resources in.
K8S_NAMESPACE = "home-assistant"

# Label applied to every resource created by Supervisor so stale ones can be
# identified and pruned.
LABEL_MANAGED = "supervisor.home-assistant.io/managed"

# Annotation carrying the Supervisor component name (add-on slug, plugin name …)
ANNOTATION_NAME = "supervisor.home-assistant.io/name"

# Label used to select pods that belong to a specific Supervisor workload.
LABEL_APP = "supervisor.home-assistant.io/app"

# Grace period (seconds) given to a Pod before SIGKILL is sent.
DEFAULT_TERMINATION_GRACE_PERIOD = 30

# ConfigMap key that stores the container identifier written to /run/cid inside
# the container (mirrors the docker cidfile pattern).
CONFIGMAP_KEY_CID = "cid"
