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

# Image pull policy used for all Supervisor-managed containers.
# "IfNotPresent" avoids unnecessary pulls while still allowing updates when the
# tag is changed.
IMAGE_PULL_POLICY = "IfNotPresent"

# Grace period (seconds) given to a Pod before SIGKILL is sent.
DEFAULT_TERMINATION_GRACE_PERIOD = 30

# Restart policy for long-running workloads managed by a Deployment.
RESTART_POLICY_ALWAYS = "Always"
# For one-shot Jobs the restart policy must be Never or OnFailure.
RESTART_POLICY_NEVER = "Never"

# Container port protocol values.
PROTOCOL_TCP = "TCP"
PROTOCOL_UDP = "UDP"

# Kubernetes resource API groups / versions used by Supervisor.
API_VERSION_CORE = "v1"
API_VERSION_APPS = "apps/v1"

# Resource kinds.
KIND_POD = "Pod"
KIND_DEPLOYMENT = "Deployment"
KIND_SERVICE = "Service"
KIND_JOB = "Job"
KIND_PERSISTENT_VOLUME_CLAIM = "PersistentVolumeClaim"
KIND_CONFIG_MAP = "ConfigMap"
KIND_SECRET = "Secret"

# ConfigMap key that stores the container identifier written to /run/cid inside
# the container (mirrors the docker cidfile pattern).
CONFIGMAP_KEY_CID = "cid"
