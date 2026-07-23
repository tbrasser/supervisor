"""Backend-neutral container runtime abstractions.

This package defines the contract between Supervisor's high-level components
(apps, plugins, Home Assistant Core management) and the container backends
that implement it (``supervisor.docker`` and ``supervisor.k8s``).  Components
should depend on the protocols and helpers defined here instead of on a
specific backend implementation.
"""
