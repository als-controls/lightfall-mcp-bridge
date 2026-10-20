"""The vendored capability module must match lightfall_utils.ipc.capability v1."""

from __future__ import annotations

from lightfall_bridge import nats_capability as cap


def test_vendored_version_marker():
    assert cap.VENDORED_FROM == "lightfall_utils.ipc.capability v1"


def test_prefix_vectors():
    assert cap.derive_prefix("test-secret-0001") == "r5itqnjn2uhkfjylxqpf7fwjv7"
    assert cap.derive_prefix("A" * 43) == "3dcidrblqoesmah3mp7u5jx4cx"
