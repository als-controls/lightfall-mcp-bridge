"""Unit tests for bridge logic (no NATS required)."""

from __future__ import annotations

import pytest

from lucid_bridge.server import BridgeState, resolve_prefix


class TestBridgeState:
    def test_initial_state(self):
        state = BridgeState(nc=None, default_prefix="als.7011")
        assert state.nc is None
        assert state.default_prefix == "als.7011"
        assert state.action_cache == {}
        assert state.auth_state == {}

    def test_default_prefix_empty(self):
        state = BridgeState(nc=None, default_prefix="")
        assert state.default_prefix == ""


class TestResolvePrefix:
    def test_explicit_prefix_used(self):
        assert resolve_prefix("als.7012", "als.7011") == "als.7012"

    def test_falls_back_to_default(self):
        assert resolve_prefix("", "als.7011") == "als.7011"

    def test_raises_when_no_prefix(self):
        with pytest.raises(ValueError, match="No prefix specified"):
            resolve_prefix("", "")
