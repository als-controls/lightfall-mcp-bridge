"""Unit tests for bridge logic (no NATS required)."""

from __future__ import annotations

import asyncio
import json
import types

import pytest

from lightfall_bridge.server import BridgeState, resolve_prefix


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


class TestActionCache:
    def test_cache_stores_actions_by_prefix(self):
        state = BridgeState(nc=None, default_prefix="")
        actions = [
            {"subject": "commands.plan.run", "description": "Run a plan", "schema": {}},
        ]
        state.action_cache["als.7011"] = actions
        assert state.action_cache["als.7011"] == actions

    def test_cache_replaces_on_update(self):
        state = BridgeState(nc=None, default_prefix="")
        state.action_cache["als.7011"] = [{"subject": "old"}]
        state.action_cache["als.7011"] = [{"subject": "new"}]
        assert state.action_cache["als.7011"] == [{"subject": "new"}]

    def test_get_cached_action_metadata(self):
        state = BridgeState(nc=None, default_prefix="")
        state.action_cache["als.7011"] = [
            {"subject": "commands.echo", "description": "Echo", "schema": {"msg": "str"}},
        ]
        match = next(
            (a for a in state.action_cache.get("als.7011", [])
             if a["subject"] == "commands.echo"),
            None,
        )
        assert match is not None
        assert match["description"] == "Echo"


class TestAuthState:
    def test_no_auth_state_initially(self):
        state = BridgeState(nc=None, default_prefix="")
        assert state.auth_state == {}

    def test_approved_state_cached(self):
        state = BridgeState(nc=None, default_prefix="")
        state.auth_state["als.7011"] = "approved"
        assert state.auth_state["als.7011"] == "approved"

    def test_denied_state_cached(self):
        state = BridgeState(nc=None, default_prefix="")
        state.auth_state["als.7011"] = "denied"
        assert state.auth_state["als.7011"] == "denied"


class TestFormatExecuteResponse:
    def test_with_cached_metadata(self):
        from lightfall_bridge.server import format_execute_response

        cache = [
            {"subject": "commands.echo", "description": "Echo", "schema": {"msg": "str"}},
        ]
        result = format_execute_response(
            action="commands.echo",
            response={"echoed": "hello"},
            action_cache=cache,
        )
        assert result["action"] == "commands.echo"
        assert result["description"] == "Echo"
        assert result["schema"] == {"msg": "str"}
        assert result["response"] == {"echoed": "hello"}

    def test_without_cached_metadata(self):
        from lightfall_bridge.server import format_execute_response

        result = format_execute_response(
            action="commands.unknown",
            response={"status": "ok"},
            action_cache=[],
        )
        assert result["description"] is None
        assert result["schema"] is None
        assert result["response"] == {"status": "ok"}


class TestSessionChannel:
    """Lightfall routes trusted actions only on {session_subject}.{action}."""

    DENIED = {
        "status": "error",
        "code": "denied",
        "message": "This action requires a session capability channel; "
        "complete the auth.request handshake and use the session subject.",
        "contract_version": 1,
    }

    def test_session_call_subject(self):
        from lightfall_bridge.server import session_call_subject

        assert session_call_subject("_PRIV.abc.session.tok", "commands.device.search") == (
            "_PRIV.abc.session.tok.commands.device.search"
        )

    def test_needs_session_channel(self):
        from lightfall_bridge.server import needs_session_channel

        assert needs_session_channel(self.DENIED)
        assert not needs_session_channel({"status": "ok"})
        assert not needs_session_channel({"status": "error", "code": "denied", "message": "nope"})
        assert not needs_session_channel(["not", "a", "dict"])

    def test_session_expired(self):
        from lightfall_bridge.server import session_expired

        assert session_expired(
            {"status": "error", "code": "denied", "message": "Invalid or expired session token"}
        )
        assert not session_expired(self.DENIED)

    def test_state_keeps_session_subjects(self):
        from lightfall_bridge.server import BridgeState

        assert BridgeState(nc=None, default_prefix="").session_subjects == {}


class _Msg:
    def __init__(self, obj):
        self.data = json.dumps(obj).encode()


class _FakeNc:
    """Fake NATS client scripting meta.actions, the private handshake and calls."""

    def __init__(self, replies):
        self.replies = replies  # subject -> list of replies (popped in order)
        self.calls = []

    async def request(self, subject, payload=b"", timeout=0.5):
        self.calls.append(subject)
        return _Msg(self.replies[subject].pop(0))


APPROVED = "approved"


def _approved(token):
    return {
        "status": "approved",
        "session_token": token,
        "session_subject": f"_PRIV.p.session.{token}",
    }


class TestExecuteActionHandshake:
    DENIED = TestSessionChannel.DENIED

    def _run(self, nc, tmp_ctx_state):
        from lightfall_bridge.server import create_server

        mcp = create_server("nats://x", "pre")

        async def go():
            tool = await mcp.get_tool("execute_action")
            ctx = types.SimpleNamespace(lifespan_context={"bridge": tmp_ctx_state})
            return json.loads(await tool.fn(action="commands.dev.get", params={}, ctx=ctx))

        return asyncio.run(go())

    def test_handshake_on_private_root_and_session_subject_call(self):
        from lightfall_bridge.server import BridgeState

        nc = _FakeNc(
            {
                "pre.meta.actions": [{"priv_root": "_PRIV.p", "actions": []}],
                "_PRIV.p.auth.request": [_approved("t1")],
                "pre.commands.dev.get": [self.DENIED],
                "_PRIV.p.session.t1.commands.dev.get": [{"status": "ok"}],
            }
        )
        state = BridgeState(nc=nc, default_prefix="pre")
        out = self._run(nc, state)
        assert out["response"] == {"status": "ok"}
        assert state.session_subjects["pre"] == "_PRIV.p.session.t1"
        assert "pre.auth.request" not in nc.calls

    def test_expired_session_rehandshakes(self):
        from lightfall_bridge.server import BridgeState

        expired = {"status": "error", "code": "denied", "message": "Invalid or expired session token"}
        nc = _FakeNc(
            {
                "pre.meta.actions": [{"priv_root": "_PRIV.p"}],
                "_PRIV.p.auth.request": [_approved("t2")],
                "pre.commands.dev.get": [expired],
                "_PRIV.p.session.t2.commands.dev.get": [{"status": "ok"}],
            }
        )
        state = BridgeState(nc=nc, default_prefix="pre")
        state.auth_state["pre"] = "approved"
        state.session_subjects["pre"] = "_PRIV.p.session.old"
        out = self._run(nc, state)
        assert out["response"] == {"status": "ok"}
        assert state.session_subjects["pre"] == "_PRIV.p.session.t2"
