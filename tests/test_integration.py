"""Integration tests — require a running NATS server at localhost:4222."""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
import nats

from lightfall_bridge.server import BridgeState


@pytest_asyncio.fixture
async def bridge_state(nats_url, nats_available):
    """A connected BridgeState for direct NATS testing."""
    nc = await nats.connect(nats_url)
    state = BridgeState(nc=nc, default_prefix="")
    yield state
    await nc.drain()


class TestListInstances:
    @pytest.mark.asyncio
    async def test_discovers_mock_instance(self, mock_lightfall, bridge_state):
        import asyncio

        nc = bridge_state.nc
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        await nc.publish("_lightfall.discover", b"{}", reply=inbox)

        responses = []
        deadline = asyncio.get_event_loop().time() + 2.0
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(sub.next_msg(), timeout=remaining)
                responses.append(json.loads(msg.data))
            except asyncio.TimeoutError:
                break
        await sub.unsubscribe()

        prefixes = [r["prefix"] for r in responses]
        assert mock_lightfall["prefix"] in prefixes


class TestListActions:
    @pytest.mark.asyncio
    async def test_fetches_actions_from_mock(self, mock_lightfall, bridge_state):
        prefix = mock_lightfall["prefix"]
        nc = bridge_state.nc

        msg = await nc.request(f"{prefix}.meta.actions", b"{}", timeout=5.0)
        data = json.loads(msg.data)

        assert data["instance_id"] == mock_lightfall["instance_id"]
        assert data["prefix"] == prefix
        assert len(data["actions"]) == 1
        assert data["actions"][0]["subject"] == "commands.echo"


class TestExecuteAction:
    @pytest.mark.asyncio
    async def test_echo_round_trip(self, mock_lightfall, bridge_state):
        prefix = mock_lightfall["prefix"]
        nc = bridge_state.nc

        payload = json.dumps({"message": "hello"}).encode()
        msg = await nc.request(f"{prefix}.commands.echo", payload, timeout=5.0)
        data = json.loads(msg.data)

        assert data["echoed"]["message"] == "hello"


class TestAuthHandshake:
    @pytest.mark.asyncio
    async def test_auth_approved(self, mock_lightfall, bridge_state):
        prefix = mock_lightfall["prefix"]
        nc = bridge_state.nc

        payload = json.dumps({
            "app_name": "claude-code-bridge",
            "app_version": "0.1.0",
        }).encode()
        msg = await nc.request(f"{prefix}.auth.request", payload, timeout=5.0)
        data = json.loads(msg.data)

        assert data["status"] == "approved"
