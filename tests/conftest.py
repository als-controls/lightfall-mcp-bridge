"""Fixtures for integration tests requiring a NATS server."""

from __future__ import annotations

import asyncio
import json
import uuid

import nats
import pytest
import pytest_asyncio

from lightfall_bridge.nats_capability import new_capability


@pytest.fixture(scope="session")
def nats_url():
    return "nats://localhost:4222"


@pytest_asyncio.fixture
async def nats_available(nats_url):
    """Skip test if NATS is not reachable."""
    try:
        nc = await nats.connect(nats_url, **new_capability().connect_kwargs())
        await nc.drain()
    except Exception:
        pytest.skip("NATS server not available at localhost:4222")


@pytest_asyncio.fixture
async def mock_lightfall(nats_url, nats_available):
    """A mock Lightfall instance on a unique prefix."""
    cap = new_capability()
    nc = await nats.connect(nats_url, **cap.connect_kwargs())
    prefix = f"test.{uuid.uuid4().hex[:8]}"

    instance_id = f"mock-{uuid.uuid4().hex[:6]}"
    display_name = "Mock Lightfall"
    actions = [
        {
            "subject": "commands.echo",
            "description": "Echo back params",
            "schema": {"message": "str"},
        },
    ]

    meta_response = json.dumps({
        "instance_id": instance_id,
        "display_name": display_name,
        "prefix": prefix,
        "priv_root": cap.priv_root,
        "actions": actions,
    }).encode()

    async def handle_meta(msg):
        if msg.reply:
            await nc.publish(msg.reply, meta_response)

    async def handle_discover(msg):
        if msg.reply:
            await nc.publish(msg.reply, meta_response)

    async def handle_echo(msg):
        data = json.loads(msg.data)
        reply_data = json.dumps({"echoed": data}).encode()
        if msg.reply:
            await nc.publish(msg.reply, reply_data)

    async def handle_auth(msg):
        reply_data = json.dumps({
            "status": "approved",
            "session_subject": f"{cap.priv_root}.session.tok",
        }).encode()
        if msg.reply:
            await nc.publish(msg.reply, reply_data)

    subs = [
        await nc.subscribe(f"{prefix}.meta.actions", cb=handle_meta),
        await nc.subscribe("_lightfall.discover", cb=handle_discover),
        await nc.subscribe(f"{prefix}.commands.echo", cb=handle_echo),
        await nc.subscribe(f"{cap.priv_root}.auth.request", cb=handle_auth),
    ]

    yield {
        "prefix": prefix,
        "instance_id": instance_id,
        "display_name": display_name,
    }

    for sub in subs:
        await sub.unsubscribe()
    await nc.drain()
