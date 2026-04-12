"""Fixtures for integration tests requiring a NATS server."""

from __future__ import annotations

import asyncio
import json
import uuid

import nats
import pytest
import pytest_asyncio


@pytest.fixture(scope="session")
def nats_url():
    return "nats://localhost:4222"


@pytest_asyncio.fixture
async def nats_available(nats_url):
    """Skip test if NATS is not reachable."""
    try:
        nc = await nats.connect(nats_url)
        await nc.drain()
    except Exception:
        pytest.skip("NATS server not available at localhost:4222")


@pytest_asyncio.fixture
async def mock_lucid(nats_url, nats_available):
    """A mock LUCID instance on a unique prefix."""
    nc = await nats.connect(nats_url)
    prefix = f"test.{uuid.uuid4().hex[:8]}"

    instance_id = f"mock-{uuid.uuid4().hex[:6]}"
    display_name = "Mock LUCID"
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
        data = json.loads(msg.data)
        reply_data = json.dumps({"status": "approved"}).encode()
        if msg.reply:
            await nc.publish(msg.reply, reply_data)

    subs = [
        await nc.subscribe(f"{prefix}.meta.actions", cb=handle_meta),
        await nc.subscribe("_lucid.discover", cb=handle_discover),
        await nc.subscribe(f"{prefix}.commands.echo", cb=handle_echo),
        await nc.subscribe(f"{prefix}.auth.request", cb=handle_auth),
    ]

    yield {
        "prefix": prefix,
        "instance_id": instance_id,
        "display_name": display_name,
    }

    for sub in subs:
        await sub.unsubscribe()
    await nc.drain()
