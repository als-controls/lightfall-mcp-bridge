"""Diagnostic probe for live Lightfall via NATS.

1. Discover Lightfall instances
2. Auth handshake (operator must approve trust prompt in Lightfall UI)
3. List actions
4. Try to introspect RE state and the most recent start doc
"""
from __future__ import annotations

import asyncio
import json
import sys

import nats

NATS_URL = "nats://localhost:4222"
DISCOVER_SUBJECT = "lightfall.discover"
DISCOVER_TIMEOUT = 2.0
REQUEST_TIMEOUT = 5.0
AUTH_TIMEOUT = 30.0  # operator needs time to click "approve"


async def main():
    nc = await nats.connect(NATS_URL)
    print(f"Connected to NATS at {NATS_URL}\n")

    # 1. Discover
    print("=== Discovering Lightfall instances ===")
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox)
    await nc.publish(DISCOVER_SUBJECT, b"{}", reply=inbox)
    deadline = asyncio.get_event_loop().time() + DISCOVER_TIMEOUT
    instances = []
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            msg = await asyncio.wait_for(sub.next_msg(), timeout=remaining)
            d = json.loads(msg.data)
            instances.append(d)
            print(f"  - {d.get('display_name')} prefix={d.get('prefix')!r} "
                  f"actions={len(d.get('actions', []))}")
        except asyncio.TimeoutError:
            break
    await sub.unsubscribe()

    if not instances:
        print("No Lightfall instances found. Is Lightfall running with IPC enabled?")
        await nc.drain()
        return

    target = instances[0]["prefix"]
    print(f"\nUsing prefix: {target}\n")

    # 2. Auth
    print("=== Auth handshake (approve trust prompt in Lightfall) ===")
    try:
        msg = await nc.request(
            f"{target}.auth.request",
            json.dumps({"app_name": "claude-probe", "app_version": "0.1"}).encode(),
            timeout=AUTH_TIMEOUT,
        )
        auth = json.loads(msg.data)
        print(f"  status: {auth.get('status')} reason: {auth.get('reason')}")
        if auth.get("status") != "approved":
            print("Auth not approved; aborting.")
            await nc.drain()
            return
    except Exception as exc:
        print(f"Auth failed: {exc}")
        await nc.drain()
        return

    # 3. List actions
    print("\n=== Available actions ===")
    try:
        msg = await nc.request(
            f"{target}.meta.actions", b"{}", timeout=REQUEST_TIMEOUT,
        )
        actions_payload = json.loads(msg.data)
        actions = actions_payload.get("actions", [])
        for a in actions:
            print(f"  - {a.get('subject')}: {a.get('description', '')[:80]}")
    except Exception as exc:
        print(f"  Failed: {exc}")
        actions = []

    # 4. Try a few useful ones
    print("\n=== Probing RE state / preprocessors ===")
    candidates = [
        "engine.state",
        "engine.md",
        "engine.preprocessors",
        "documents.last_start",
        "documents.recent",
        "preferences.get",
    ]
    for cand in candidates:
        match = next((a for a in actions if cand in a.get("subject", "")), None)
        if not match:
            continue
        subject = match["subject"]
        params = {} if "preferences" not in subject else {"key": "tiled_beamline"}
        try:
            msg = await asyncio.wait_for(
                nc.request(f"{target}.{subject}", json.dumps(params).encode(),
                           timeout=REQUEST_TIMEOUT),
                timeout=REQUEST_TIMEOUT + 1,
            )
            data = msg.data.decode("utf-8", errors="replace")
            print(f"\n  {subject}:\n    {data[:1500]}")
        except Exception as exc:
            print(f"\n  {subject}: ERROR {exc}")

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
