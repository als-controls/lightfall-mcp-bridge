"""FastMCP server bridging Claude Code to LUCID via NATS."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field

import nats
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan

logger = logging.getLogger(__name__)

DISCOVER_SUBJECT = "_lucid.discover"
DISCOVER_TIMEOUT = 2.0
REQUEST_TIMEOUT = 5.0
AUTH_TIMEOUT = 10.0


@dataclass
class BridgeState:
    """Shared state for the bridge, stored in lifespan context."""
    nc: nats.NATS | None
    default_prefix: str
    action_cache: dict[str, list[dict]] = field(default_factory=dict)
    auth_state: dict[str, str] = field(default_factory=dict)


def resolve_prefix(prefix: str, default: str) -> str:
    """Return *prefix* if non-empty, else *default*. Raise if both empty."""
    result = prefix or default
    if not result:
        raise ValueError(
            "No prefix specified. Use list_instances to discover "
            "available LUCID instances, or set --default-prefix."
        )
    return result


def create_server(nats_url: str, default_prefix: str = "") -> FastMCP:
    """Build and return the FastMCP server."""

    @lifespan
    async def nats_lifespan(server):
        nc = None
        try:
            nc = await nats.connect(nats_url)
            logger.info("Connected to NATS at %s", nats_url)
        except Exception as exc:
            logger.warning("Failed to connect to NATS at %s: %s", nats_url, exc)
        state = BridgeState(nc=nc, default_prefix=default_prefix)
        try:
            yield {"bridge": state}
        finally:
            if nc:
                await nc.drain()

    mcp = FastMCP("LUCID Bridge", lifespan=nats_lifespan)

    def _get_state(ctx: Context) -> BridgeState:
        return ctx.lifespan_context["bridge"]

    @mcp.tool
    async def list_instances(ctx: Context) -> str:
        """Discover LUCID instances on the NATS bus.

        Broadcasts to all instances and collects responses over a
        2-second window. Returns a JSON array of discovered instances.
        """
        state = _get_state(ctx)
        if state.nc is None:
            return json.dumps({"error": f"Cannot connect to NATS at {nats_url}"})

        nc = state.nc
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        await nc.publish(DISCOVER_SUBJECT, b"{}", reply=inbox)

        responses: list[dict] = []
        deadline = asyncio.get_event_loop().time() + DISCOVER_TIMEOUT
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(
                    sub.next_msg(), timeout=remaining
                )
                data = json.loads(msg.data)
                responses.append({
                    "instance_id": data.get("instance_id"),
                    "display_name": data.get("display_name"),
                    "prefix": data.get("prefix"),
                    "actions_count": len(data.get("actions", [])),
                })
            except asyncio.TimeoutError:
                break
            except Exception as exc:
                logger.warning("Bad discover response: %s", exc)

        await sub.unsubscribe()
        return json.dumps(responses, indent=2)

    return mcp
