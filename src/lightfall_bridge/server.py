"""FastMCP server bridging Claude Code to Lightfall via NATS."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field

import nats
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan

logger = logging.getLogger(__name__)

DISCOVER_SUBJECT = "_lightfall.discover"
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
            "available Lightfall instances, or set --default-prefix."
        )
    return result


def format_execute_response(
    action: str,
    response: dict,
    action_cache: list[dict],
) -> dict:
    """Wrap a Lightfall response with cached action metadata."""
    match = next(
        (a for a in action_cache if a.get("subject") == action),
        None,
    )
    return {
        "action": action,
        "description": match["description"] if match else None,
        "schema": match.get("schema") if match else None,
        "response": response,
    }


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

    mcp = FastMCP("Lightfall Bridge", lifespan=nats_lifespan)

    def _get_state(ctx: Context) -> BridgeState:
        return ctx.lifespan_context["bridge"]

    @mcp.tool
    async def list_instances(ctx: Context) -> str:
        """Discover Lightfall instances on the NATS bus.

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

    @mcp.tool
    async def list_actions(prefix: str = "", ctx: Context = None) -> str:
        """Get available actions from a specific Lightfall instance.

        Args:
            prefix: Topic prefix of the Lightfall instance (e.g. "als.7011").
                    Falls back to the configured default prefix.
        """
        state = _get_state(ctx)
        if state.nc is None:
            return json.dumps({"error": f"Cannot connect to NATS at {nats_url}"})

        try:
            target = resolve_prefix(prefix, state.default_prefix)
        except ValueError as exc:
            return json.dumps({"error": str(exc)})

        # Auth handshake
        auth_error = await _ensure_auth(state, target)
        if auth_error:
            return json.dumps({"error": auth_error})

        subject = f"{target}.meta.actions"
        try:
            msg = await state.nc.request(
                subject, b"{}", timeout=REQUEST_TIMEOUT
            )
            data = json.loads(msg.data)
        except nats.errors.TimeoutError:
            return json.dumps({
                "error": f"No response from '{target}' — Lightfall instance "
                         f"may be offline. Timeout: {REQUEST_TIMEOUT}s"
            })
        except Exception as exc:
            return json.dumps({"error": f"Request failed: {exc}"})

        # Cache the action metadata for execute_action
        actions = data.get("actions", [])
        state.action_cache[target] = actions

        return json.dumps(data, indent=2)

    async def _ensure_auth(state: BridgeState, target: str) -> str | None:
        """Perform auth handshake if needed. Returns error string or None."""
        auth = state.auth_state.get(target)
        if auth == "approved":
            return None
        if auth == "denied":
            return (
                f"Lightfall instance at '{target}' denied access. "
                "Approve the trust prompt in Lightfall to continue."
            )
        # First contact — do handshake
        subject = f"{target}.auth.request"
        payload = json.dumps({
            "app_name": "claude-code-bridge",
            "app_version": "0.1.0",
        }).encode()
        try:
            msg = await state.nc.request(subject, payload, timeout=AUTH_TIMEOUT)
            data = json.loads(msg.data)
        except nats.errors.TimeoutError:
            return (
                f"Auth handshake timed out for '{target}'. "
                "Check that Lightfall is running and respond to the trust prompt."
            )
        except Exception as exc:
            return f"Auth handshake failed: {exc}"

        status = data.get("status")
        if status == "approved":
            state.auth_state[target] = "approved"
            return None
        else:
            reason = data.get("reason", "denied by operator")
            state.auth_state[target] = "denied"
            return (
                f"Lightfall instance at '{target}' denied access: {reason}. "
                "Approve the trust prompt in Lightfall to continue."
            )

    @mcp.tool
    async def execute_action(
        action: str,
        params: dict | None = None,
        prefix: str = "",
        ctx: Context = None,
    ) -> str:
        """Execute an action on a Lightfall instance.

        Args:
            action: Action subject suffix (e.g. "commands.plan.run").
            params: JSON payload to send with the action. Defaults to {}.
            prefix: Topic prefix of the Lightfall instance. Falls back to default.
        """
        state = _get_state(ctx)
        if state.nc is None:
            return json.dumps({"error": f"Cannot connect to NATS at {nats_url}"})

        try:
            target = resolve_prefix(prefix, state.default_prefix)
        except ValueError as exc:
            return json.dumps({"error": str(exc)})

        # Auth handshake
        auth_error = await _ensure_auth(state, target)
        if auth_error:
            return json.dumps({"error": auth_error})

        # Execute
        subject = f"{target}.{action}"
        payload = json.dumps(params or {}).encode()
        try:
            msg = await state.nc.request(subject, payload, timeout=REQUEST_TIMEOUT)
            response = json.loads(msg.data)
        except nats.errors.TimeoutError:
            return json.dumps({
                "error": f"No response from '{target}' — Lightfall instance "
                         f"may be offline. Timeout: {REQUEST_TIMEOUT}s"
            })
        except json.JSONDecodeError:
            return json.dumps({
                "warning": "Response was not valid JSON",
                "raw": msg.data.decode("utf-8", errors="replace"),
            })
        except Exception as exc:
            return json.dumps({"error": f"Request failed: {exc}"})

        cache = state.action_cache.get(target, [])
        result = format_execute_response(action, response, cache)
        return json.dumps(result, indent=2)

    return mcp
