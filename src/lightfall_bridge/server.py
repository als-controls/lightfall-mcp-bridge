"""FastMCP server bridging Claude Code to Lightfall via NATS."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field

import nats
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan

from lightfall_bridge.nats_capability import (
    PrivateNoResponders,
    new_capability,
    private_auth_request,
    private_request,
)

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
    # Per-instance capability-channel subject ("_PRIV.<p>.session.<token>")
    # from the approved auth reply.
    session_subjects: dict[str, str] = field(default_factory=dict)


_NO_RESPONDERS = (nats.errors.NoRespondersError, PrivateNoResponders)


def session_call_subject(session_subject: str, action: str) -> str:
    """The capability-channel subject Lightfall routes trusted actions on."""
    return f"{session_subject}.{action}"


def _denied_message(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    if response.get("status") != "error" or response.get("code") != "denied":
        return ""
    return str(response.get("message", ""))


def needs_session_channel(response: object) -> bool:
    """Lightfall refused a trusted action sent on the bare subject."""
    return "session capability channel" in _denied_message(response)


def session_expired(response: object) -> bool:
    """Lightfall no longer knows our channel token (re-login, trust revoked)."""
    return "invalid or expired session token" in _denied_message(response).lower()


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
            nc = await nats.connect(nats_url, **new_capability().connect_kwargs())
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
        payload = {
            "app_name": "claude-code-bridge",
            "app_version": "0.1.0",
        }
        try:
            data = await private_auth_request(
                state.nc, target, payload, timeout=AUTH_TIMEOUT
            )
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
            subject = data.get("session_subject")
            if subject:
                state.session_subjects[target] = subject
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

        # Execute. Public actions answer on the bare subject; trusted ones
        # (device, plan, agent, ...) only on the session capability channel.
        payload = json.dumps(params or {}).encode()

        last_raw: dict[str, bytes] = {}

        async def _send(subject: str) -> object:
            # Public subject: a plain request (its reply is forgeable, like discovery).
            msg = await state.nc.request(subject, payload, timeout=REQUEST_TIMEOUT)
            last_raw["data"] = msg.data
            return json.loads(msg.data)

        async def _send_session(subject: str) -> object:
            # Session channel: unguessable reply subject, so the reply can't be forged.
            try:
                return await private_request(
                    state.nc, subject, params or {}, timeout=REQUEST_TIMEOUT
                )
            except json.JSONDecodeError as exc:
                last_raw["data"] = exc.doc.encode()
                raise

        def _not_answering() -> str:
            return json.dumps({
                "error": f"Lightfall instance '{target}' is not answering on its "
                         "session channel (it may have restarted or logged out)"
            })

        async def _reset_auth() -> str | None:
            state.auth_state.pop(target, None)
            state.session_subjects.pop(target, None)
            return await _ensure_auth(state, target)

        def _finish(response: object) -> str:
            cache = state.action_cache.get(target, [])
            return json.dumps(format_execute_response(action, response, cache), indent=2)

        # Routing rule: only the public `meta.*` actions go to the bare subject
        # (its reply subject is predictable and forgeable via allow_responses
        # reflection). Every other action is trusted and is reachable only on
        # the session capability channel, sent with private_request (an
        # unguessable reply subject). Private actions (catalog "private": true,
        # e.g. auth.request) live on the private root and are never sent here.
        # _ensure_auth already ran, so a session subject is normally cached; if
        # an instance approved us without one (legacy, no auth callout) we fall
        # back to the bare subject.
        try:
            if action.startswith("meta.") or not state.session_subjects.get(target):
                try:
                    response = await _send(f"{target}.{action}")
                except nats.errors.NoRespondersError:
                    return json.dumps({
                        "error": f"No Lightfall instance is answering at '{target}'"
                    })
                if not (needs_session_channel(response) or session_expired(response)):
                    return _finish(response)
                if session_expired(response) or target not in state.session_subjects:
                    auth_error = await _reset_auth()
                    if auth_error:
                        return json.dumps({"error": auth_error})

            rehandshaked_on_dead = False
            for _attempt in range(3):
                subject = state.session_subjects.get(target)
                if not subject:
                    break
                try:
                    response = await _send_session(session_call_subject(subject, action))
                except _NO_RESPONDERS:
                    # Stale subject: the instance restarted (new _PRIV root) or
                    # logged out. Re-handshake once and retry on the fresh one.
                    if rehandshaked_on_dead:
                        return _not_answering()
                    rehandshaked_on_dead = True
                    auth_error = await _reset_auth()
                    if auth_error:
                        return json.dumps({"error": auth_error})
                    if not state.session_subjects.get(target):
                        return _not_answering()
                    continue
                if not session_expired(response):
                    break
                # Re-handshake for a fresh channel token and retry.
                auth_error = await _reset_auth()
                if auth_error:
                    return json.dumps({"error": auth_error})
        except nats.errors.TimeoutError:
            return json.dumps({
                "error": f"No response from '{target}' — Lightfall instance "
                         f"may be offline. Timeout: {REQUEST_TIMEOUT}s"
            })
        except json.JSONDecodeError:
            return json.dumps({
                "warning": "Response was not valid JSON",
                "raw": last_raw.get("data", b"").decode("utf-8", errors="replace"),
            })
        except Exception as exc:
            return json.dumps({"error": f"Request failed: {exc}"})

        return _finish(response)

    return mcp
