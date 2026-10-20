"""Per-connection NATS capability helpers (stdlib only; vendor byte-for-byte).

Every connection sends a random secret as its NATS connect token.  The broker's
auth callout derives the same prefix ``p`` from it and grants subscribe rights on
``_INBOX.<p>.>`` and ``_PRIV.<p>.>``.  This file must stay free of Qt, lightfall
and third-party imports so other repos can copy it verbatim.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any

VENDORED_FROM = "lightfall_utils.ipc.capability v1"

_DOMAIN = "lightfall-nats-v1:"
_PREFIX_LEN = 26


class PrivateRootUnavailable(RuntimeError):
    """The service did not advertise a ``priv_root`` in its meta.actions reply."""


def derive_prefix(secret: str) -> str:
    """Return base32(sha256(domain + secret)), lowercase, unpadded, 26 chars."""
    digest = hashlib.sha256((_DOMAIN + secret).encode("utf-8")).digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()[:_PREFIX_LEN]


def new_token() -> str:
    """Return a 32-byte URL-safe random string (43 chars)."""
    return secrets.token_urlsafe(32)


@dataclass(frozen=True)
class ConnectionCapability:
    secret: str
    prefix: str

    @property
    def inbox_prefix(self) -> str:
        return f"_INBOX.{self.prefix}"

    @property
    def priv_root(self) -> str:
        return f"_PRIV.{self.prefix}"

    def connect_kwargs(self) -> dict[str, str]:
        return {"token": self.secret, "inbox_prefix": self.inbox_prefix}


def new_capability() -> ConnectionCapability:
    secret = new_token()
    return ConnectionCapability(secret=secret, prefix=derive_prefix(secret))


async def private_auth_request(
    nc: Any, topic_prefix: str, payload: dict, timeout: float = 5.0
) -> dict:
    """Discover the private root via ``meta.actions`` then send ``auth.request`` there."""
    msg = await nc.request(f"{topic_prefix}.meta.actions", b"{}", timeout=timeout)
    info = json.loads(msg.data.decode())
    priv_root = info.get("priv_root") if isinstance(info, dict) else None
    if not priv_root:
        raise PrivateRootUnavailable(f"{topic_prefix}.meta.actions did not return priv_root")
    reply = await nc.request(
        f"{priv_root}.auth.request", json.dumps(payload).encode(), timeout=timeout
    )
    return json.loads(reply.data.decode())
