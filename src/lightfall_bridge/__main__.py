"""Entry point for ``python -m lightfall_bridge``."""

from __future__ import annotations

import argparse
import sys

from lightfall_bridge.server import create_server


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="lightfall-bridge",
        description="MCP server bridging Claude Code to Lightfall via NATS",
    )
    parser.add_argument(
        "--nats-url",
        required=True,
        help="NATS server URL (e.g. nats://localhost:4222)",
    )
    parser.add_argument(
        "--default-prefix",
        default="",
        help="Default topic prefix for Lightfall instance (e.g. als.7011)",
    )
    args = parser.parse_args(argv)

    server = create_server(
        nats_url=args.nats_url,
        default_prefix=args.default_prefix,
    )
    server.run()


if __name__ == "__main__":
    main()
