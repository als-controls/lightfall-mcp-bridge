# lightfall-mcp-bridge

MCP server that bridges [Claude Code](https://claude.ai/claude-code) to running [Lightfall](https://github.com/als-controls/lightfall) beamline control instances via [NATS](https://nats.io/).

## What it does

Provides three MCP tools:

- **`list_instances`** — Discover Lightfall instances on the NATS bus
- **`list_actions`** — Get available actions from a specific instance
- **`execute_action`** — Invoke an action (run plans, abort, query state, etc.)

Actions are discovered dynamically — the bridge never needs updating when Lightfall adds new capabilities.

## Installation

```bash
pip install -e .
```

## Configuration

Add to your Claude Code MCP config (`.mcp.json` or settings):

```json
{
  "lightfall": {
    "command": "python",
    "args": ["-m", "lightfall_bridge", "--nats-url", "nats://localhost:4222"]
  }
}
```

Optional: `--default-prefix als.7011` to set a default Lightfall instance.

## Requirements

- A running NATS server (local or remote)
- A running Lightfall instance with IPC enabled

## Development

```bash
python -m venv .venv
.venv/Scripts/activate  # or source .venv/bin/activate on Unix
pip install -e ".[dev]"
pytest
```

Integration tests require a NATS server at `localhost:4222` and are skipped otherwise.
