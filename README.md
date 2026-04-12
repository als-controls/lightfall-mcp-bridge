# lucid-mcp-bridge

MCP server that bridges [Claude Code](https://claude.ai/claude-code) to running [LUCID](https://git.als.lbl.gov/ncs/ncs) beamline control instances via [NATS](https://nats.io/).

## What it does

Provides three MCP tools:

- **`list_instances`** — Discover LUCID instances on the NATS bus
- **`list_actions`** — Get available actions from a specific instance
- **`execute_action`** — Invoke an action (run plans, abort, query state, etc.)

Actions are discovered dynamically — the bridge never needs updating when LUCID adds new capabilities.

## Installation

```bash
pip install -e .
```

## Configuration

Add to your Claude Code MCP config (`.mcp.json` or settings):

```json
{
  "lucid": {
    "command": "python",
    "args": ["-m", "lucid_bridge", "--nats-url", "nats://localhost:4222"]
  }
}
```

Optional: `--default-prefix als.7011` to set a default LUCID instance.

## Requirements

- A running NATS server (local or remote)
- A running LUCID instance with IPC enabled

## Development

```bash
python -m venv .venv
.venv/Scripts/activate  # or source .venv/bin/activate on Unix
pip install -e ".[dev]"
pytest
```

Integration tests require a NATS server at `localhost:4222` and are skipped otherwise.
