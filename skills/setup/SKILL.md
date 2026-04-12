---
name: setup
description: Set up the LUCID MCP bridge — verify NATS connectivity, discover LUCID instances, and configure the default prefix. Use when the user says "connect to LUCID", "set up LUCID bridge", or after first installing this plugin.
---

# LUCID Bridge Setup

Guide the user through connecting Claude Code to a running LUCID instance.

## Steps

1. **Check NATS connectivity.** Use the `lucid__list_instances` tool. If it returns a connection error, ask the user to verify:
   - NATS server is running (e.g. `nats-server -p 4222`)
   - The `--nats-url` in `.mcp.json` matches their NATS server address

2. **Discover LUCID instances.** If `list_instances` returns results, show the user which instances are on the bus (display name, prefix, action count). If empty, LUCID may not be running or may not have IPC enabled — ask them to check LUCID's IPC settings (Settings > IPC > Server URL).

3. **Test communication.** Run `lucid__list_actions` with the chosen prefix. This triggers the trust handshake — tell the user to watch for the trust prompt in LUCID and approve it.

4. **Configure default prefix.** If there's only one instance, suggest updating `.mcp.json` to include `--default-prefix` so the user doesn't have to specify it on every call:
   ```json
   {
     "lucid": {
       "command": "python",
       "args": ["-m", "lucid_bridge", "--nats-url", "nats://localhost:4222", "--default-prefix", "als.7011"]
     }
   }
   ```

5. **Confirm.** Run a test action if one is available (e.g. echo) to verify end-to-end communication.

## Troubleshooting

- **"Cannot connect to NATS"** — NATS server isn't running or URL is wrong.
- **"No response from..."** — LUCID isn't running, IPC is disabled, or prefix is wrong.
- **"Auth handshake timed out"** — Trust prompt appeared in LUCID but wasn't answered within 10 seconds. Ask the user to try again and approve the prompt.
- **"denied access"** — Operator denied the trust prompt. Ask them to approve it, or check LUCID's trusted apps list in Settings > IPC.
