---
name: setup
description: Set up the Lightfall MCP bridge — verify NATS connectivity, discover Lightfall instances, and configure the default prefix. Use when the user says "connect to Lightfall", "set up Lightfall bridge", or after first installing this plugin.
---

# Lightfall Bridge Setup

Guide the user through connecting Claude Code to a running Lightfall instance.

## Steps

1. **Check NATS connectivity.** Use the `lightfall__list_instances` tool. If it returns a connection error, ask the user to verify:
   - NATS server is running (e.g. `nats-server -p 4222`)
   - The `--nats-url` in `.mcp.json` matches their NATS server address

2. **Discover Lightfall instances.** If `list_instances` returns results, show the user which instances are on the bus (display name, prefix, action count). If empty, Lightfall may not be running or may not have IPC enabled — ask them to check Lightfall's IPC settings (Settings > IPC > Server URL).

3. **Test communication.** Run `lightfall__list_actions` with the chosen prefix. This triggers the trust handshake — tell the user to watch for the trust prompt in Lightfall and approve it.

4. **Configure default prefix.** If there's only one instance, suggest updating `.mcp.json` to include `--default-prefix` so the user doesn't have to specify it on every call:
   ```json
   {
     "lightfall": {
       "command": "python",
       "args": ["-m", "lightfall_bridge", "--nats-url", "nats://localhost:4222", "--default-prefix", "als.7011"]
     }
   }
   ```

5. **Confirm.** Run a test action if one is available (e.g. echo) to verify end-to-end communication.

## Troubleshooting

- **"Cannot connect to NATS"** — NATS server isn't running or URL is wrong.
- **"No response from..."** — Lightfall isn't running, IPC is disabled, or prefix is wrong.
- **"Auth handshake timed out"** — Trust prompt appeared in Lightfall but wasn't answered within 10 seconds. Ask the user to try again and approve the prompt.
- **"denied access"** — Operator denied the trust prompt. Ask them to approve it, or check Lightfall's trusted apps list in Settings > IPC.
