# `turnbus` — Local Multi-Agent Turn Bus & MCP Switchboard

`turnbus` is a lightweight, SQLite-backed local message bus and Model Context Protocol (MCP) server for coordinating multiple concurrent CLI coding agents (**Claude Code**, **OpenAI Codex CLI**, **Antigravity CLI**) and a human operator console.

Unlike passive mailbox MCP servers that require agents to poll in a loop, `turnbus` **actively wakes idle agent turns and steers active sessions** while keeping unmentioned group traffic completely passive (`0` LLM tokens).

---

## Key Features

1. **Dynamic Zero-Config Roster**:
   - No hardcoded agent names or paths. Agents self-register when their MCP server starts (`<tool>-<folder>` by default, or `--handle <name>` / `TURNBUS_HANDLE=<name>`).
   - Rename handles, set capitalized display names, or persist workspace defaults (`workspace + tool -> handle`) at runtime via MCP (`rename_agent`), CLI (`turnbus rename`), or the Web UI.
2. **Active Turn Wakeup & Busy-Session Steering**:
   - **Claude Code (`claude`)**: Emits session-bound `notifications/claude/channel` notifications over WebSocket when launched with `--dangerously-load-development-channels server:turnbus`.
   - **OpenAI Codex CLI (`codex`)**: Discovers loaded threads via `~/.codex/app-server-control/app-server-control.sock`, steers active turns (`turn/steer` with `expectedTurnId`), or queues and starts idle turns (`thread/queue/add` + `thread/queue/start` with `queuedSubmissionId`) while respecting approval gates (`waitingOnApproval`).
   - **Antigravity CLI (`agy`)**: Pushes messages via `agy agentapi send-message` using session-scoped language server coordinates (`ANTIGRAVITY_LS_ADDRESS` & `ANTIGRAVITY_CSRF_TOKEN`).
3. **First-Class Fan-Out Messaging (`fanout_message` & `check_fanout`)**:
   - Dispatch a prompt or review request to N agents in parallel with a single call and optionally await correlated replies across a barrier (`wait_for: "all" | "any" | k`).
4. **Runtime Group Channels with Mention-Gated Wakeups**:
   - Create, join, leave, and archive channels at runtime (`create_channel`, `join_channel`, `leave_channel`, `archive_channel`).
   - Posting to a channel without `@handle` delivers passively (`0` LLM tokens spent); mentioning `@handle` (outside fenced code blocks and blockquotes) wakes only the mentioned member(s).
5. **Loop Protection & Token Conservation**:
   - Built-in sliding-window wake rate limiter (default `15` wakes/min per recipient), unbroken agent-to-agent reply-chain depth cap (`10` hops), and a global **Pause Pushes** switch in the Web UI and CLI.
6. **Real-Time Operator Web UI (`http://127.0.0.1:8790`)**:
   - Live delivery state pills (`queued` $\rightarrow$ `pushed` $\rightarrow$ `acked`), DM/Channel/Fan-Out composer, runtime channel & handle management, and a toggleable **Auto-scroll: ON/OFF** stream viewer.

---

## How `turnbus` Compares to Existing Projects

| Capability | `turnbus` | `mcp_agent_mail` / `agmsg` | `hcom` |
| :--- | :---: | :---: | :---: |
| **SQLite WAL Durable Inbox & Channels** | Yes | Yes | Partial |
| **Claude Code Native Channel Wakeup** | Yes | No (poll) | Hooks only |
| **Codex Active-Turn Steering (`turn/steer`)** | Yes | No | No |
| **Antigravity `agentapi` Turn Wakeup** | Yes | No | No |
| **Parallel Fan-Out with Reply Barrier (`all`/`any`/`k`)** | Yes | No | No |
| **Mention-Gated Channel Wake (`0` tokens when unmentioned)** | Yes | N/A | No |
| **Per-Session Token Fencing & Loopback `Sec-Fetch-Site` Guard** | Yes | No | No |

---

## Quickstart

### 1. Install & Run Tests

```bash
bun install
bun test
```

### 2. Configure Your Agents' MCP Settings

Add `turnbus` to your agent CLI MCP configuration:

```json
{
  "mcpServers": {
    "turnbus": {
      "command": "bun",
      "args": ["run", "/path/to/turnbus/src/mcp-server.ts"]
    }
  }
}
```

*(Optional: pass `"--handle", "reviewer", "--display-name", "Reviewer"` in `args` or set `TURNBUS_HANDLE=reviewer`.)*

### 3. Open the Operator Web UI

```bash
bun run src/cli.ts ui
```

Open the printed `http://127.0.0.1:8790/#token=...` URL in your browser. The UI token is stored in `localStorage` and stripped from the URL bar automatically.

---

## Platform Internals & Fragile Dependencies (Honest Disclosure)

`turnbus` achieves zero-polling turn wakeup across three closed/semi-closed vendor CLIs by integrating with their local control interfaces. Keep these platform constraints in mind:

1. **Claude Code (`--dangerously-load-development-channels server:turnbus`)**:
   - Claude Code's `notifications/claude/channel` notification path requires launching `claude` with `--dangerously-load-development-channels server:turnbus` (a research-preview flag in Claude Code $\ge 2.1.x$) and discovers the parent session via `~/.claude/sessions/<pid>.json` + `/proc/<pid>/stat` start-time verification.
   - If launched without that flag, `turnbus` automatically detects `pushCapability: "none"` and degrades gracefully to standard MCP `check_messages()` inbox reads.
2. **OpenAI Codex CLI (`~/.codex/app-server-control/app-server-control.sock`)**:
   - Codex push delivery speaks JSON-RPC over Codex's local `app-server-control.sock` (`thread/loaded/list`, `thread/read`, `turn/steer`, `thread/queue/add`, `thread/queue/start`, `config/mcpServer/reload`). If future Codex releases change this internal RPC schema, `turnbus` holds messages safely in the SQLite inbox (`status = 'queued'`) rather than dropping them.
3. **Antigravity CLI (`agy agentapi send-message`)**:
   - Antigravity wakeup uses `agy agentapi send-message`, which requires the session's `ANTIGRAVITY_CONVERSATION_ID`, `ANTIGRAVITY_LS_ADDRESS`, and `ANTIGRAVITY_CSRF_TOKEN`. Because Antigravity does not pass `ANTIGRAVITY_LS_ADDRESS` to stdio MCP child processes, run `turnbus register` once from the agent's shell tool (or wire an Antigravity hook) so the daemon can cache the coordinates in `<stateDir>/agy-creds.json` (`0600`).
4. **Same-UID Local Security Boundary**:
   - `turnbus` binds strictly to `127.0.0.1`, validates `Host`, `Origin`, and `Sec-Fetch-Site` on every HTTP/WebSocket request, separates `agent.token` from `ui.token`, and requires per-session `x-turnbus-session-token` headers once a handle is bound.
   - However, if all local agents run under the same OS Unix user without container/UID sandboxing, any process with full filesystem access can read `~/.local/share/turnbus/ui.token`. For hard multi-tenant isolation against untrusted local processes, run agents under separate Unix UIDs or sandboxes.

---

## License

[MIT License](./LICENSE)
