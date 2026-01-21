## Advanced

If you already lean on Codex every day and just need a little more control, this page collects the knobs you are most likely to reach for: tweak defaults in [Config](./config.md), add extra tools through [Model Context Protocol support](#model-context-protocol), and script full runs with [`codex exec`](./exec.md). Jump to the section you need and keep building.

## Config quickstart {#config-quickstart}

Most day-to-day tuning lives in `config.toml`: set approval + sandbox presets, pin model defaults, and add MCP server launchers. The [Config guide](./config.md) walks through every option and provides copy-paste examples for common setups.

## Tracing / verbose logging {#tracing-verbose-logging}

Because Codex is written in Rust, it honors the `RUST_LOG` environment variable to configure its logging behavior.

The TUI defaults to `RUST_LOG=codex_core=info,codex_tui=info,codex_rmcp_client=info` and log messages are written to `~/.codex/log/codex-tui.log`, so you can leave the following running in a separate terminal to monitor log messages as they are written:

```bash
tail -F ~/.codex/log/codex-tui.log
```

By comparison, the non-interactive mode (`codex exec`) defaults to `RUST_LOG=error`, but messages are printed inline, so there is no need to monitor a separate file.

See the Rust documentation on [`RUST_LOG`](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) for more information on the configuration options.

## Live session monitor (WebSocket) {#monitor}

Use the monitor server to watch for **active** Codex sessions and stream the latest assistant response as it is produced.

```bash
codex monitor --host 127.0.0.1 --port 8787
```

Connect with a WebSocket client (for example, [`websocat`](https://github.com/vi/websocat)):

```bash
websocat ws://127.0.0.1:8787/ws
```

Each message is a JSON snapshot containing only sessions that are considered **active**:

```json
{
  "sessions": [
    {
      "session_id": "thr_123",
      "thread_id": "thr_123",
      "turn_id": "turn_456",
      "cwd": "/Users/me/project",
      "last_response": "Streaming output so far…",
      "updated_at": "2025-01-01T00:00:04Z",
      "last_modified_ms": 1735689604000,
      "status": "working",
      "rollout_path": "/Users/me/.codex/sessions/2025/01/01/rollout-2025-01-01T00-00-00Z-...jsonl"
    }
  ]
}
```

### What “active” means

The monitor marks a session as active if **either**:

1) The rollout includes a live task lifecycle event (a `task_started` without a matching completion), **or**
2) The rollout file was modified recently (within `--active-window-seconds`).

This fallback is important for sessions that are still running but have not yet written a task lifecycle marker.

### Working vs idle

The payload includes a `status` field:

- `working` if the rollout was modified within the **working window**
- `idle` if the session is active but no recent write happened

You can override the working window with `--working-window-seconds` (default: 15).

### Fields

- `last_response` is the newest assistant text (partial while streaming).
- `updated_at` is the last recorded timestamp in the rollout file (from the JSONL line).
- `last_modified_ms` is the file modification time in Unix milliseconds (useful for recency).
- `status` is `working` or `idle` based on `last_modified_ms`.
- `rollout_path` points to the JSONL file under `~/.codex/sessions/`.

When no sessions are active, the `sessions` array is empty.

### Configure the active window

By default, the monitor treats sessions as active if their rollout file was updated within the last **120 seconds**. You can adjust the window:

```bash
codex monitor --host 127.0.0.1 --port 8787 --active-window-seconds 120
```

You can also adjust the working window:

```bash
codex monitor --host 127.0.0.1 --port 8787 --working-window-seconds 15
```

## Model Context Protocol (MCP) {#model-context-protocol}

The Codex CLI and IDE extension is a MCP client which means that it can be configured to connect to MCP servers. For more information, refer to the [`config docs`](./config.md#mcp-integration).

## Using Codex as an MCP Server {#mcp-server}

The Codex CLI can also be run as an MCP _server_ via `codex mcp-server`. For example, you can use `codex mcp-server` to make Codex available as a tool inside of a multi-agent framework like the OpenAI [Agents SDK](https://platform.openai.com/docs/guides/agents). Use `codex mcp` separately to add/list/get/remove MCP server launchers in your configuration.

### Codex MCP Server Quickstart {#mcp-server-quickstart}

You can launch a Codex MCP server with the [Model Context Protocol Inspector](https://modelcontextprotocol.io/legacy/tools/inspector):

```bash
npx @modelcontextprotocol/inspector codex mcp-server
```

Send a `tools/list` request and you will see that there are two tools available:

**`codex`** - Run a Codex session. Accepts configuration parameters matching the Codex Config struct. The `codex` tool takes the following properties:

| Property                | Type   | Description                                                                                                                                            |
| ----------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`prompt`** (required) | string | The initial user prompt to start the Codex conversation.                                                                                               |
| `approval-policy`       | string | Approval policy for shell commands generated by the model: `untrusted`, `on-failure`, `on-request`, `never`.                                           |
| `base-instructions`     | string | The set of instructions to use instead of the default ones.                                                                                            |
| `config`                | object | Individual [config settings](https://github.com/openai/codex/blob/main/docs/config.md#config) that will override what is in `$CODEX_HOME/config.toml`. |
| `cwd`                   | string | Working directory for the session. If relative, resolved against the server process's current directory.                                               |
| `model`                 | string | Optional override for the model name (e.g. `o3`, `o4-mini`).                                                                                           |
| `profile`               | string | Configuration profile from `config.toml` to specify default options.                                                                                   |
| `sandbox`               | string | Sandbox mode: `read-only`, `workspace-write`, or `danger-full-access`.                                                                                 |

**`codex-reply`** - Continue a Codex session by providing the conversation id and prompt. The `codex-reply` tool takes the following properties:

| Property                        | Type   | Description                                              |
| ------------------------------- | ------ | -------------------------------------------------------- |
| **`prompt`** (required)         | string | The next user prompt to continue the Codex conversation. |
| **`conversationId`** (required) | string | The id of the conversation to continue.                  |

### Trying it Out {#mcp-server-trying-it-out}

> [!TIP]
> Codex often takes a few minutes to run. To accommodate this, adjust the MCP inspector's Request and Total timeouts to 600000ms (10 minutes) under ⛭ Configuration.

Use the MCP inspector and `codex mcp-server` to build a simple tic-tac-toe game with the following settings:

**approval-policy:** never

**prompt:** Implement a simple tic-tac-toe game with HTML, JavaScript, and CSS. Write the game in a single file called index.html.

**sandbox:** workspace-write

Click "Run Tool" and you should see a list of events emitted from the Codex MCP server as it builds the game.
