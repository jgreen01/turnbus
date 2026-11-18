#!/usr/bin/env bun
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import WebSocket from 'ws'
import { randomUUID } from 'node:crypto'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { getDefaultStateDir, readAgentToken } from './auth.ts'
import { discoverLoadedCodexThreads } from './adapters/codex.ts'
import { ToolType } from './db.ts'
import {
  canonicalizePath,
  discoverClaudeSessionByParentWalk,
  isSameOrSubpath,
} from './roster.ts'

const __dirname = dirname(fileURLToPath(import.meta.url))

export interface AdapterRuntimeConfig {
  daemonUrl: string
  stateDir: string
  tool: ToolType
  serverName: string
  workspaceCwd: string
  explicitAgentId?: string
  displayName?: string
}

function parseCliArgs(argv: string[]): {
  tool?: ToolType
  agentId?: string
  displayName?: string
  serverName?: string
  daemonUrl?: string
} {
  const out: { tool?: ToolType; agentId?: string; displayName?: string; serverName?: string; daemonUrl?: string } = {}
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--tool' && argv[i + 1]) out.tool = argv[++i] as ToolType
    else if ((arg === '--agent-id' || arg === '--handle') && argv[i + 1]) out.agentId = argv[++i]
    else if (arg === '--display-name' && argv[i + 1]) out.displayName = argv[++i]
    else if (arg === '--server-name' && argv[i + 1]) out.serverName = argv[++i]
    else if (arg === '--daemon-url' && argv[i + 1]) out.daemonUrl = argv[++i]
  }
  return out
}

function detectToolFromParentProcess(): ToolType {
  const envTool = process.env.TURNBUS_TOOL?.toLowerCase()
  if (envTool === 'claude' || envTool === 'codex' || envTool === 'agy') return envTool
  if (process.env.ANTIGRAVITY_CONVERSATION_ID || process.env.ANTIGRAVITY_CLI_ALIAS) return 'agy'
  if (discoverClaudeSessionByParentWalk(process.ppid)) return 'claude'
  try {
    const commPath = `/proc/${process.ppid}/comm`
    if (existsSync(commPath)) {
      const comm = readFileSync(commPath, 'utf-8').trim().toLowerCase()
      if (comm.includes('claude')) return 'claude'
      if (comm.includes('codex')) return 'codex'
      if (comm.includes('agy') || comm.includes('antigravity')) return 'agy'
    }
  } catch {}
  return 'codex'
}

function discoverSingleAgyLockConversationId(): string | null {
  if (process.env.ANTIGRAVITY_CONVERSATION_ID) return process.env.ANTIGRAVITY_CONVERSATION_ID
  const presenceDir = join(homedir(), '.gemini', 'antigravity-cli', 'presence')
  if (!existsSync(presenceDir)) return null
  try {
    const locks = readdirSync(presenceDir).filter(f => f.endsWith('.lock'))
    if (locks.length === 1) return locks[0].slice(0, -5)
  } catch {}
  return null
}

export async function ensureDaemonRunning(daemonUrl: string): Promise<boolean> {
  const checkHealth = async (): Promise<boolean> => {
    try {
      const res = await fetch(`${daemonUrl}/api/health`, {
        headers: { Host: new URL(daemonUrl).host },
        signal: AbortSignal.timeout(1200),
      })
      if (!res.ok) return false
      const data = (await res.json()) as { product?: string; ok?: boolean }
      return Boolean(data?.ok && data?.product === 'turnbus')
    } catch {
      return false
    }
  }

  if (await checkHealth()) return true

  const daemonScript = join(__dirname, 'daemon.ts')
  try {
    const urlObj = new URL(daemonUrl)
    const proc = Bun.spawn([process.execPath, daemonScript], {
      env: {
        ...process.env,
        TURNBUS_PORT: urlObj.port || '8790',
      },
      stdout: 'ignore',
      stderr: 'ignore',
      stdin: 'ignore',
    })
    proc.unref()
  } catch {}

  for (let i = 0; i < 15; i++) {
    await new Promise(r => setTimeout(r, 150))
    if (await checkHealth()) return true
  }
  return false
}

export async function startTurnBusMcpServer(customConfig?: Partial<AdapterRuntimeConfig>): Promise<void> {
  const cli = parseCliArgs(process.argv.slice(2))
  const tool: ToolType = customConfig?.tool ?? cli.tool ?? detectToolFromParentProcess()
  const daemonUrl =
    customConfig?.daemonUrl ?? cli.daemonUrl ?? process.env.TURNBUS_URL ?? 'http://127.0.0.1:8790'
  const stateDir = customConfig?.stateDir ?? getDefaultStateDir()
  const workspaceCwd = customConfig?.workspaceCwd ?? process.cwd()
  const explicitAgentId = customConfig?.explicitAgentId ?? cli.agentId ?? process.env.TURNBUS_HANDLE
  const explicitDisplayName = customConfig?.displayName ?? cli.displayName ?? process.env.TURNBUS_DISPLAY_NAME
  const serverName = customConfig?.serverName ?? cli.serverName ?? 'turnbus'

  const agentToken = readAgentToken(stateDir)
  const daemonHost = new URL(daemonUrl).host

  function deriveInitialHandle(): string {
    if (explicitAgentId) return explicitAgentId.toLowerCase()
    const folder = basename(canonicalizePath(workspaceCwd) ?? workspaceCwd)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'session'
    return `${tool}-${folder}`.slice(0, 48)
  }

  let resolvedAgentId = deriveInitialHandle()
  let resolvedSessionId: string | null = null
  let sessionToken: string | null = null
  let rememberedNativeSessionId: string | null = null
  let channelCapable = false
  let initialCredsSent = false

  async function apiCall(
    path: string,
    method: 'GET' | 'POST',
    body?: unknown,
    timeoutMs = 15_000,
    retried = false
  ): Promise<any> {
    const headers: Record<string, string> = {
      Host: daemonHost,
      Authorization: `Bearer ${agentToken}`,
      'Content-Type': 'application/json',
    }
    if (sessionToken) headers['x-turnbus-session-token'] = sessionToken
    if (resolvedAgentId) headers['x-turnbus-caller'] = resolvedAgentId

    const res = await fetch(`${daemonUrl}${path}`, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(timeoutMs),
    })
    const json = await res.json().catch(() => ({}))
    if (!res.ok) {
      if (!retried && res.status === 403 && path !== '/api/agents/register' && sessionToken) {
        sessionToken = null
        await registerAndBind(true)
        return apiCall(path, method, body, timeoutMs, true)
      }
      throw new Error(json?.error || `HTTP ${res.status} on ${path}`)
    }
    return json
  }

  async function registerAndBind(isHeartbeat = false): Promise<void> {
    let nativeSessionId: string | null = null
    let endpointKey: string | null = null
    let pid: number | null = process.ppid
    let processStartedAt: string | null = null
    let effectiveCwd = workspaceCwd

    if (tool === 'claude') {
      const claudeSess = discoverClaudeSessionByParentWalk(process.ppid)
      if (claudeSess) {
        nativeSessionId = claudeSess.sessionId
        pid = claudeSess.pid
        processStartedAt = claudeSess.procStart
        effectiveCwd = claudeSess.cwd || workspaceCwd
        channelCapable = claudeSess.channelPushEnabled
      }
    } else if (tool === 'codex') {
      nativeSessionId = process.env.CODEX_THREAD_ID ?? process.env.CODEX_SESSION_ID ?? null
      if (!nativeSessionId) {
        try {
          const loaded = await discoverLoadedCodexThreads()
          const canonCwd = canonicalizePath(effectiveCwd)
          const matches = loaded.filter(t => {
            const tc = canonicalizePath(t.cwd)
            return canonCwd && tc && isSameOrSubpath(canonCwd, tc).match
          })
          if (matches.length === 1) {
            nativeSessionId = matches[0].threadId
            endpointKey = matches[0].endpointKey
            effectiveCwd = matches[0].cwd || effectiveCwd
          }
        } catch {}
      }
    } else if (tool === 'agy') {
      nativeSessionId = rememberedNativeSessionId ?? discoverSingleAgyLockConversationId()
    }

    const sendInitialCreds = !isHeartbeat && !initialCredsSent
    try {
      const reg = await apiCall('/api/agents/register', 'POST', {
        tool,
        workspaceCwd: effectiveCwd,
        explicitAgentId: resolvedSessionId ? resolvedAgentId : explicitAgentId,
        displayName: !isHeartbeat ? explicitDisplayName : undefined,
        nativeSessionId,
        endpointKey,
        pid,
        processStartedAt,
        channelCapable,
        lsAddress: sendInitialCreds ? (process.env.ANTIGRAVITY_LS_ADDRESS ?? null) : null,
        csrfToken: sendInitialCreds ? (process.env.ANTIGRAVITY_CSRF_TOKEN ?? null) : null,
        agentApiExe: sendInitialCreds ? (process.env.ANTIGRAVITY_AGENTAPI_EXE ?? null) : null,
        issueSessionToken: true,
      })
      if (reg?.ok !== false) {
        initialCredsSent = true
        if (nativeSessionId) rememberedNativeSessionId = nativeSessionId
        if (reg?.agentId) resolvedAgentId = reg.agentId
        if (reg?.session?.id) resolvedSessionId = reg.session.id
        if (typeof reg?.sessionToken === 'string' && reg.sessionToken) sessionToken = reg.sessionToken
        if (wsClient && wsClient.readyState === WebSocket.OPEN && resolvedAgentId) {
          wsClient.send(
            JSON.stringify({
              type: 'auth_adapter',
              token: agentToken,
              sessionToken,
              agentId: resolvedAgentId,
              sessionId: resolvedSessionId,
              channelCapable: tool === 'claude' ? channelCapable : false,
            })
          )
        }
      } else if (!resolvedSessionId) {
        resolvedAgentId = `${tool}-unbound-${randomUUID().slice(0, 6)}`
      }
    } catch {}
  }

  const mcp = new Server(
    { name: serverName, version: '1.0.0' },
    {
      capabilities: {
        ...(tool === 'claude' ? { experimental: { 'claude/channel': {} } } : {}),
        tools: {},
      },
      instructions: [
        `Connected to turnbus as '${resolvedAgentId}' (tool=${tool}).`,
        'Use send_message(to, message, wait_seconds?) to DM a peer agent or operator ("user").',
        'Use fanout_message(to=[...], message, wait_seconds?) to message multiple agents in parallel and optionally collect replies.',
        'Use reply_message(reply_to_id, message) to reply to a specific message ID.',
        'Use check_messages() to read your inbox and list_agents() to see active peers.',
        'Use create_channel, join_channel, post_channel, and read_channel for group channels.',
        'Avoid sending courtesy-only ack replies ("Got it, thanks") that wake another agent turn unnecessarily.',
      ].join(' '),
    }
  )

  let wsClient: WebSocket | null = null
  function connectDaemonWs(): void {
    const wsUrl = daemonUrl.replace(/^http/, 'ws') + '/ws'
    try {
      const ws = new WebSocket(wsUrl, { headers: { Host: daemonHost } })
      wsClient = ws
      ws.on('open', async () => {
        if (!sessionToken) await registerAndBind(true).catch(() => {})
        ws.send(
          JSON.stringify({
            type: 'auth_adapter',
            token: agentToken,
            sessionToken,
            agentId: resolvedAgentId,
            sessionId: resolvedSessionId,
            channelCapable: tool === 'claude' ? channelCapable : false,
          })
        )
      })
      ws.on('message', async raw => {
        try {
          const msg = JSON.parse(raw.toString())
          if (msg.type === 'claude_channel_push' && msg.requestId && msg.payload) {
            try {
              await mcp.notification({
                method: 'notifications/claude/channel',
                params: msg.payload,
              })
              ws.send(JSON.stringify({ type: 'claude_channel_ack', requestId: msg.requestId, ok: true }))
            } catch (err: any) {
              ws.send(
                JSON.stringify({
                  type: 'claude_channel_ack',
                  requestId: msg.requestId,
                  ok: false,
                  error: err?.message ?? String(err),
                })
              )
            }
          }
        } catch {}
      })
      ws.on('close', () => {
        wsClient = null
        sessionToken = null
        setTimeout(connectDaemonWs, 3000).unref?.()
      })
      ws.on('error', () => {})
    } catch {
      setTimeout(connectDaemonWs, 3000).unref?.()
    }
  }

  const initPromise = (async () => {
    await ensureDaemonRunning(daemonUrl)
    await registerAndBind(false)
    connectDaemonWs()
  })()

  setInterval(() => {
    void registerAndBind(true)
  }, 25_000).unref?.()

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: 'send_message',
        description:
          'Send a direct message (DM) to any agent handle or workspace selector (e.g. "claude:backend"). Set wait_seconds > 0 (max 110) only when you need an immediate correlated reply.',
        inputSchema: {
          type: 'object',
          properties: {
            to: { type: 'string', description: 'Recipient handle or workspace selector' },
            message: { type: 'string', description: 'Message content' },
            wait_seconds: { type: 'number', description: 'Seconds to wait for a correlated reply (0..110, default 0)' },
            reply_to_id: { type: 'string', description: 'Optional message ID being replied to' },
            client_message_id: { type: 'string', description: 'Optional idempotency key for safe retries' },
          },
          required: ['to', 'message'],
        },
      },
      {
        name: 'fanout_message',
        description:
          'Send a message in parallel to multiple recipient handles (fan-out) and optionally wait for correlated replies from all, any, or k recipients.',
        inputSchema: {
          type: 'object',
          properties: {
            to: {
              type: 'array',
              items: { type: 'string' },
              description: 'Array of recipient handles',
            },
            message: { type: 'string', description: 'Message content sent to each recipient' },
            wait_seconds: { type: 'number', description: 'Optional barrier wait in seconds (0..110, default 0)' },
            wait_for: {
              oneOf: [{ type: 'string', enum: ['all', 'any'] }, { type: 'number' }],
              description: 'Reply barrier mode when wait_seconds > 0: "all" (default), "any", or numeric quorum k',
            },
            wake: { type: 'boolean', description: 'Whether to actively wake recipients (default true)' },
            client_message_id: { type: 'string', description: 'Optional idempotency key' },
          },
          required: ['to', 'message'],
        },
      },
      {
        name: 'check_fanout',
        description: 'Check per-recipient delivery status and collect correlated replies for a fanout_id.',
        inputSchema: {
          type: 'object',
          properties: {
            fanout_id: { type: 'string', description: 'Fan-out ID returned by fanout_message' },
            wait_seconds: { type: 'number', description: 'Optional wait in seconds until all replies arrive (0..110)' },
            auto_ack: { type: 'boolean', description: 'Whether to acknowledge collected replies (default true)' },
          },
          required: ['fanout_id'],
        },
      },
      {
        name: 'reply_message',
        description:
          'Send a correlated reply to a specific message ID (automatically routes to the original sender and acknowledges the original message).',
        inputSchema: {
          type: 'object',
          properties: {
            reply_to_id: { type: 'string', description: 'Message ID to reply to' },
            message: { type: 'string', description: 'Reply content' },
          },
          required: ['reply_to_id', 'message'],
        },
      },
      {
        name: 'check_messages',
        description:
          'Check your turnbus inbox for unread direct, fan-out, and channel messages. Returns immediately by default (wait_seconds=0).',
        inputSchema: {
          type: 'object',
          properties: {
            wait_seconds: { type: 'number', description: 'Optional bounded wait in seconds (0..110, default 0)' },
            from: { type: 'string', description: 'Optional filter by sender handle' },
            limit: { type: 'number', description: 'Max messages to return (default 25)' },
            auto_ack: { type: 'boolean', description: 'Automatically mark returned messages as acked (default true)' },
          },
        },
      },
      {
        name: 'ack_messages',
        description: 'Explicitly acknowledge receipt of one or more message IDs.',
        inputSchema: {
          type: 'object',
          properties: {
            ids: { type: 'array', items: { type: 'string' }, description: 'Array of message IDs to acknowledge' },
          },
          required: ['ids'],
        },
      },
      {
        name: 'edit_message',
        description: 'Append an authorized revision to a previously sent message.',
        inputSchema: {
          type: 'object',
          properties: {
            message_id: { type: 'string', description: 'Message ID to revise' },
            text: { type: 'string', description: 'Updated message text' },
          },
          required: ['message_id', 'text'],
        },
      },
      {
        name: 'whoami',
        description: 'Return your current turnbus handle, display name, tool, workspace, and bound native session.',
        inputSchema: { type: 'object', properties: {} },
      },
      {
        name: 'list_agents',
        description: 'List all registered agents on the bus, their tool type, workspace, live session binding, push capability, and unread count.',
        inputSchema: { type: 'object', properties: {} },
      },
      {
        name: 'bind_session',
        description:
          'Bind or update your agent handle, display name, workspace default, or native session ID. All fields are optional; omit native_session_id to keep your auto-discovered session.',
        inputSchema: {
          type: 'object',
          properties: {
            agent_id: { type: 'string', description: 'Handle to claim/bind (e.g. "atlas")' },
            display_name: { type: 'string', description: 'Human-friendly display name (e.g. "Atlas")' },
            set_workspace_default: {
              type: 'boolean',
              description: 'Persist this handle as the default for (workspace, tool) across restarts (default true when agent_id is provided)',
            },
            native_session_id: { type: 'string', description: 'Optional native threadId / sessionId / conversationId' },
          },
        },
      },
      {
        name: 'rename_agent',
        description: 'Rename your agent handle, update your display name, or persist your handle as the workspace default.',
        inputSchema: {
          type: 'object',
          properties: {
            new_handle: { type: 'string', description: 'Optional new handle ID' },
            display_name: { type: 'string', description: 'Optional new capitalized display name' },
            aliases: { type: 'array', items: { type: 'string' }, description: 'Optional alias handles' },
            set_workspace_default: {
              type: 'boolean',
              description: 'Save this handle as the default for your current workspace and tool',
            },
          },
        },
      },
      {
        name: 'list_channels',
        description: 'List active group channels and their member handles/wake policies.',
        inputSchema: {
          type: 'object',
          properties: {
            include_archived: { type: 'boolean', description: 'Include archived channels (default false)' },
          },
        },
      },
      {
        name: 'create_channel',
        description: 'Create a group channel at runtime and optionally invite member handles.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name (e.g. "#backend")' },
            description: { type: 'string', description: 'Channel description' },
            wake_mode: { type: 'string', enum: ['mention', 'always', 'passive'], description: 'Your wake mode in the channel' },
            members: { type: 'array', items: { type: 'string' }, description: 'Optional member handles to add' },
          },
          required: ['channel'],
        },
      },
      {
        name: 'join_channel',
        description: 'Join a group channel (creating it if it does not exist) with an optional wake_mode.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name (e.g. "#backend")' },
            wake_mode: { type: 'string', enum: ['mention', 'always', 'passive'], description: 'Wake mode (default "mention")' },
          },
          required: ['channel'],
        },
      },
      {
        name: 'leave_channel',
        description: 'Leave a group channel.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name (e.g. "#backend")' },
          },
          required: ['channel'],
        },
      },
      {
        name: 'set_channel_wake_mode',
        description: 'Update your wake_mode ("mention", "always", or "passive") for a channel.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name' },
            wake_mode: { type: 'string', enum: ['mention', 'always', 'passive'] },
          },
          required: ['channel', 'wake_mode'],
        },
      },
      {
        name: 'archive_channel',
        description: 'Archive or unarchive a group channel.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name' },
            archived: { type: 'boolean', description: 'True to archive (default), false to unarchive' },
          },
          required: ['channel'],
        },
      },
      {
        name: 'post_channel',
        description:
          'Post a message to a group channel. Only @mentioned members (or members with wake_mode="always") are actively woken; unmentioned members receive the post passively.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name (e.g. "#general")' },
            message: { type: 'string', description: 'Message content' },
            reply_to_id: { type: 'string', description: 'Optional channel message ID being replied to' },
            client_message_id: { type: 'string', description: 'Optional idempotency key' },
          },
          required: ['channel', 'message'],
        },
      },
      {
        name: 'read_channel',
        description: 'Read messages in a group channel and acknowledge returned messages for your handle.',
        inputSchema: {
          type: 'object',
          properties: {
            channel: { type: 'string', description: 'Channel name (e.g. "#general")' },
            limit: { type: 'number', description: 'Max messages to return (default 20)' },
            after_seq: { type: 'number', description: 'Optional forward sequence cursor' },
          },
          required: ['channel'],
        },
      },
    ],
  }))

  mcp.setRequestHandler(CallToolRequestSchema, async req => {
    await initPromise
    const name = req.params.name
    const args = (req.params.arguments ?? {}) as Record<string, any>

    try {
      if (name === 'whoami') {
        const rosterRes = await apiCall('/api/agents', 'GET')
        const me = (rosterRes.agents ?? []).find((a: any) => a.id === resolvedAgentId)
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  agentId: resolvedAgentId,
                  tool,
                  workspaceCwd,
                  sessionId: resolvedSessionId,
                  status: me ?? null,
                },
                null,
                2
              ),
            },
          ],
        }
      }

      if (name === 'list_agents') {
        const res = await apiCall('/api/agents', 'GET')
        return { content: [{ type: 'text', text: JSON.stringify(res.agents ?? [], null, 2) }] }
      }

      if (name === 'bind_session') {
        const requestedNativeId = args.native_session_id
          ? String(args.native_session_id)
          : rememberedNativeSessionId
        const setDefault =
          args.set_workspace_default !== undefined
            ? Boolean(args.set_workspace_default)
            : Boolean(args.agent_id)
        const reg = await apiCall('/api/agents/register', 'POST', {
          tool,
          workspaceCwd,
          explicitAgentId: args.agent_id ? String(args.agent_id) : resolvedAgentId,
          displayName: args.display_name ? String(args.display_name) : undefined,
          setWorkspaceDefault: setDefault,
          nativeSessionId: requestedNativeId,
          lsAddress: null,
          csrfToken: null,
          agentApiExe: process.env.ANTIGRAVITY_AGENTAPI_EXE ?? null,
          issueSessionToken: true,
        })
        if (reg?.ok !== false) {
          if (requestedNativeId) rememberedNativeSessionId = requestedNativeId
          if (reg?.agentId) resolvedAgentId = reg.agentId
          if (reg?.session?.id) resolvedSessionId = reg.session.id
          if (typeof reg?.sessionToken === 'string' && reg.sessionToken) sessionToken = reg.sessionToken
        }
        return {
          content: [{ type: 'text', text: JSON.stringify(reg, null, 2) }],
          isError: reg?.ok === false,
        }
      }

      if (name === 'rename_agent') {
        const setDefault =
          args.set_workspace_default !== undefined ? Boolean(args.set_workspace_default) : true
        const res = await apiCall('/api/agents/rename', 'POST', {
          caller_id: resolvedAgentId,
          old_handle: resolvedAgentId,
          new_handle: args.new_handle ? String(args.new_handle) : undefined,
          display_name: args.display_name ? String(args.display_name) : undefined,
          aliases: Array.isArray(args.aliases) ? args.aliases.map(String) : undefined,
          set_workspace_default: setDefault,
        })
        if (res?.agent?.id) resolvedAgentId = res.agent.id
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'send_message') {
        const waitSec = Math.max(0, Math.min(Number(args.wait_seconds ?? 0), 110))
        const clientMessageId = args.client_message_id ? String(args.client_message_id) : randomUUID()
        const res = await apiCall(
          '/api/messages/send',
          'POST',
          {
            caller_id: resolvedAgentId,
            caller_session_id: resolvedSessionId,
            to: String(args.to),
            message: String(args.message),
            reply_to_id: args.reply_to_id ? String(args.reply_to_id) : null,
            client_message_id: clientMessageId,
            wait_seconds: waitSec,
          },
          (waitSec + 10) * 1000
        )

        if (res.status === 'replied' && res.reply) {
          return {
            content: [
              {
                type: 'text',
                text: `[Reply from ${res.reply.from} | msg #${res.reply.message_id} | in reply to #${res.message_id}]\n${res.reply.body}`,
              },
            ],
          }
        }
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'fanout_message') {
        const waitSec = Math.max(0, Math.min(Number(args.wait_seconds ?? 0), 110))
        const targets = Array.isArray(args.to) ? args.to.map(String) : []
        const res = await apiCall(
          '/api/messages/fanout',
          'POST',
          {
            caller_id: resolvedAgentId,
            caller_session_id: resolvedSessionId,
            to: targets,
            message: String(args.message),
            client_message_id: args.client_message_id ? String(args.client_message_id) : randomUUID(),
            wake: args.wake !== false,
            wait_seconds: waitSec,
            wait_for: args.wait_for ?? 'all',
          },
          (waitSec + 10) * 1000
        )
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'check_fanout') {
        const waitSec = Math.max(0, Math.min(Number(args.wait_seconds ?? 0), 110))
        const res = await apiCall(
          '/api/messages/fanout-status',
          'POST',
          {
            caller_id: resolvedAgentId,
            fanout_id: String(args.fanout_id),
            wait_seconds: waitSec,
            auto_ack: args.auto_ack !== false,
          },
          (waitSec + 10) * 1000
        )
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'reply_message') {
        const replyToId = String(args.reply_to_id)
        const lookup = await apiCall(`/api/messages/lookup?id=${encodeURIComponent(replyToId)}`, 'GET')
        const origSender = lookup.message.sender_id
        const res = await apiCall('/api/messages/send', 'POST', {
          caller_id: resolvedAgentId,
          caller_session_id: resolvedSessionId,
          to: origSender,
          message: String(args.message),
          reply_to_id: lookup.message.id,
        })
        return {
          content: [
            {
              type: 'text',
              text: `Replied to ${origSender} (message_id=${res.message_id}, reply_to_id=${lookup.message.id}, delivery=${res.delivery_status})`,
            },
          ],
        }
      }

      if (name === 'check_messages') {
        const waitSec = Math.max(0, Math.min(Number(args.wait_seconds ?? 0), 110))
        const autoAck = args.auto_ack !== undefined ? Boolean(args.auto_ack) : true
        const res = await apiCall(
          '/api/messages/inbox',
          'POST',
          {
            caller_id: resolvedAgentId,
            from: args.from ? String(args.from) : undefined,
            limit: args.limit ? Number(args.limit) : 25,
            wait_seconds: waitSec,
            auto_ack: autoAck,
            ack_source: name,
          },
          (waitSec + 10) * 1000
        )
        const msgs = res.messages ?? []
        if (msgs.length === 0) {
          return { content: [{ type: 'text', text: 'No new messages.' }] }
        }
        const formatted = msgs
          .map((m: any) => {
            const target = m.channel_id ? `channel ${m.channel_id}` : m.fanout_id ? `fanout #${m.fanout_id.slice(0, 8)}` : 'DM'
            const replyPart = m.reply_to_id ? ` (reply_to: ${m.reply_to_id})` : ''
            return `[#${m.message_id} | ${target} from ${m.from}${replyPart} | ${new Date(m.created_at).toISOString()}]\n${m.body}`
          })
          .join('\n\n---\n\n')
        return { content: [{ type: 'text', text: formatted }] }
      }

      if (name === 'ack_messages') {
        const ids = Array.isArray(args.ids) ? args.ids.map(String) : []
        const res = await apiCall('/api/messages/ack', 'POST', {
          caller_id: resolvedAgentId,
          ids,
          ack_source: 'ack_messages_tool',
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'edit_message') {
        const res = await apiCall('/api/messages/edit', 'POST', {
          caller_id: resolvedAgentId,
          message_id: String(args.message_id),
          text: String(args.text),
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'list_channels') {
        const inc = args.include_archived ? '?include_archived=true' : ''
        const res = await apiCall(`/api/channels${inc}`, 'GET')
        return { content: [{ type: 'text', text: JSON.stringify(res.channels ?? [], null, 2) }] }
      }

      if (name === 'create_channel') {
        const res = await apiCall('/api/channels/create', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          description: args.description ? String(args.description) : undefined,
          wake_mode: args.wake_mode ? String(args.wake_mode) : undefined,
          members: Array.isArray(args.members) ? args.members.map(String) : undefined,
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'join_channel') {
        const res = await apiCall('/api/channels/join', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          wake_mode: args.wake_mode ? String(args.wake_mode) : undefined,
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'leave_channel') {
        const res = await apiCall('/api/channels/leave', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'set_channel_wake_mode') {
        const res = await apiCall('/api/channels/wake-mode', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          wake_mode: String(args.wake_mode),
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'archive_channel') {
        const res = await apiCall('/api/channels/archive', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          archived: args.archived !== false,
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'post_channel') {
        const res = await apiCall('/api/channels/post', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          message: String(args.message),
          reply_to_id: args.reply_to_id ? String(args.reply_to_id) : null,
          client_message_id: args.client_message_id ? String(args.client_message_id) : randomUUID(),
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      if (name === 'read_channel') {
        const res = await apiCall('/api/channels/read', 'POST', {
          caller_id: resolvedAgentId,
          channel: String(args.channel),
          limit: args.limit ? Number(args.limit) : 20,
          after_seq: args.after_seq !== undefined ? Number(args.after_seq) : undefined,
        })
        return { content: [{ type: 'text', text: JSON.stringify(res, null, 2) }] }
      }

      throw new Error(`Unknown tool: ${name}`)
    } catch (err: any) {
      return {
        content: [{ type: 'text', text: `turnbus error: ${err?.message ?? String(err)}` }],
        isError: true,
      }
    }
  })

  const transport = new StdioServerTransport()
  await mcp.connect(transport)
}

if (import.meta.main) {
  startTurnBusMcpServer().catch(err => {
    console.error('[turnbus-mcp] fatal:', err)
    process.exit(1)
  })
}
