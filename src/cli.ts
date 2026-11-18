#!/usr/bin/env bun
import { basename } from 'node:path'
import { getDefaultStateDir, initBusAuth, readAgentToken } from './auth.ts'
import { ensureDaemonRunning } from './mcp-server.ts'
import { canonicalizePath, discoverClaudeSessionByParentWalk } from './roster.ts'

async function main() {
  const args = process.argv.slice(2)
  const cmd = args[0] ?? 'help'
  const daemonUrl = process.env.TURNBUS_URL ?? 'http://127.0.0.1:8790'
  const stateDir = getDefaultStateDir()

  const healthy = await ensureDaemonRunning(daemonUrl)
  if (!healthy) {
    console.error(`Error: turnbus daemon at ${daemonUrl} failed health check (product !== 'turnbus')`)
    process.exit(1)
  }

  const auth = initBusAuth(stateDir)
  const token = readAgentToken(stateDir)
  const host = new URL(daemonUrl).host

  if (cmd === 'ui' || cmd === 'ui-url') {
    console.log(`${daemonUrl}/#token=${auth.uiToken}`)
    return
  }

  function inferCaller(): string {
    if (process.env.TURNBUS_HANDLE) return process.env.TURNBUS_HANDLE.toLowerCase()
    const folder = basename(canonicalizePath(process.cwd()) ?? process.cwd())
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'session'
    if (process.env.ANTIGRAVITY_CONVERSATION_ID) return `agy-${folder}`
    if (discoverClaudeSessionByParentWalk(process.ppid)) return `claude-${folder}`
    if (process.env.CODEX_THREAD_ID || process.env.CODEX_SESSION_ID) return `codex-${folder}`
    return 'user'
  }

  let sessionToken: string | null = null
  let registeredAgentId: string | null = null

  async function api(
    path: string,
    method: 'GET' | 'POST',
    body?: unknown,
    caller?: string,
    useUiToken = true,
    timeoutMs = 15_000
  ): Promise<any> {
    const includeSessionToken = Boolean(sessionToken && (!caller || caller === registeredAgentId))
    const res = await fetch(`${daemonUrl}${path}`, {
      method,
      headers: {
        Host: host,
        ...(useUiToken
          ? {
              'x-turnbus-ui-token': auth.uiToken,
              ...(caller ? { 'x-turnbus-caller': caller } : {}),
            }
          : {
              Authorization: `Bearer ${token}`,
              ...(includeSessionToken ? { 'x-turnbus-session-token': sessionToken! } : {}),
              ...(caller ? { 'x-turnbus-caller': caller } : {}),
            }),
        'Content-Type': 'application/json',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(timeoutMs),
    })
    const data = (await res.json().catch(() => ({}))) as any
    if (!res.ok) {
      console.error(`Error (${res.status}): ${data?.error || res.statusText}`)
      process.exit(1)
    }
    return data
  }

  async function autoRegisterCurrentEnv() {
    if (
      process.env.ANTIGRAVITY_CONVERSATION_ID &&
      process.env.ANTIGRAVITY_LS_ADDRESS &&
      process.env.ANTIGRAVITY_CSRF_TOKEN
    ) {
      const reg = await api(
        '/api/agents/register',
        'POST',
        {
          tool: 'agy',
          workspaceCwd: process.cwd(),
          explicitAgentId: process.env.TURNBUS_HANDLE ?? undefined,
          displayName: process.env.TURNBUS_DISPLAY_NAME ?? undefined,
          nativeSessionId: process.env.ANTIGRAVITY_CONVERSATION_ID,
          lsAddress: process.env.ANTIGRAVITY_LS_ADDRESS,
          csrfToken: process.env.ANTIGRAVITY_CSRF_TOKEN,
          agentApiExe: process.env.ANTIGRAVITY_AGENTAPI_EXE ?? null,
          issueSessionToken: false,
        },
        undefined,
        false
      ).catch(() => null)
      if (reg?.sessionToken) sessionToken = reg.sessionToken
      if (reg?.agentId) registeredAgentId = reg.agentId
      return reg
    }
    if (process.env.CODEX_THREAD_ID || process.env.CODEX_SESSION_ID) {
      const reg = await api(
        '/api/agents/register',
        'POST',
        {
          tool: 'codex',
          workspaceCwd: process.cwd(),
          explicitAgentId: process.env.TURNBUS_HANDLE ?? undefined,
          displayName: process.env.TURNBUS_DISPLAY_NAME ?? undefined,
          nativeSessionId: process.env.CODEX_THREAD_ID || process.env.CODEX_SESSION_ID,
          issueSessionToken: false,
        },
        undefined,
        false
      ).catch(() => null)
      if (reg?.sessionToken) sessionToken = reg.sessionToken
      if (reg?.agentId) registeredAgentId = reg.agentId
      return reg
    }
    return null
  }

  if (cmd === 'register') {
    const regRes = await autoRegisterCurrentEnv()
    console.log(JSON.stringify({ ok: true, caller: regRes?.agentId ?? inferCaller() }, null, 2))
    return
  }

  if (cmd === 'agents' || cmd === 'list-agents') {
    const res = await api('/api/agents', 'GET')
    console.log(JSON.stringify(res.agents, null, 2))
    return
  }

  if (cmd === 'rename') {
    const oldHandle = args[1]
    const newHandle = args[2]
    if (!oldHandle || !newHandle) {
      console.error('Usage: turnbus rename <old_handle> <new_handle> [--display-name <name>] [--default]')
      process.exit(1)
    }
    let displayName: string | undefined
    let setDefault = false
    for (let i = 3; i < args.length; i++) {
      if (args[i] === '--display-name' && args[i + 1]) displayName = args[++i]
      else if (args[i] === '--default') setDefault = true
    }
    const res = await api(
      '/api/agents/rename',
      'POST',
      {
        old_handle: oldHandle,
        new_handle: newHandle,
        display_name: displayName,
        set_workspace_default: setDefault,
      },
      undefined,
      true
    )
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'channels' || cmd === 'list-channels') {
    const res = await api('/api/channels', 'GET')
    console.log(JSON.stringify(res.channels, null, 2))
    return
  }

  if (cmd === 'create-channel') {
    const channel = args[1]
    if (!channel) {
      console.error('Usage: turnbus create-channel <#channel> [--desc <description>] [--members <a,b,c>]')
      process.exit(1)
    }
    let description = ''
    let members: string[] = []
    for (let i = 2; i < args.length; i++) {
      if (args[i] === '--desc' && args[i + 1]) description = args[++i]
      else if (args[i] === '--members' && args[i + 1]) members = args[++i].split(',').map(s => s.trim()).filter(Boolean)
    }
    const res = await api('/api/channels/create', 'POST', { channel, description, members }, undefined, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'join') {
    const channel = args[1]
    const agentId = args[2] ?? inferCaller()
    if (!channel) {
      console.error('Usage: turnbus join <#channel> [agent_id] [--wake mention|always|passive]')
      process.exit(1)
    }
    let wakeMode = 'mention'
    for (let i = 2; i < args.length; i++) {
      if (args[i] === '--wake' && args[i + 1]) wakeMode = args[++i]
    }
    const res = await api('/api/channels/join', 'POST', { channel, agent_id: agentId, wake_mode: wakeMode }, undefined, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'leave') {
    const channel = args[1]
    const agentId = args[2] ?? inferCaller()
    if (!channel) {
      console.error('Usage: turnbus leave <#channel> [agent_id]')
      process.exit(1)
    }
    const res = await api('/api/channels/leave', 'POST', { channel, agent_id: agentId }, undefined, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'send') {
    const to = args[1]
    const message = args[2]
    if (!to || !message) {
      console.error('Usage: turnbus send <to> <message> [--from <caller>] [--reply-to <id>] [--wait <sec>]')
      process.exit(1)
    }
    let from = inferCaller()
    let replyToId: string | undefined
    let waitSec = 0
    for (let i = 3; i < args.length; i++) {
      if (args[i] === '--from' && args[i + 1]) from = args[++i]
      else if (args[i] === '--reply-to' && args[i + 1]) replyToId = args[++i]
      else if (args[i] === '--wait' && args[i + 1]) waitSec = Number(args[++i])
    }
    const res = await api(
      '/api/messages/send',
      'POST',
      {
        caller_id: from,
        to,
        message,
        reply_to_id: replyToId ?? null,
        wait_seconds: waitSec,
      },
      from,
      true,
      (waitSec + 10) * 1000
    )
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'fanout') {
    const toList = (args[1] ?? '').split(',').map(s => s.trim()).filter(Boolean)
    const message = args[2]
    if (toList.length === 0 || !message) {
      console.error('Usage: turnbus fanout <handle1,handle2,...> <message> [--from <caller>] [--wait <sec>] [--wait-for all|any|<n>]')
      process.exit(1)
    }
    let from = inferCaller()
    let waitSec = 0
    let waitFor: 'all' | 'any' | number = 'all'
    for (let i = 3; i < args.length; i++) {
      if (args[i] === '--from' && args[i + 1]) from = args[++i]
      else if (args[i] === '--wait' && args[i + 1]) waitSec = Number(args[++i])
      else if (args[i] === '--wait-for' && args[i + 1]) {
        const val = args[++i]
        waitFor = val === 'all' || val === 'any' ? val : Number(val)
      }
    }
    const res = await api(
      '/api/messages/fanout',
      'POST',
      {
        caller_id: from,
        to: toList,
        message,
        wait_seconds: waitSec,
        wait_for: waitFor,
      },
      from,
      true,
      (waitSec + 10) * 1000
    )
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'fanout-status') {
    const fanoutId = args[1]
    if (!fanoutId) {
      console.error('Usage: turnbus fanout-status <fanout_id> [--wait <sec>]')
      process.exit(1)
    }
    let waitSec = 0
    for (let i = 2; i < args.length; i++) {
      if (args[i] === '--wait' && args[i + 1]) waitSec = Number(args[++i])
    }
    const res = await api(
      '/api/messages/fanout-status',
      'POST',
      { fanout_id: fanoutId, wait_seconds: waitSec },
      undefined,
      true,
      (waitSec + 10) * 1000
    )
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'inbox' || cmd === 'check') {
    let caller = inferCaller()
    let waitSec = 0
    for (let i = 1; i < args.length; i++) {
      if ((args[i] === '--for' || args[i] === '--from-agent') && args[i + 1]) caller = args[++i]
      else if (args[i] === '--wait' && args[i + 1]) waitSec = Number(args[++i])
    }
    const res = await api(
      '/api/messages/inbox',
      'POST',
      {
        caller_id: caller,
        wait_seconds: waitSec,
        auto_ack: true,
        ack_source: 'turnbus_cli',
      },
      caller,
      true,
      (waitSec + 10) * 1000
    )
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'post') {
    const channel = args[1]
    const message = args[2]
    if (!channel || !message) {
      console.error('Usage: turnbus post <#channel> <message> [--from <caller>]')
      process.exit(1)
    }
    let from = inferCaller()
    for (let i = 3; i < args.length; i++) {
      if (args[i] === '--from' && args[i + 1]) from = args[++i]
    }
    const res = await api('/api/channels/post', 'POST', { caller_id: from, channel, message }, from, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'read') {
    const channel = args[1]
    if (!channel) {
      console.error('Usage: turnbus read <#channel> [--for <caller>] [--limit <n>]')
      process.exit(1)
    }
    let caller = inferCaller()
    let limit = 20
    for (let i = 2; i < args.length; i++) {
      if (args[i] === '--for' && args[i + 1]) caller = args[++i]
      else if (args[i] === '--limit' && args[i + 1]) limit = Number(args[++i])
    }
    const res = await api('/api/channels/read', 'POST', { caller_id: caller, channel, limit }, caller, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  if (cmd === 'pause' || cmd === 'unpause') {
    const res = await api('/api/ui/pause', 'POST', { paused: cmd === 'pause' }, undefined, true)
    console.log(JSON.stringify(res, null, 2))
    return
  }

  console.log(`turnbus CLI
Commands:
  turnbus ui                                                   Print tokenized Web UI URL
  turnbus agents                                               List all registered agents and status
  turnbus rename <old> <new> [--display-name <name>] [--default] Rename agent handle / set display name
  turnbus channels                                             List all group channels
  turnbus create-channel <#channel> [--desc <text>]            Create a runtime group channel
  turnbus join <#channel> [agent_id] [--wake mention|always]   Add an agent to a channel
  turnbus leave <#channel> [agent_id]                          Remove an agent from a channel
  turnbus send <to> <message> [--wait <sec>]                   Send a direct message
  turnbus fanout <h1,h2,...> <message> [--wait <sec>]          Fan-out message to multiple agents in parallel
  turnbus fanout-status <fanout_id> [--wait <sec>]             Check fan-out status & replies
  turnbus inbox [--for <id>] [--wait <sec>]                    Read and ack unread messages
  turnbus post <#channel> <message>                            Post to a group channel
  turnbus read <#channel> [--limit <n>]                        Read group channel history
  turnbus pause | unpause                                      Pause or unpause native push wakeups`)
}

main()
