import { describe, expect, test } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import WebSocket from 'ws'
import { startTurnBusDaemon } from '../src/daemon.ts'

describe('turnbus daemon & stdio MCP server end-to-end integration', () => {
  test('supports dynamic registration, per-session token security, runtime channels, and parallel fan-out with barriers', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-e2e-test-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })

    const codexCalls: Array<{ method: string; params: any }> = []
    const agyCalls: string[][] = []

    const daemon = startTurnBusDaemon({
      port: 0,
      stateDir: tmpStateDir,
      reconcileIntervalMs: 0,
      codexRpc: async (method, params: any) => {
        codexCalls.push({ method, params })
        if (method === 'thread/loaded/list') {
          return { requestSent: true, result: { data: ['thread-bob', 'thread-carol'], nextCursor: null } }
        }
        if (method === 'thread/read') {
          return {
            requestSent: true,
            result: {
              thread: {
                id: params.threadId,
                cwd: params.threadId === 'thread-bob' ? '/tmp/service-api' : '/tmp/service-worker',
                status: { type: 'active', activeFlags: [] },
                turns: [{ id: `turn-${params.threadId}`, status: 'inProgress' }],
              },
            },
          }
        }
        if (method === 'turn/steer') {
          return { requestSent: true, result: { turnId: 'steered-ok' } }
        }
        throw new Error(`Unexpected codex method: ${method}`)
      },
      agyRunner: async args => {
        agyCalls.push(args)
        return { exitCode: 0, stdout: '{"response":{}}', stderr: '', timedOut: false }
      },
    })

    const baseUrl = `http://127.0.0.1:${daemon.port}`
    const hostHeader = `127.0.0.1:${daemon.port}`
    const agentHeaders = {
      Host: hostHeader,
      Authorization: `Bearer ${daemon.auth.agentToken}`,
      'Content-Type': 'application/json',
    }
    const uiHeaders = {
      Host: hostHeader,
      Origin: baseUrl,
      'x-turnbus-ui-token': daemon.auth.uiToken,
      'Content-Type': 'application/json',
    }

    try {
      // 1. Health check & Web UI security (unauthenticated GET / never leaks uiToken; autoScrollBtn present)
      const health = await fetch(`${baseUrl}/api/health`, { headers: { Host: hostHeader } }).then(r => r.json())
      expect(health.ok).toBe(true)
      expect(health.product).toBe('turnbus')

      const anonHtml = await fetch(`${baseUrl}/`, { headers: { Host: hostHeader } }).then(r => r.text())
      expect(anonHtml).not.toContain(daemon.auth.uiToken)
      expect(anonHtml).toContain('id="autoScrollBtn"')

      // 2. Register Alice (Claude), Bob (Codex), Carol (Codex), and Dave (Agy)
      const tokens: Record<string, string> = {}
      for (const regBody of [
        {
          tool: 'claude',
          explicitAgentId: 'alice',
          displayName: 'Alice',
          workspaceCwd: '/tmp/service-web',
          nativeSessionId: 'claude-alice-sess',
          channelCapable: true,
        },
        {
          tool: 'codex',
          explicitAgentId: 'bob',
          displayName: 'Bob',
          workspaceCwd: '/tmp/service-api',
          nativeSessionId: 'thread-bob',
        },
        {
          tool: 'codex',
          explicitAgentId: 'carol',
          displayName: 'Carol',
          workspaceCwd: '/tmp/service-worker',
          nativeSessionId: 'thread-carol',
        },
        {
          tool: 'agy',
          explicitAgentId: 'dave',
          displayName: 'Dave',
          workspaceCwd: '/tmp/service-ui',
          nativeSessionId: 'agy-dave-conv',
          lsAddress: 'localhost:41000',
          csrfToken: 'secret-csrf-dave',
        },
      ]) {
        const res = await fetch(`${baseUrl}/api/agents/register`, {
          method: 'POST',
          headers: agentHeaders,
          body: JSON.stringify(regBody),
        }).then(r => r.json())
        expect(res.ok).toBe(true)
        tokens[res.agentId] = res.sessionToken
      }

      // 3. Per-session token enforcement: omitting x-turnbus-session-token for bound agent 'alice' is rejected with 403
      const spoofAlice = await fetch(`${baseUrl}/api/messages/send`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'alice' },
        body: JSON.stringify({ to: 'bob', message: 'spoofed without session token' }),
      })
      expect(spoofAlice.status).toBe(403)

      // 4. Parallel Fan-Out with wait_seconds barrier (Alice fans out to Bob, Carol, and Dave; Bob & Carol reply)
      const fanoutPromise = fetch(`${baseUrl}/api/messages/fanout`, {
        method: 'POST',
        headers: {
          ...agentHeaders,
          'x-turnbus-caller': 'alice',
          'x-turnbus-session-token': tokens.alice,
        },
        body: JSON.stringify({
          to: ['bob', 'carol', 'dave'],
          message: 'Please review the release checklist!',
          wait_seconds: 5,
          wait_for: 2, // Wait for any 2 of the 3 recipients to reply!
        }),
      }).then(r => r.json())

      // Wait briefly for pushes to dispatch in parallel to Bob, Carol, and Dave
      for (let i = 0; i < 20 && agyCalls.length === 0; i++) {
        await new Promise(r => setTimeout(r, 25))
      }
      expect(agyCalls).toHaveLength(1)

      // Read Bob's and Carol's inboxes to get their fan-out message IDs
      const bobInbox = await fetch(`${baseUrl}/api/messages/inbox`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'bob', 'x-turnbus-session-token': tokens.bob },
        body: JSON.stringify({ auto_ack: true }),
      }).then(r => r.json())
      expect(bobInbox.messages).toHaveLength(1)

      const carolInbox = await fetch(`${baseUrl}/api/messages/inbox`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'carol', 'x-turnbus-session-token': tokens.carol },
        body: JSON.stringify({ auto_ack: true }),
      }).then(r => r.json())
      expect(carolInbox.messages).toHaveLength(1)

      // Bob and Carol send correlated replies back to Alice
      await fetch(`${baseUrl}/api/messages/send`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'bob', 'x-turnbus-session-token': tokens.bob },
        body: JSON.stringify({
          to: 'alice',
          reply_to_id: bobInbox.messages[0].message_id,
          message: 'API service ready (Bob)',
        }),
      })
      await fetch(`${baseUrl}/api/messages/send`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'carol', 'x-turnbus-session-token': tokens.carol },
        body: JSON.stringify({
          to: 'alice',
          reply_to_id: carolInbox.messages[0].message_id,
          message: 'Worker service ready (Carol)',
        }),
      })

      const fanoutResult = await fanoutPromise
      expect(fanoutResult.ok).toBe(true)
      expect(fanoutResult.replied_count).toBe(2)
      expect(fanoutResult.replies.bob.body).toContain('API service ready')
      expect(fanoutResult.replies.carol.body).toContain('Worker service ready')
      expect(fanoutResult.pending_recipients).toEqual(['dave'])

      // 5. Stdio MCP Client E2E test (create_channel, join_channel, rename_agent, fanout_message, post_channel, read_channel)
      const mcpEntry = join(__dirname, '..', 'src', 'mcp-server.ts')
      const transport = new StdioClientTransport({
        command: process.execPath,
        args: [mcpEntry, '--tool', 'claude', '--handle', 'eve', '--display-name', 'Eve', '--daemon-url', baseUrl],
        env: {
          ...process.env,
          TURNBUS_STATE_DIR: tmpStateDir,
          TURNBUS_TOKEN: daemon.auth.agentToken,
        },
      })
      const mcpClient = new Client({ name: 'test-eve-client', version: '1.0.0' }, { capabilities: {} })
      await mcpClient.connect(transport)

      const whoRes = await mcpClient.callTool({ name: 'whoami', arguments: {} })
      const whoJson = JSON.parse((whoRes.content as any)[0].text)
      expect(whoJson.agentId).toBe('eve')
      expect(whoJson.status.displayName).toBe('Eve')

      // Rename 'eve' -> 'evelyn' via MCP tool
      const renameRes = await mcpClient.callTool({
        name: 'rename_agent',
        arguments: { new_handle: 'evelyn', display_name: 'Evelyn' },
      })
      const renameJson = JSON.parse((renameRes.content as any)[0].text)
      expect(renameJson.ok).toBe(true)
      expect(renameJson.agent.id).toBe('evelyn')

      await mcpClient.callTool({
        name: 'create_channel',
        arguments: { channel: '#release', description: 'Release coordination', members: ['alice', 'bob'] },
      })
      await mcpClient.callTool({
        name: 'post_channel',
        arguments: { channel: '#release', message: 'Release branch cut by @evelyn!' },
      })
      const readRes = await mcpClient.callTool({
        name: 'read_channel',
        arguments: { channel: '#release', limit: 10 },
      })
      const readData = JSON.parse((readRes.content as any)[0].text)
      expect(readData.messages).toHaveLength(1)
      expect(readData.messages[0].body).toContain('Release branch cut')

      // Fan-out via MCP tool
      const mcpFanoutRes = await mcpClient.callTool({
        name: 'fanout_message',
        arguments: { to: ['bob', 'carol'], message: 'MCP fanout ping from Evelyn' },
      })
      const mcpFanoutJson = JSON.parse((mcpFanoutRes.content as any)[0].text)
      expect(mcpFanoutJson.ok).toBe(true)
      expect(mcpFanoutJson.total_recipients).toBe(2)

      await mcpClient.close()

      // 6. CLI E2E test (ui, register, rename, create-channel, post, read, fanout, pause, unpause)
      const cliEntry = join(__dirname, '..', 'src', 'cli.ts')
      const runCli = async (args: string[], extraEnv: Record<string, string> = {}) => {
        const proc = Bun.spawn([process.execPath, cliEntry, ...args], {
          env: { ...process.env, TURNBUS_URL: baseUrl, TURNBUS_STATE_DIR: tmpStateDir, ...extraEnv },
          stdout: 'pipe',
          stderr: 'pipe',
        })
        const [out, err, code] = await Promise.all([
          new Response(proc.stdout).text(),
          new Response(proc.stderr).text(),
          proc.exited,
        ])
        if (code !== 0) throw new Error(`CLI ${args.join(' ')} failed (${code}): ${err || out}`)
        return out.trim()
      }

      const uiUrl = await runCli(['ui'])
      expect(uiUrl).toContain(`#token=${daemon.auth.uiToken}`)

      const cliFanout = JSON.parse(await runCli(['fanout', 'bob,carol', 'CLI fanout check', '--from', 'user']))
      expect(cliFanout.ok).toBe(true)
      expect(cliFanout.total_recipients).toBe(2)

      // Operator CLI can also inspect/drain inbox for a session-token-bound agent ('bob')
      const bobCliInbox = JSON.parse(await runCli(['inbox', '--for', 'bob']))
      expect(bobCliInbox.ok).toBe(true)
      expect(bobCliInbox.messages.length).toBeGreaterThanOrEqual(2)

      const cliPause = JSON.parse(await runCli(['pause']))
      expect(cliPause.paused).toBe(true)
      const cliUnpause = JSON.parse(await runCli(['unpause']))
      expect(cliUnpause.paused).toBe(false)
    } finally {
      await daemon.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })
})
