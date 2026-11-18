import { describe, expect, test } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startTurnBusDaemon } from '../src/daemon.ts'

describe('stdio MCP server & CLI end-to-end integration', () => {
  test('connects stdio MCP client for whoami, rename_agent, runtime channels, and fanout_message', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-mcp-e2e-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })
    const daemon = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const baseUrl = `http://127.0.0.1:${daemon.port}`

    daemon.db.upsertAgent({ id: 'alice', tool: 'claude' })
    daemon.db.upsertAgent({ id: 'bob', tool: 'codex' })
    daemon.db.upsertAgent({ id: 'carol', tool: 'codex' })

    try {
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

      const mcpFanoutRes = await mcpClient.callTool({
        name: 'fanout_message',
        arguments: { to: ['bob', 'carol'], message: 'MCP fanout ping from Evelyn' },
      })
      const mcpFanoutJson = JSON.parse((mcpFanoutRes.content as any)[0].text)
      expect(mcpFanoutJson.ok).toBe(true)
      expect(mcpFanoutJson.total_recipients).toBe(2)

      await mcpClient.close()
    } finally {
      await daemon.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })

  test('runs turnbus CLI commands for UI URL, fanout, operator --for inbox read, and pause/unpause', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-cli-e2e-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })
    const daemon = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const baseUrl = `http://127.0.0.1:${daemon.port}`
    const agentHeaders = {
      Host: `127.0.0.1:${daemon.port}`,
      Authorization: `Bearer ${daemon.auth.agentToken}`,
      'Content-Type': 'application/json',
    }

    try {
      await fetch(`${baseUrl}/api/agents/register`, {
        method: 'POST',
        headers: agentHeaders,
        body: JSON.stringify({ tool: 'codex', explicitAgentId: 'bob', nativeSessionId: 'thread-bob' }),
      })
      await fetch(`${baseUrl}/api/agents/register`, {
        method: 'POST',
        headers: agentHeaders,
        body: JSON.stringify({ tool: 'codex', explicitAgentId: 'carol', nativeSessionId: 'thread-carol' }),
      })

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

      const bobCliInbox = JSON.parse(await runCli(['inbox', '--for', 'bob']))
      expect(bobCliInbox.ok).toBe(true)
      expect(bobCliInbox.messages.length).toBe(1)

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
