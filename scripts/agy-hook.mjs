#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

const daemonUrl = process.env.TURNBUS_URL ?? 'http://127.0.0.1:8790'
const stateDir = process.env.TURNBUS_STATE_DIR ?? join(homedir(), '.local', 'share', 'turnbus')
const tokenPath = join(stateDir, 'agent.token')

async function main() {
  const convId = process.env.ANTIGRAVITY_CONVERSATION_ID
  const lsAddress = process.env.ANTIGRAVITY_LS_ADDRESS
  const csrfToken = process.env.ANTIGRAVITY_CSRF_TOKEN
  if (!convId || !lsAddress || !csrfToken || !existsSync(tokenPath)) return

  const host = new URL(daemonUrl).host
  try {
    const healthRes = await fetch(`${daemonUrl}/api/health`, {
      headers: { Host: host },
      signal: AbortSignal.timeout(1200),
    })
    if (!healthRes.ok) return
    const health = await healthRes.json()
    if (!health?.ok || health?.product !== 'turnbus') return
  } catch {
    return
  }

  const token = readFileSync(tokenPath, 'utf-8').trim()
  if (!token) return

  try {
    await fetch(`${daemonUrl}/api/agents/register`, {
      method: 'POST',
      headers: {
        Host: host,
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        tool: 'agy',
        workspaceCwd: process.cwd(),
        explicitAgentId: process.env.TURNBUS_HANDLE ?? undefined,
        displayName: process.env.TURNBUS_DISPLAY_NAME ?? undefined,
        nativeSessionId: convId,
        lsAddress,
        csrfToken,
        agentApiExe: process.env.ANTIGRAVITY_AGENTAPI_EXE ?? null,
        issueSessionToken: false,
      }),
      signal: AbortSignal.timeout(2000),
    })
  } catch {}
}

main()
