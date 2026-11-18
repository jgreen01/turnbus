#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

const daemonUrl = process.env.TURNBUS_URL ?? 'http://127.0.0.1:8790'
const stateDir = process.env.TURNBUS_STATE_DIR ?? join(homedir(), '.local', 'share', 'turnbus')
const uiTokenPath = join(stateDir, 'ui.token')

async function main() {
  if (!existsSync(uiTokenPath)) return
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

  const uiToken = readFileSync(uiTokenPath, 'utf-8').trim()
  const handle = process.env.TURNBUS_HANDLE
  if (!uiToken || !handle) return

  try {
    const res = await fetch(`${daemonUrl}/api/messages/inbox`, {
      method: 'POST',
      headers: {
        Host: host,
        'x-turnbus-ui-token': uiToken,
        'x-turnbus-caller': handle,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ caller_id: handle, limit: 10, auto_ack: true, ack_source: 'claude_stop_hook' }),
      signal: AbortSignal.timeout(2000),
    })
    if (!res.ok) return
    const data = await res.json()
    const msgs = data?.messages ?? []
    if (msgs.length > 0) {
      const summary = msgs.map(m => `[#${m.message_id} from ${m.from}]: ${m.body}`).join('\n\n')
      process.stdout.write(
        JSON.stringify({
          decision: 'block',
          reason: `Unread turnbus messages arrived:\n\n${summary}`,
        }) + '\n'
      )
    }
  } catch {}
}

main()
