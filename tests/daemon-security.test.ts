import { describe, expect, test } from 'bun:test'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import WebSocket from 'ws'
import { startTurnBusDaemon } from '../src/daemon.ts'

describe('daemon HTTP & WebSocket security hardening', () => {
  test('GET / and GET /?token=<uiToken> never embed or leak ui.token in HTML', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-ui-sec-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })
    const daemon = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const baseUrl = `http://127.0.0.1:${daemon.port}`
    const hostHeader = `127.0.0.1:${daemon.port}`

    try {
      const rootHtml = await fetch(`${baseUrl}/`, { headers: { Host: hostHeader } }).then(r => r.text())
      expect(rootHtml).not.toContain(daemon.auth.uiToken)
      expect(rootHtml).not.toContain('__TURNBUS_UI_TOKEN__')
      expect(rootHtml).toContain('id="autoScrollBtn"')

      const queryHtml = await fetch(`${baseUrl}/?token=${encodeURIComponent(daemon.auth.uiToken)}`, {
        headers: { Host: hostHeader, 'x-turnbus-ui-token': daemon.auth.uiToken },
      }).then(r => r.text())
      expect(queryHtml).not.toContain(daemon.auth.uiToken)
      expect(queryHtml).not.toContain('__TURNBUS_UI_TOKEN__')
    } finally {
      await daemon.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })

  test('persists bound session token across daemon restart and rejects header-only HTTP or WS impersonation after restart', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-restart-sec-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })

    // 1. Start daemon #1 and register 'alice' + 'bob'
    const daemon1 = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const base1 = `http://127.0.0.1:${daemon1.port}`
    const headers1 = {
      Host: `127.0.0.1:${daemon1.port}`,
      Authorization: `Bearer ${daemon1.auth.agentToken}`,
      'Content-Type': 'application/json',
    }

    const aliceReg = await fetch(`${base1}/api/agents/register`, {
      method: 'POST',
      headers: headers1,
      body: JSON.stringify({
        tool: 'claude',
        explicitAgentId: 'alice',
        nativeSessionId: 'alice-session-1',
      }),
    }).then(r => r.json() as Promise<any>)
    expect(aliceReg.ok).toBe(true)
    expect(typeof aliceReg.sessionToken).toBe('string')
    const aliceToken: string = aliceReg.sessionToken

    await fetch(`${base1}/api/agents/register`, {
      method: 'POST',
      headers: headers1,
      body: JSON.stringify({
        tool: 'codex',
        explicitAgentId: 'bob',
        nativeSessionId: 'bob-session-1',
      }),
    })

    // Stop daemon #1 (wiping all in-memory maps while SQLite session leases remain active)
    await daemon1.stop()

    // 2. Start daemon #2 on the exact same stateDir
    const daemon2 = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const base2 = `http://127.0.0.1:${daemon2.port}`
    const headers2 = {
      Host: `127.0.0.1:${daemon2.port}`,
      Authorization: `Bearer ${daemon2.auth.agentToken}`,
      'Content-Type': 'application/json',
    }

    try {
      // (a) Header-only HTTP impersonation of 'alice' without x-turnbus-session-token MUST be rejected with 403
      const spoofSend = await fetch(`${base2}/api/messages/send`, {
        method: 'POST',
        headers: { ...headers2, 'x-turnbus-caller': 'alice' },
        body: JSON.stringify({ to: 'bob', message: 'impersonated after restart' }),
      })
      expect(spoofSend.status).toBe(403)

      // (b) Rebinding 'alice' to a different nativeSessionId without her session token MUST be rejected with 403
      const hijackRebind = await fetch(`${base2}/api/agents/register`, {
        method: 'POST',
        headers: headers2,
        body: JSON.stringify({
          tool: 'claude',
          explicitAgentId: 'alice',
          nativeSessionId: 'hijacker-session',
          allowRebind: true,
        }),
      })
      expect(hijackRebind.status).toBe(403)

      // (c) WebSocket auth_adapter for 'alice' without sessionToken MUST be rejected with 1008
      const wsSpoofCloseCode = await new Promise<number>(resolve => {
        const ws = new WebSocket(`ws://127.0.0.1:${daemon2.port}/ws`, {
          headers: { Host: `127.0.0.1:${daemon2.port}` },
        })
        ws.on('open', () => {
          ws.send(JSON.stringify({ type: 'auth_adapter', token: daemon2.auth.agentToken, agentId: 'alice' }))
        })
        ws.on('close', code => resolve(code))
      })
      expect(wsSpoofCloseCode).toBe(1008)

      // (d) Legitimate 'alice' presenting her pre-restart sessionToken succeeds on both HTTP and WebSocket
      const validSend = await fetch(`${base2}/api/messages/send`, {
        method: 'POST',
        headers: {
          ...headers2,
          'x-turnbus-caller': 'alice',
          'x-turnbus-session-token': aliceToken,
        },
        body: JSON.stringify({ to: 'bob', message: 'legitimate message after restart' }),
      })
      expect(validSend.status).toBe(200)

      const wsValidAuth = await new Promise<any>((resolve, reject) => {
        const ws = new WebSocket(`ws://127.0.0.1:${daemon2.port}/ws`, {
          headers: { Host: `127.0.0.1:${daemon2.port}` },
        })
        ws.on('open', () => {
          ws.send(
            JSON.stringify({
              type: 'auth_adapter',
              token: daemon2.auth.agentToken,
              sessionToken: aliceToken,
              agentId: 'alice',
            })
          )
        })
        ws.on('message', data => {
          const msg = JSON.parse(data.toString())
          ws.close()
          resolve(msg)
        })
        ws.on('error', reject)
      })
      expect(wsValidAuth.type).toBe('auth_ok')
      expect(wsValidAuth.agentId).toBe('alice')
    } finally {
      await daemon2.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })

  test('aborts chunked streaming request body exceeding 512 KiB without Content-Length with HTTP 413', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-stream-cap-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })
    const daemon = startTurnBusDaemon({ port: 0, stateDir: tmpStateDir, reconcileIntervalMs: 0 })
    const baseUrl = `http://127.0.0.1:${daemon.port}`

    try {
      const chunk = new Uint8Array(64 * 1024).fill(65) // 64 KiB of 'A'
      let chunksSent = 0
      const stream = new ReadableStream<Uint8Array>({
        pull(controller) {
          if (chunksSent === 0) {
            controller.enqueue(new TextEncoder().encode('{"to":"user","message":"'))
          } else if (chunksSent <= 10) {
            controller.enqueue(chunk) // 10 * 64 KiB = 640 KiB > 512 KiB
          } else {
            controller.enqueue(new TextEncoder().encode('"}'))
            controller.close()
          }
          chunksSent++
        },
      })

      const res = await fetch(`${baseUrl}/api/messages/send`, {
        method: 'POST',
        headers: {
          Host: `127.0.0.1:${daemon.port}`,
          'x-turnbus-ui-token': daemon.auth.uiToken,
          'Content-Type': 'application/json',
        },
        body: stream,
        // @ts-ignore
        duplex: 'half',
      })
      expect(res.status).toBe(413)
    } finally {
      await daemon.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })
})
