import { describe, expect, test } from 'bun:test'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  authenticateRequest,
  initBusAuth,
  readAgentToken,
  readUiToken,
  redactSecrets,
  validateLoopbackHostAndOrigin,
} from '../src/auth.ts'
import { TurnBusDB } from '../src/db.ts'

describe('auth & loopback security primitives', () => {
  test('rejects non-loopback Host, cross-site Sec-Fetch-Site, and foreign Origin headers; allows IPv6 [::1]:port', () => {
    expect(
      validateLoopbackHostAndOrigin(
        new Request('http://127.0.0.1:8790/api/health', { headers: { Host: 'evil.com' } }),
        8790
      ).ok
    ).toBe(false)

    expect(
      validateLoopbackHostAndOrigin(
        new Request('http://127.0.0.1:8790/api/health', {
          headers: { Host: '127.0.0.1:8790', 'Sec-Fetch-Site': 'cross-site' },
        }),
        8790
      ).ok
    ).toBe(false)

    expect(
      validateLoopbackHostAndOrigin(
        new Request('http://127.0.0.1:8790/api/health', {
          headers: { Host: '127.0.0.1:8790', Origin: 'https://attacker.example' },
        }),
        8790
      ).ok
    ).toBe(false)

    expect(
      validateLoopbackHostAndOrigin(
        new Request('http://[::1]:8790/api/health', {
          headers: { Host: '[::1]:8790', Origin: 'http://[::1]:8790' },
        }),
        8790
      ).ok
    ).toBe(true)
  })

  test('enforces minimum 32-character length on TURNBUS_TOKEN and TURNBUS_UI_TOKEN environment variables', () => {
    const tmp = join(tmpdir(), `turnbus-env-auth-${Date.now()}`)
    mkdirSync(tmp, { recursive: true })
    const prevAgent = process.env.TURNBUS_TOKEN
    const prevUi = process.env.TURNBUS_UI_TOKEN
    try {
      process.env.TURNBUS_TOKEN = 'too-short'
      expect(() => readAgentToken(tmp)).toThrow(/at least 32 characters/)
      delete process.env.TURNBUS_TOKEN

      process.env.TURNBUS_UI_TOKEN = 'short-ui-token'
      expect(() => readUiToken(tmp)).toThrow(/at least 32 characters/)
    } finally {
      if (prevAgent === undefined) delete process.env.TURNBUS_TOKEN
      else process.env.TURNBUS_TOKEN = prevAgent
      if (prevUi === undefined) delete process.env.TURNBUS_UI_TOKEN
      else process.env.TURNBUS_UI_TOKEN = prevUi
      rmSync(tmp, { recursive: true, force: true })
    }
  })

  test('separates agent Bearer token from UI token and redacts secrets from logs', () => {
    const tmp = join(tmpdir(), `turnbus-auth-test-${Date.now()}`)
    mkdirSync(tmp, { recursive: true })
    try {
      const auth = initBusAuth(tmp)

      const agentAuth = authenticateRequest(
        new Request('http://127.0.0.1:8790/api/agents', {
          headers: { Host: '127.0.0.1:8790', Authorization: `Bearer ${auth.agentToken}` },
        }),
        auth,
        8790
      )
      expect(agentAuth).toEqual({ ok: true, role: 'agent' })

      const uiAuth = authenticateRequest(
        new Request('http://127.0.0.1:8790/api/ui/state', {
          headers: { Host: '127.0.0.1:8790', 'x-turnbus-ui-token': auth.uiToken },
        }),
        auth,
        8790
      )
      expect(uiAuth).toEqual({ ok: true, role: 'ui' })

      expect(
        redactSecrets('token=my-secret-123456 and ANTIGRAVITY_CSRF_TOKEN=csrf-987654', ['my-secret-123456'])
      ).toBe('token=[REDACTED] and ANTIGRAVITY_CSRF_TOKEN=[REDACTED]')
    } finally {
      rmSync(tmp, { recursive: true, force: true })
    }
  })
})

describe('TurnBusDB direct messaging, waits, and revisions', () => {
  test('supports DM idempotency, transactional reply reservation, and message revision history', () => {
    const db = new TurnBusDB(':memory:')
    db.upsertAgent({ id: 'alice', tool: 'claude' })
    db.upsertAgent({ id: 'bob', tool: 'codex' })

    const sent = db.sendDirectMessage({
      senderId: 'alice',
      recipientId: 'bob',
      body: 'Ping v1',
      clientMessageId: 'dm-1',
      waitSeconds: 30,
    })
    expect(sent.deduplicated).toBe(false)
    expect(sent.waitId).toBeTruthy()

    const dup = db.sendDirectMessage({
      senderId: 'alice',
      recipientId: 'bob',
      body: 'Ping v1',
      clientMessageId: 'dm-1',
      waitSeconds: 30,
    })
    expect(dup.deduplicated).toBe(true)
    expect(dup.message.id).toBe(sent.message.id)

    expect(() =>
      db.sendDirectMessage({
        senderId: 'alice',
        recipientId: 'bob',
        body: 'Different body',
        clientMessageId: 'dm-1',
      })
    ).toThrow(/Idempotency conflict/)

    const rev = db.addMessageRevision({
      editorId: 'alice',
      messageId: sent.message.id,
      newBody: 'Ping v2 (edited)',
    })
    expect(rev.revisionId).toBeGreaterThan(0)

    const reply = db.sendDirectMessage({
      senderId: 'bob',
      recipientId: 'alice',
      replyToId: sent.message.id,
      body: 'Pong!',
    })
    expect(reply.matchedWaitId).toBe(sent.waitId)

    const claimed = db.claimReservedWaitReply(sent.waitId!)
    expect(claimed?.id).toBe(reply.message.id)
    db.completeWaitReplyHandoff(sent.waitId!)

    db.close()
  })
})
