import { describe, expect, test } from 'bun:test'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startTurnBusDaemon } from '../src/daemon.ts'
import { TurnBusDB } from '../src/db.ts'

describe('first-class fan-out messaging & correlated barriers', () => {
  test('creates atomic multi-target fan-out messages with idempotent client_message_id', () => {
    const db = new TurnBusDB(':memory:')
    db.upsertAgent({ id: 'alice', tool: 'claude' })
    db.upsertAgent({ id: 'bob', tool: 'codex' })
    db.upsertAgent({ id: 'carol', tool: 'agy' })

    const fanout = db.sendFanoutMessage({
      senderId: 'alice',
      recipients: ['bob', 'carol'],
      body: 'Please review the API schema',
      clientMessageId: 'fanout-key-1',
      waitSeconds: 30,
    })
    expect(fanout.deduplicated).toBe(false)
    expect(fanout.messages).toHaveLength(2)
    expect(fanout.waitIds).toHaveLength(2)

    const retry = db.sendFanoutMessage({
      senderId: 'alice',
      recipients: ['bob', 'carol'],
      body: 'Please review the API schema',
      clientMessageId: 'fanout-key-1',
      waitSeconds: 30,
    })
    expect(retry.deduplicated).toBe(true)
    expect(retry.fanoutId).toBe(fanout.fanoutId)

    db.close()
  })

  test('correlates replies per recipient and tracks partial vs complete fan-out status', () => {
    const db = new TurnBusDB(':memory:')
    db.upsertAgent({ id: 'alice', tool: 'claude' })
    db.upsertAgent({ id: 'bob', tool: 'codex' })
    db.upsertAgent({ id: 'carol', tool: 'agy' })

    const fanout = db.sendFanoutMessage({
      senderId: 'alice',
      recipients: ['bob', 'carol'],
      body: 'Status check',
      waitSeconds: 30,
    })

    const bobMsg = fanout.messages.find(m => m.recipient_id === 'bob')!
    const bobReply = db.sendDirectMessage({
      senderId: 'bob',
      recipientId: 'alice',
      replyToId: bobMsg.id,
      body: 'LGTM from Bob',
    })
    expect(bobReply.matchedFanoutId).toBe(fanout.fanoutId)

    const partial = db.getFanoutStatus({ fanoutId: fanout.fanoutId, callerId: 'alice', autoAck: false })
    expect(partial.status).toBe('partial')
    expect(partial.replied_count).toBe(1)
    expect(partial.pending_recipients).toEqual(['carol'])
    expect(partial.replies.bob.body).toBe('LGTM from Bob')

    const carolMsg = fanout.messages.find(m => m.recipient_id === 'carol')!
    db.sendDirectMessage({
      senderId: 'carol',
      recipientId: 'alice',
      replyToId: carolMsg.id,
      body: 'LGTM from Carol',
    })

    const complete = db.getFanoutStatus({ fanoutId: fanout.fanoutId, callerId: 'alice', autoAck: true })
    expect(complete.status).toBe('complete')
    expect(complete.replied_count).toBe(2)
    expect(complete.pending_recipients).toEqual([])
    expect(complete.replies.carol.body).toBe('LGTM from Carol')

    db.close()
  })

  test('executes parallel HTTP fan-out with wait_for barrier over live daemon', async () => {
    const tmpStateDir = join(tmpdir(), `turnbus-fanout-http-${Date.now()}`)
    mkdirSync(tmpStateDir, { recursive: true })
    const agyCalls: string[][] = []

    const daemon = startTurnBusDaemon({
      port: 0,
      stateDir: tmpStateDir,
      reconcileIntervalMs: 0,
      codexRpc: async (method, params: any) => {
        if (method === 'thread/loaded/list') {
          return { requestSent: true, result: { data: ['thread-bob', 'thread-carol'], nextCursor: null } }
        }
        if (method === 'thread/read') {
          return {
            requestSent: true,
            result: {
              thread: {
                id: params.threadId,
                cwd: '/tmp/service',
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
    const agentHeaders = {
      Host: `127.0.0.1:${daemon.port}`,
      Authorization: `Bearer ${daemon.auth.agentToken}`,
      'Content-Type': 'application/json',
    }

    try {
      const tokens: Record<string, string> = {}
      for (const regBody of [
        { tool: 'claude', explicitAgentId: 'alice', nativeSessionId: 'claude-alice' },
        { tool: 'codex', explicitAgentId: 'bob', nativeSessionId: 'thread-bob' },
        { tool: 'codex', explicitAgentId: 'carol', nativeSessionId: 'thread-carol' },
        {
          tool: 'agy',
          explicitAgentId: 'dave',
          nativeSessionId: 'agy-dave',
          lsAddress: 'localhost:41000',
          csrfToken: 'secret-csrf',
        },
      ]) {
        const res = await fetch(`${baseUrl}/api/agents/register`, {
          method: 'POST',
          headers: agentHeaders,
          body: JSON.stringify(regBody),
        }).then(r => r.json() as Promise<any>)
        tokens[res.agentId] = res.sessionToken
      }

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
          wait_for: 2,
        }),
      }).then(r => r.json() as Promise<any>)

      for (let i = 0; i < 20 && agyCalls.length === 0; i++) {
        await new Promise(r => setTimeout(r, 25))
      }
      expect(agyCalls).toHaveLength(1)

      const bobInbox = await fetch(`${baseUrl}/api/messages/inbox`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'bob', 'x-turnbus-session-token': tokens.bob },
        body: JSON.stringify({ auto_ack: true }),
      }).then(r => r.json() as Promise<any>)
      const carolInbox = await fetch(`${baseUrl}/api/messages/inbox`, {
        method: 'POST',
        headers: { ...agentHeaders, 'x-turnbus-caller': 'carol', 'x-turnbus-session-token': tokens.carol },
        body: JSON.stringify({ auto_ack: true }),
      }).then(r => r.json() as Promise<any>)

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
    } finally {
      await daemon.stop()
      rmSync(tmpStateDir, { recursive: true, force: true })
    }
  })
})
