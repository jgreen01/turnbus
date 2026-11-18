import { describe, expect, test } from 'bun:test'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  authenticateRequest,
  initBusAuth,
  redactSecrets,
  validateLoopbackHostAndOrigin,
} from '../src/auth.ts'
import { parseChannelMentions, TurnBusDB } from '../src/db.ts'

describe('auth, dynamic handles, runtime channels, and fan-out store', () => {
  test('enforces loopback Host, Origin, Sec-Fetch-Site, token separation, and secret redaction', () => {
    const tmp = join(tmpdir(), `turnbus-auth-test-${Date.now()}`)
    mkdirSync(tmp, { recursive: true })
    const auth = initBusAuth(tmp)

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

    expect(redactSecrets('token=my-secret-123456 and ANTIGRAVITY_CSRF_TOKEN=csrf-987654', ['my-secret-123456'])).toBe(
      'token=[REDACTED] and ANTIGRAVITY_CSRF_TOKEN=[REDACTED]'
    )

    rmSync(tmp, { recursive: true, force: true })
  })

  test('supports dynamic agent registration, handle renaming with alias retention, and runtime channels', () => {
    const db = new TurnBusDB(':memory:')

    db.upsertAgent({ id: 'alice', displayName: 'Alice', tool: 'claude', workspace: '/tmp/ws-alpha' })
    db.upsertAgent({ id: 'bob', displayName: 'Bob', tool: 'codex', workspace: '/tmp/ws-beta' })
    db.upsertAgent({ id: 'carol', displayName: 'Carol', tool: 'agy', workspace: '/tmp/ws-gamma' })

    // All three auto-joined #general on registration
    const general = db.getChannel('#general')!
    expect(general.members).toEqual(['alice', 'bob', 'carol', 'user'])

    // Create a runtime channel #backend, join, change wake_mode, leave, and archive
    db.createChannel({
      id: '#backend',
      description: 'Backend engineering channel',
      members: [{ agentId: 'alice', wakeMode: 'mention' }],
    })
    db.joinChannel('#backend', 'bob', 'always')
    db.joinChannel('#backend', 'carol', 'passive')

    const parsed = parseChannelMentions(
      'Hey @alice check this out ```\n@carol in code block\n```\n> @carol in quote',
      new Set(['alice', 'bob', 'carol'])
    )
    expect(parsed.mentions).toEqual(['alice'])

    const post = db.postChannelMessage({
      senderId: 'user',
      channelId: '#backend',
      body: 'Hello @alice and @carol!',
    })
    const byRec = Object.fromEntries(post.deliveries.map(d => [d.recipient_id, d]))
    expect(byRec.alice.wake_required).toBe(1) // mentioned + wake_mode='mention'
    expect(byRec.bob.wake_required).toBe(1) // unmentioned, but wake_mode='always'
    expect(byRec.carol.wake_required).toBe(0) // mentioned, but wake_mode='passive'

    // Rename 'carol' -> 'cassandra' and persist workspace default
    const renamed = db.renameAgent({
      oldId: 'carol',
      newId: 'cassandra',
      displayName: 'Cassandra',
      setWorkspaceDefault: true,
    })
    expect(renamed.id).toBe('cassandra')
    expect(renamed.display_name).toBe('Cassandra')
    expect(JSON.parse(renamed.aliases)).toContain('carol')
    expect(db.listWorkspaceRules()).toEqual([{ workspace: '/tmp/ws-gamma', tool: 'agy', handle: 'cassandra' }])

    // Forward cursor pagination with afterSeq returns earliest next N without skipping backlog
    for (let i = 1; i <= 25; i++) {
      db.postChannelMessage({ senderId: 'alice', channelId: '#backend', body: `Msg ${i}` })
    }
    const page1 = db.readChannelHistory({
      callerId: 'bob',
      channelId: '#backend',
      afterSeq: post.message.seq,
      limit: 20,
    })
    expect(page1).toHaveLength(20)
    expect(page1[0].body).toBe('Msg 1')
    expect(page1[19].body).toBe('Msg 20')

    const page2 = db.readChannelHistory({
      callerId: 'bob',
      channelId: '#backend',
      afterSeq: page1[19].seq,
      limit: 20,
    })
    expect(page2).toHaveLength(5)
    expect(page2[4].body).toBe('Msg 25')

    // Leave channel revokes access
    expect(db.leaveChannel('#backend', 'bob')).toBe(true)
    expect(() => db.readChannelHistory({ callerId: 'bob', channelId: '#backend' })).toThrow(/Access denied/)

    // Archive channel blocks new posts
    db.archiveChannel('#backend', true)
    expect(() => db.postChannelMessage({ senderId: 'alice', channelId: '#backend', body: 'Blocked' })).toThrow(
      /is archived/
    )

    db.close()
  })

  test('supports atomic multi-target fan-out messaging, idempotency, and correlated reply collection', () => {
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

    // Idempotent retry returns exact same fanout
    const retry = db.sendFanoutMessage({
      senderId: 'alice',
      recipients: ['bob', 'carol'],
      body: 'Please review the API schema',
      clientMessageId: 'fanout-key-1',
      waitSeconds: 30,
    })
    expect(retry.deduplicated).toBe(true)
    expect(retry.fanoutId).toBe(fanout.fanoutId)

    // Bob replies to his fanout message
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

    // Carol replies to her fanout message
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
})
