import { describe, expect, test } from 'bun:test'
import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseChannelMentions, TurnBusDB } from '../src/db.ts'
import { discoverClaudeSessionByParentWalk, RosterManager } from '../src/roster.ts'

describe('dynamic roster & session discovery', () => {
  test('walks parent PIDs, verifies server:turnbus flag, and reserves 6-char hash on long folder names', () => {
    const tmp = join(tmpdir(), `turnbus-roster-test-${Date.now()}`)
    const sessionsDir = join(tmp, 'sessions')
    const procRoot = join(tmp, 'proc')
    mkdirSync(sessionsDir, { recursive: true })
    mkdirSync(join(procRoot, '5000'), { recursive: true })
    mkdirSync(join(procRoot, '5001'), { recursive: true })

    const makeStat = (pid: number, comm: string, ppid: number, starttime: string) =>
      `${pid} (${comm}) S ${ppid} 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 ${starttime} 0 0\n`

    writeFileSync(join(procRoot, '5000', 'stat'), makeStat(5000, 'claude main', 1000, '99887766'))
    writeFileSync(
      join(procRoot, '5000', 'cmdline'),
      'claude\0--dangerously-load-development-channels\0server:turnbus\0'
    )
    writeFileSync(join(procRoot, '5001', 'stat'), makeStat(5001, 'bun mcp', 5000, '99887770'))
    writeFileSync(
      join(sessionsDir, '5000.json'),
      JSON.stringify({
        pid: 5000,
        sessionId: 'sess-alice-uuid',
        cwd: '/tmp/project-alpha',
        procStart: '99887766',
        version: '2.1.318',
        status: 'busy',
      })
    )
    writeFileSync(join(sessionsDir, '5000.secret.key'), 'PRIVATE_KEY_IGNORED')

    const found = discoverClaudeSessionByParentWalk(5001, {
      claudeSessionsDir: sessionsDir,
      procRoot,
    })
    expect(found?.channelPushEnabled).toBe(true)
    expect(found?.sessionId).toBe('sess-alice-uuid')

    const db = new TurnBusDB(':memory:')
    const roster = new RosterManager(db)

    const longCwd = '/tmp/this-is-an-extremely-long-workspace-folder-name-for-multi-agent-testing'
    const first = roster.resolveAgentIdentity({
      tool: 'codex',
      workspaceCwd: longCwd,
      nativeSessionId: 'sess-1',
    })
    db.bindAgentSession({
      agentId: first.agentId,
      endpointKey: 'codex:local',
      nativeSessionId: 'sess-1',
    })
    const second = roster.resolveAgentIdentity({
      tool: 'codex',
      workspaceCwd: longCwd,
      nativeSessionId: 'sess-2',
    })
    expect(first.agentId).not.toBe(second.agentId)
    expect(second.agentId.length).toBeLessThanOrEqual(48)

    expect(() =>
      roster.resolveAgentIdentity({
        tool: 'agy',
        explicitAgentId: first.agentId,
        workspaceCwd: longCwd,
      })
    ).toThrow(/registered for tool 'codex'/)

    db.close()
    rmSync(tmp, { recursive: true, force: true })
  })

  test('supports agent registration, handle renaming with alias retention, and workspace rules', () => {
    const db = new TurnBusDB(':memory:')
    const roster = new RosterManager(db)

    db.upsertAgent({ id: 'alice', displayName: 'Alice', tool: 'claude', workspace: '/tmp/ws-alpha' })
    db.upsertAgent({ id: 'bob', displayName: 'Bob', tool: 'codex', workspace: '/tmp/ws-beta' })
    db.upsertAgent({ id: 'carol', displayName: 'Carol', tool: 'agy', workspace: '/tmp/ws-gamma' })

    const general = db.getChannel('#general')!
    expect(general.members).toEqual(['alice', 'bob', 'carol', 'user'])

    const renamed = db.renameAgent({
      oldId: 'carol',
      newId: 'cassandra',
      displayName: 'Cassandra',
      setWorkspaceDefault: true,
    })
    expect(renamed.id).toBe('cassandra')
    expect(renamed.display_name).toBe('Cassandra')
    expect(JSON.parse(renamed.aliases)).toContain('carol')
    expect(roster.resolveTargetHandle('carol')).toBe('cassandra')
    expect(db.listWorkspaceRules()).toEqual([{ workspace: '/tmp/ws-gamma', tool: 'agy', handle: 'cassandra' }])

    db.close()
  })
})

describe('runtime channels & cursor pagination', () => {
  test('supports runtime channel creation, mention parsing outside code/quotes, wake modes, cursor pagination, and archiving', () => {
    const db = new TurnBusDB(':memory:')
    db.upsertAgent({ id: 'alice', displayName: 'Alice', tool: 'claude' })
    db.upsertAgent({ id: 'bob', displayName: 'Bob', tool: 'codex' })
    db.upsertAgent({ id: 'carol', displayName: 'Carol', tool: 'agy' })

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
    expect(byRec.alice.wake_required).toBe(1)
    expect(byRec.bob.wake_required).toBe(1)
    expect(byRec.carol.wake_required).toBe(0)

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

    expect(db.leaveChannel('#backend', 'bob')).toBe(true)
    expect(() => db.readChannelHistory({ callerId: 'bob', channelId: '#backend' })).toThrow(/Access denied/)

    db.archiveChannel('#backend', true)
    expect(() => db.postChannelMessage({ senderId: 'alice', channelId: '#backend', body: 'Blocked' })).toThrow(
      /is archived/
    )

    db.close()
  })
})
