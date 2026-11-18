import { Database } from 'bun:sqlite'
import { randomUUID } from 'node:crypto'
import { dirname } from 'node:path'
import { ensureStateDir } from './auth.ts'

export type ToolType = 'claude' | 'codex' | 'agy' | 'user'
export type WakeMode = 'mention' | 'always' | 'passive'
export type DeliveryStatus = 'queued' | 'pushing' | 'pushed' | 'acked' | 'failed' | 'unknown'
export type AttemptOutcome = 'in_progress' | 'accepted' | 'definitely_not_accepted' | 'unknown'

export interface AgentRecord {
  id: string
  display_name: string
  tool: ToolType
  workspace: string | null
  aliases: string // JSON array
  created_at: number
  updated_at: number
}

export interface AgentSessionRecord {
  id: string
  agent_id: string
  endpoint_key: string
  native_session_id: string
  generation: number
  is_current: number
  workspace: string | null
  pid: number | null
  process_started_at: string | null
  native_status: string
  capabilities: string
  push_config: string
  connection_epoch: string | null
  last_seen_at: number
  lease_expires_at: number
}

export interface MessageRecord {
  seq: number
  id: string
  sender_id: string
  sender_session_id: string | null
  client_message_id: string | null
  recipient_id: string | null
  channel_id: string | null
  fanout_id: string | null
  body: string
  reply_to_id: string | null
  metadata: string | null
  created_at: number
}

export interface DeliveryRecord {
  id: number
  message_id: string
  recipient_id: string
  wake_required: number
  status: DeliveryStatus
  active_claim_token: string | null
  attempt_count: number
  last_attempt_at: number | null
  next_retry_at: number | null
  pushed_at: number | null
  acked_at: number | null
  ack_source: string | null
  last_error: string | null
}

export interface ClaimedDelivery {
  delivery: DeliveryRecord
  message: MessageRecord
  claimToken: string
  nativeClientMessageId: string
  session: AgentSessionRecord | null
}

export const MAX_MESSAGE_BYTES = 256 * 1024
export const DEFAULT_SESSION_LEASE_MS = 60_000
export const MAX_PUSH_ATTEMPTS = 5

export function normalizeChannelId(raw: string): string {
  const clean = raw.trim().toLowerCase().replace(/^#+/, '')
  if (!clean || !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(clean)) {
    throw new Error(`Invalid channel name: '${raw}' (use alphanumeric, '-', '_')`)
  }
  return `#${clean}`
}

export function normalizeHandle(raw: string): string {
  const clean = raw.trim().toLowerCase().replace(/^@+/, '')
  if (!clean || !/^[a-z0-9][a-z0-9_:-]{0,47}$/.test(clean)) {
    throw new Error(`Invalid agent handle: '${raw}'`)
  }
  return clean
}

export function parseChannelMentions(body: string, validMemberHandles: Set<string>): {
  mentions: string[]
  mentionAll: boolean
} {
  const stripped = body
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`[^`\n]+`/g, ' ')
    .replace(/^\s*>.*$/gm, ' ')

  const found = new Set<string>()
  let mentionAll = false
  const regex = /(?:^|[^a-zA-Z0-9_/-])@([a-zA-Z0-9_-]+)\b/g
  let match: RegExpExecArray | null
  while ((match = regex.exec(stripped)) !== null) {
    const token = match[1].toLowerCase()
    if (token === 'all' || token === 'channel') {
      mentionAll = true
    } else if (validMemberHandles.has(token)) {
      found.add(token)
    }
  }
  return { mentions: Array.from(found).sort(), mentionAll }
}

export class TurnBusDB {
  public readonly db: Database

  constructor(dbPath = ':memory:', options: { seedDefaultChannel?: boolean } = {}) {
    if (dbPath !== ':memory:') {
      ensureStateDir(dirname(dbPath))
    }
    this.db = new Database(dbPath, { create: true })
    this.initSchema(options.seedDefaultChannel !== false)
  }

  private initSchema(seedDefaultChannel: boolean): void {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 5000;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS agents (
        id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        tool TEXT NOT NULL CHECK(tool IN ('claude', 'codex', 'agy', 'user')),
        workspace TEXT,
        aliases TEXT NOT NULL DEFAULT '[]',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS workspace_rules (
        workspace TEXT NOT NULL,
        tool TEXT NOT NULL CHECK(tool IN ('claude', 'codex', 'agy')),
        handle TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (workspace, tool)
      );

      CREATE TABLE IF NOT EXISTS agent_sessions (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        endpoint_key TEXT NOT NULL,
        native_session_id TEXT NOT NULL,
        generation INTEGER NOT NULL DEFAULT 1,
        is_current INTEGER NOT NULL DEFAULT 1,
        workspace TEXT,
        pid INTEGER,
        process_started_at TEXT,
        native_status TEXT NOT NULL DEFAULT 'idle',
        capabilities TEXT NOT NULL DEFAULT '{}',
        push_config TEXT NOT NULL DEFAULT '{}',
        connection_epoch TEXT,
        last_seen_at INTEGER NOT NULL,
        lease_expires_at INTEGER NOT NULL,
        UNIQUE(endpoint_key, native_session_id)
      );

      CREATE INDEX IF NOT EXISTS idx_sessions_current
        ON agent_sessions(agent_id, is_current, lease_expires_at);

      CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        description TEXT NOT NULL DEFAULT '',
        archived_at INTEGER,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS channel_members (
        channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
        agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        wake_mode TEXT NOT NULL DEFAULT 'mention' CHECK(wake_mode IN ('mention', 'always', 'passive')),
        PRIMARY KEY (channel_id, agent_id)
      );

      CREATE TABLE IF NOT EXISTS fanouts (
        id TEXT PRIMARY KEY,
        sender_id TEXT NOT NULL REFERENCES agents(id),
        client_message_id TEXT,
        body TEXT NOT NULL,
        wake_required INTEGER NOT NULL DEFAULT 1,
        recipient_count INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(sender_id, client_message_id)
      );

      CREATE TABLE IF NOT EXISTS messages (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        sender_id TEXT NOT NULL REFERENCES agents(id),
        sender_session_id TEXT,
        client_message_id TEXT,
        recipient_id TEXT REFERENCES agents(id),
        channel_id TEXT REFERENCES channels(id),
        fanout_id TEXT REFERENCES fanouts(id),
        body TEXT NOT NULL,
        reply_to_id TEXT REFERENCES messages(id),
        metadata TEXT,
        created_at INTEGER NOT NULL,
        CHECK (
          (recipient_id IS NOT NULL AND channel_id IS NULL) OR
          (recipient_id IS NULL AND channel_id IS NOT NULL)
        ),
        UNIQUE(sender_id, client_message_id)
      );

      CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to_id, seq);
      CREATE INDEX IF NOT EXISTS idx_messages_channel_seq ON messages(channel_id, seq);
      CREATE INDEX IF NOT EXISTS idx_messages_fanout ON messages(fanout_id, seq);

      CREATE TABLE IF NOT EXISTS message_revisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        editor_id TEXT NOT NULL REFERENCES agents(id),
        body TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        recipient_id TEXT NOT NULL REFERENCES agents(id),
        wake_required INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued', 'pushing', 'pushed', 'acked', 'failed', 'unknown')),
        active_claim_token TEXT,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_attempt_at INTEGER,
        next_retry_at INTEGER,
        pushed_at INTEGER,
        acked_at INTEGER,
        ack_source TEXT,
        last_error TEXT,
        UNIQUE(message_id, recipient_id)
      );

      CREATE INDEX IF NOT EXISTS idx_deliveries_inbox
        ON deliveries(recipient_id, status, id);

      CREATE TABLE IF NOT EXISTS delivery_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id INTEGER NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
        claim_token TEXT NOT NULL UNIQUE,
        route TEXT NOT NULL,
        target_session_id TEXT,
        target_generation INTEGER,
        native_client_message_id TEXT NOT NULL,
        stage TEXT NOT NULL DEFAULT 'claimed',
        outcome TEXT NOT NULL DEFAULT 'in_progress' CHECK(outcome IN ('in_progress', 'accepted', 'definitely_not_accepted', 'unknown')),
        native_receipt TEXT,
        error TEXT,
        started_at INTEGER NOT NULL,
        ended_at INTEGER
      );

      CREATE TABLE IF NOT EXISTS reply_waits (
        id TEXT PRIMARY KEY,
        request_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        fanout_id TEXT REFERENCES fanouts(id) ON DELETE CASCADE,
        caller_agent_id TEXT NOT NULL REFERENCES agents(id),
        expected_replier_id TEXT NOT NULL REFERENCES agents(id),
        status TEXT NOT NULL DEFAULT 'waiting' CHECK(status IN ('waiting', 'reserved', 'returning', 'completed', 'expired', 'cancelled', 'unknown')),
        reply_id TEXT REFERENCES messages(id),
        expires_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_reply_waits_active
        ON reply_waits(request_message_id, expected_replier_id, status, expires_at);
      CREATE INDEX IF NOT EXISTS idx_reply_waits_fanout
        ON reply_waits(fanout_id, status);
    `)

    // Always ensure the human 'user' operator handle exists
    const now = Date.now()
    this.upsertAgent({ id: 'user', displayName: 'Operator', tool: 'user', now })
    if (seedDefaultChannel) {
      this.createChannel({
        id: '#general',
        description: 'General multi-agent coordination channel',
        members: [{ agentId: 'user', wakeMode: 'passive' }],
        now,
      })
    }
  }

  close(): void {
    this.db.close()
  }

  upsertAgent(params: {
    id: string
    displayName?: string
    tool: ToolType
    workspace?: string | null
    aliases?: string[]
    autoJoinGeneral?: boolean
    now?: number
  }): AgentRecord {
    const now = params.now ?? Date.now()
    const id = normalizeHandle(params.id)
    const existing = this.getAgent(id)
    const displayName =
      params.displayName?.trim() ||
      existing?.display_name ||
      id.charAt(0).toUpperCase() + id.slice(1)
    const aliasesJson = params.aliases ? JSON.stringify(params.aliases) : (existing?.aliases ?? '[]')

    this.db.query(`
      INSERT INTO agents (id, display_name, tool, workspace, aliases, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        display_name = excluded.display_name,
        workspace = COALESCE(excluded.workspace, agents.workspace),
        aliases = excluded.aliases,
        updated_at = excluded.updated_at
    `).run(id, displayName, params.tool, params.workspace ?? null, aliasesJson, now, now)

    if (!existing && params.autoJoinGeneral !== false && params.tool !== 'user') {
      const gen = this.db.query(`SELECT id FROM channels WHERE id = '#general' AND archived_at IS NULL`).get()
      if (gen) {
        this.db.query(`
          INSERT OR IGNORE INTO channel_members (channel_id, agent_id, wake_mode)
          VALUES ('#general', ?, 'mention')
        `).run(id)
      }
    }

    return this.getAgent(id)!
  }

  getAgent(id: string): AgentRecord | null {
    return this.db.query('SELECT * FROM agents WHERE id = ?').get(id.trim().toLowerCase()) as AgentRecord | null
  }

  listAgents(): AgentRecord[] {
    return this.db.query('SELECT * FROM agents ORDER BY id ASC').all() as AgentRecord[]
  }

  setWorkspaceRule(workspace: string, tool: Exclude<ToolType, 'user'>, handle: string, now = Date.now()): void {
    this.db.query(`
      INSERT INTO workspace_rules (workspace, tool, handle, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(workspace, tool) DO UPDATE SET handle = excluded.handle, updated_at = excluded.updated_at
    `).run(workspace, tool, normalizeHandle(handle), now)
  }

  listWorkspaceRules(): Array<{ workspace: string; tool: Exclude<ToolType, 'user'>; handle: string }> {
    return this.db.query('SELECT workspace, tool, handle FROM workspace_rules ORDER BY workspace ASC').all() as any
  }

  renameAgent(params: {
    oldId: string
    newId?: string
    displayName?: string
    aliases?: string[]
    setWorkspaceDefault?: boolean
    now?: number
  }): AgentRecord {
    const now = params.now ?? Date.now()
    const oldId = normalizeHandle(params.oldId)
    if (oldId === 'user') throw new Error("Cannot rename reserved 'user' handle")
    const existing = this.getAgent(oldId)
    if (!existing) throw new Error(`Unknown agent handle: '${oldId}'`)

    const targetId = params.newId ? normalizeHandle(params.newId) : oldId
    if (targetId === 'user') throw new Error("Cannot rename to reserved 'user' handle")

    const tx = this.db.transaction(() => {
      const nextDisplayName = params.displayName?.trim() || existing.display_name
      const nextAliases = params.aliases ? JSON.stringify(params.aliases) : existing.aliases

      if (targetId !== oldId) {
        const collision = this.getAgent(targetId)
        if (collision) {
          throw new Error(`Target handle '${targetId}' already exists`)
        }
        // Add oldId as an alias on the renamed agent so references still resolve
        const parsedAliases: string[] = JSON.parse(nextAliases || '[]')
        if (!parsedAliases.includes(oldId)) parsedAliases.push(oldId)

        this.db.query(`
          INSERT INTO agents (id, display_name, tool, workspace, aliases, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(targetId, nextDisplayName, existing.tool, existing.workspace, JSON.stringify(parsedAliases), existing.created_at, now)

        this.db.query(`UPDATE agent_sessions SET agent_id = ? WHERE agent_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE OR IGNORE channel_members SET agent_id = ? WHERE agent_id = ?`).run(targetId, oldId)
        this.db.query(`DELETE FROM channel_members WHERE agent_id = ?`).run(oldId)
        this.db.query(`UPDATE messages SET sender_id = ? WHERE sender_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE messages SET recipient_id = ? WHERE recipient_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE fanouts SET sender_id = ? WHERE sender_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE deliveries SET recipient_id = ? WHERE recipient_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE reply_waits SET caller_agent_id = ? WHERE caller_agent_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE reply_waits SET expected_replier_id = ? WHERE expected_replier_id = ?`).run(targetId, oldId)
        this.db.query(`UPDATE workspace_rules SET handle = ? WHERE handle = ?`).run(targetId, oldId)
        this.db.query(`DELETE FROM agents WHERE id = ?`).run(oldId)
      } else {
        this.db.query(`
          UPDATE agents SET display_name = ?, aliases = ?, updated_at = ? WHERE id = ?
        `).run(nextDisplayName, nextAliases, now, oldId)
      }

      const updated = this.getAgent(targetId)!
      if (params.setWorkspaceDefault && updated.workspace && updated.tool !== 'user') {
        this.setWorkspaceRule(updated.workspace, updated.tool, targetId, now)
      }
      return updated
    })
    return tx()
  }

  // ===========================================================================
  // Runtime Channel Management
  // ===========================================================================
  createChannel(params: {
    id: string
    description?: string
    members?: Array<{ agentId: string; wakeMode?: WakeMode }>
    reconcileMembers?: boolean
    now?: number
  }): { id: string; description: string; members: Array<{ agent_id: string; wake_mode: WakeMode }> } {
    const chanId = normalizeChannelId(params.id)
    const now = params.now ?? Date.now()
    const tx = this.db.transaction(() => {
      this.db.query(`
        INSERT INTO channels (id, description, archived_at, created_at)
        VALUES (?, ?, NULL, ?)
        ON CONFLICT(id) DO UPDATE SET
          description = CASE WHEN excluded.description != '' THEN excluded.description ELSE channels.description END,
          archived_at = NULL
      `).run(chanId, params.description ?? '', now)

      if (params.reconcileMembers) {
        this.db.query(`DELETE FROM channel_members WHERE channel_id = ?`).run(chanId)
      }

      // Ensure operator 'user' is passively in every channel so Web UI can read/post
      this.db.query(`
        INSERT OR IGNORE INTO channel_members (channel_id, agent_id, wake_mode)
        VALUES (?, 'user', 'passive')
      `).run(chanId)

      for (const m of params.members ?? []) {
        const agent = this.getAgent(m.agentId)
        if (!agent) continue
        const effectiveMode: WakeMode =
          agent.id === 'user' ? 'passive' : (m.wakeMode ?? 'mention')
        this.db.query(`
          INSERT INTO channel_members (channel_id, agent_id, wake_mode)
          VALUES (?, ?, ?)
          ON CONFLICT(channel_id, agent_id) DO UPDATE SET wake_mode = excluded.wake_mode
        `).run(chanId, agent.id, effectiveMode)
      }

      return this.getChannel(chanId)!
    })
    return tx()
  }

  joinChannel(channelId: string, agentId: string, wakeMode?: WakeMode): { channel_id: string; agent_id: string; wake_mode: WakeMode } {
    const chanId = normalizeChannelId(channelId)
    const handle = normalizeHandle(agentId)
    const tx = this.db.transaction(() => {
      const agent = this.getAgent(handle)
      if (!agent) throw new Error(`Unknown agent handle: '${handle}'`)
      let chan = this.db.query(`SELECT * FROM channels WHERE id = ?`).get(chanId) as any
      if (!chan) {
        this.createChannel({ id: chanId })
      } else if (chan.archived_at !== null) {
        throw new Error(`Channel '${chanId}' is archived`)
      }
      const mode: WakeMode = wakeMode ?? (handle === 'user' ? 'passive' : 'mention')
      this.db.query(`
        INSERT INTO channel_members (channel_id, agent_id, wake_mode)
        VALUES (?, ?, ?)
        ON CONFLICT(channel_id, agent_id) DO UPDATE SET wake_mode = excluded.wake_mode
      `).run(chanId, handle, mode)
      return { channel_id: chanId, agent_id: handle, wake_mode: mode }
    })
    return tx()
  }

  leaveChannel(channelId: string, agentId: string): boolean {
    const chanId = normalizeChannelId(channelId)
    const handle = normalizeHandle(agentId)
    const res = this.db.query(`DELETE FROM channel_members WHERE channel_id = ? AND agent_id = ?`).run(chanId, handle)
    return res.changes > 0
  }

  setChannelWakeMode(channelId: string, agentId: string, wakeMode: WakeMode): void {
    const chanId = normalizeChannelId(channelId)
    const handle = normalizeHandle(agentId)
    const res = this.db.query(`
      UPDATE channel_members SET wake_mode = ? WHERE channel_id = ? AND agent_id = ?
    `).run(wakeMode, chanId, handle)
    if (res.changes === 0) {
      throw new Error(`Agent '${handle}' is not a member of '${chanId}'`)
    }
  }

  archiveChannel(channelId: string, archived = true, now = Date.now()): void {
    const chanId = normalizeChannelId(channelId)
    const res = this.db.query(`
      UPDATE channels SET archived_at = ? WHERE id = ?
    `).run(archived ? now : null, chanId)
    if (res.changes === 0) throw new Error(`Unknown channel: '${chanId}'`)
  }

  getChannel(channelId: string) {
    const chanId = normalizeChannelId(channelId)
    const c = this.db.query(`SELECT * FROM channels WHERE id = ?`).get(chanId) as {
      id: string
      description: string
      archived_at: number | null
      created_at: number
    } | null
    if (!c) return null
    const members = this.db
      .query(`SELECT agent_id, wake_mode FROM channel_members WHERE channel_id = ? ORDER BY agent_id ASC`)
      .all(c.id) as Array<{ agent_id: string; wake_mode: WakeMode }>
    return {
      id: c.id,
      description: c.description,
      archived: c.archived_at !== null,
      archived_at: c.archived_at,
      members: members.map(m => m.agent_id),
      memberPolicies: members,
    }
  }

  listChannels(includeArchived = false) {
    const rows = (
      includeArchived
        ? this.db.query(`SELECT id FROM channels ORDER BY id ASC`).all()
        : this.db.query(`SELECT id FROM channels WHERE archived_at IS NULL ORDER BY id ASC`).all()
    ) as Array<{ id: string }>
    return rows.map(r => this.getChannel(r.id)!)
  }

  // ===========================================================================
  // Session Binding & Leases
  // ===========================================================================
  bindAgentSession(params: {
    agentId: string
    endpointKey: string
    nativeSessionId: string
    workspace?: string | null
    pid?: number | null
    processStartedAt?: string | null
    nativeStatus?: string
    capabilities?: Record<string, unknown>
    pushConfig?: Record<string, unknown>
    connectionEpoch?: string | null
    allowRebind?: boolean
    leaseMs?: number
    now?: number
  }): {
    ok: boolean
    conflict?: string
    session?: AgentSessionRecord
    rebound?: boolean
  } {
    const now = params.now ?? Date.now()
    const leaseExpiresAt = now + (params.leaseMs ?? DEFAULT_SESSION_LEASE_MS)
    const agentId = normalizeHandle(params.agentId)
    const capabilitiesJson = JSON.stringify(params.capabilities ?? {})
    const pushConfigJson = JSON.stringify(params.pushConfig ?? {})

    const tx = this.db.transaction(() => {
      const agent = this.getAgent(agentId)
      if (!agent) throw new Error(`Unknown agent handle: ${agentId}`)
      if (agent.tool === 'user') throw new Error(`Reserved 'user' handle cannot bind a native agent session`)

      const existingByNative = this.db.query(`
        SELECT * FROM agent_sessions WHERE endpoint_key = ? AND native_session_id = ?
      `).get(params.endpointKey, params.nativeSessionId) as AgentSessionRecord | null

      if (existingByNative && existingByNative.agent_id !== agentId) {
        if (!params.allowRebind) {
          return {
            ok: false,
            conflict: `Native session ${params.nativeSessionId} is already bound to agent '${existingByNative.agent_id}'`,
          }
        }
        this.db.query('DELETE FROM agent_sessions WHERE id = ?').run(existingByNative.id)
      }

      const currentForHandle = this.db.query(`
        SELECT * FROM agent_sessions WHERE agent_id = ? AND is_current = 1
      `).get(agentId) as AgentSessionRecord | null

      if (currentForHandle && currentForHandle.native_session_id === params.nativeSessionId) {
        const nextEndpointKey =
          params.endpointKey.endsWith(':local') && !currentForHandle.endpoint_key.endsWith(':local')
            ? currentForHandle.endpoint_key
            : params.endpointKey
        this.db.query(`
          UPDATE agent_sessions
          SET endpoint_key = ?,
              workspace = COALESCE(?, workspace),
              pid = COALESCE(?, pid),
              process_started_at = COALESCE(?, process_started_at),
              native_status = COALESCE(?, native_status),
              capabilities = ?,
              push_config = ?,
              connection_epoch = COALESCE(?, connection_epoch),
              last_seen_at = ?,
              lease_expires_at = ?
          WHERE id = ?
        `).run(
          nextEndpointKey,
          params.workspace ?? null,
          params.pid ?? null,
          params.processStartedAt ?? null,
          params.nativeStatus ?? null,
          capabilitiesJson,
          pushConfigJson,
          params.connectionEpoch ?? null,
          now,
          leaseExpiresAt,
          currentForHandle.id
        )
        const updated = this.db.query('SELECT * FROM agent_sessions WHERE id = ?').get(currentForHandle.id) as AgentSessionRecord
        return { ok: true, session: updated, rebound: false }
      }

      if (currentForHandle && currentForHandle.lease_expires_at > now && !params.allowRebind) {
        return {
          ok: false,
          conflict: `Handle '${agentId}' is currently bound to active session '${currentForHandle.native_session_id}' (lease expires in ${currentForHandle.lease_expires_at - now}ms)`,
        }
      }

      if (currentForHandle) {
        this.db.query('UPDATE agent_sessions SET is_current = 0 WHERE id = ?').run(currentForHandle.id)
      }

      if (existingByNative && existingByNative.agent_id === agentId) {
        const maxGenRow = this.db.query('SELECT COALESCE(MAX(generation), 0) as max_gen FROM agent_sessions WHERE agent_id = ?').get(agentId) as { max_gen: number }
        const nextGen = maxGenRow.max_gen + 1
        this.db.query(`
          UPDATE agent_sessions
          SET is_current = 1,
              generation = ?,
              workspace = COALESCE(?, workspace),
              pid = ?,
              process_started_at = ?,
              native_status = ?,
              capabilities = ?,
              push_config = ?,
              connection_epoch = ?,
              last_seen_at = ?,
              lease_expires_at = ?
          WHERE id = ?
        `).run(
          nextGen,
          params.workspace ?? null,
          params.pid ?? null,
          params.processStartedAt ?? null,
          params.nativeStatus ?? 'online',
          capabilitiesJson,
          pushConfigJson,
          params.connectionEpoch ?? null,
          now,
          leaseExpiresAt,
          existingByNative.id
        )
        const reactivated = this.db.query('SELECT * FROM agent_sessions WHERE id = ?').get(existingByNative.id) as AgentSessionRecord
        return { ok: true, session: reactivated, rebound: Boolean(currentForHandle) }
      }

      const maxGenRow = this.db.query('SELECT COALESCE(MAX(generation), 0) as max_gen FROM agent_sessions WHERE agent_id = ?').get(agentId) as { max_gen: number }
      const nextGen = maxGenRow.max_gen + 1
      const sessionId = randomUUID()

      this.db.query(`
        INSERT INTO agent_sessions (
          id, agent_id, endpoint_key, native_session_id, generation, is_current,
          workspace, pid, process_started_at, native_status, capabilities, push_config,
          connection_epoch, last_seen_at, lease_expires_at
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        sessionId,
        agentId,
        params.endpointKey,
        params.nativeSessionId,
        nextGen,
        params.workspace ?? null,
        params.pid ?? null,
        params.processStartedAt ?? null,
        params.nativeStatus ?? 'online',
        capabilitiesJson,
        pushConfigJson,
        params.connectionEpoch ?? null,
        now,
        leaseExpiresAt
      )

      const created = this.db.query('SELECT * FROM agent_sessions WHERE id = ?').get(sessionId) as AgentSessionRecord
      return { ok: true, session: created, rebound: Boolean(currentForHandle) }
    })

    return tx()
  }

  getCurrentSession(agentId: string, now = Date.now()): AgentSessionRecord | null {
    const row = this.db.query(`
      SELECT * FROM agent_sessions WHERE agent_id = ? AND is_current = 1
    `).get(agentId.toLowerCase()) as AgentSessionRecord | null
    if (!row || row.lease_expires_at <= now) return null
    return row
  }

  expireStaleLeases(now = Date.now()): number {
    const res = this.db.query(`
      UPDATE agent_sessions
      SET native_status = 'expired'
      WHERE is_current = 1 AND lease_expires_at <= ? AND native_status != 'expired'
    `).run(now)
    return res.changes
  }

  // ===========================================================================
  // Direct Messages, Fan-Out Messages & Correlated Waits
  // ===========================================================================
  private validateMessageBody(body: string): void {
    if (!body || body.trim().length === 0) {
      throw new Error('Message body must not be empty')
    }
    if (Buffer.byteLength(body, 'utf-8') > MAX_MESSAGE_BYTES) {
      throw new Error(`Message body exceeds maximum size of ${MAX_MESSAGE_BYTES} bytes`)
    }
  }

  sendDirectMessage(params: {
    senderId: string
    senderSessionId?: string | null
    recipientId: string
    body: string
    clientMessageId?: string | null
    replyToId?: string | null
    metadata?: Record<string, unknown> | null
    wakeRequired?: boolean
    waitSeconds?: number
    now?: number
  }): {
    message: MessageRecord
    delivery: DeliveryRecord
    deduplicated: boolean
    waitId: string | null
    matchedWaitId: string | null
    matchedFanoutId: string | null
  } {
    const now = params.now ?? Date.now()
    const senderId = normalizeHandle(params.senderId)
    const recipientId = normalizeHandle(params.recipientId)
    this.validateMessageBody(params.body)

    const tx = this.db.transaction(() => {
      if (!this.getAgent(senderId)) throw new Error(`Unknown sender handle: ${senderId}`)
      if (!this.getAgent(recipientId)) throw new Error(`Unknown recipient handle: ${recipientId}`)

      if (params.clientMessageId) {
        const existingMsg = this.db.query(`
          SELECT * FROM messages WHERE sender_id = ? AND client_message_id = ?
        `).get(senderId, params.clientMessageId) as MessageRecord | null
        if (existingMsg) {
          if (existingMsg.recipient_id !== recipientId || existingMsg.body !== params.body) {
            throw new Error(`Idempotency conflict: client_message_id '${params.clientMessageId}' reused with different target or body`)
          }
          const existingDelivery = this.db.query(`
            SELECT * FROM deliveries WHERE message_id = ? AND recipient_id = ?
          `).get(existingMsg.id, recipientId) as DeliveryRecord
          const existingWait = this.db.query(`
            SELECT id FROM reply_waits WHERE request_message_id = ? AND caller_agent_id = ? LIMIT 1
          `).get(existingMsg.id, senderId) as { id: string } | null
          return {
            message: existingMsg,
            delivery: existingDelivery,
            deduplicated: true,
            waitId: existingWait?.id ?? null,
            matchedWaitId: null,
            matchedFanoutId: null,
          }
        }
      }

      const resolvedReplyTo = params.replyToId ?? null
      if (resolvedReplyTo) {
        const parent = this.db.query('SELECT id FROM messages WHERE id = ?').get(resolvedReplyTo)
        if (!parent) throw new Error(`Unknown reply_to_id: ${params.replyToId}`)
      }

      const messageId = randomUUID()
      const metadataJson = params.metadata ? JSON.stringify(params.metadata) : null

      let matchedWaitId: string | null = null
      let matchedFanoutId: string | null = null
      if (resolvedReplyTo) {
        const activeWait = this.db.query(`
          SELECT * FROM reply_waits
          WHERE request_message_id = ?
            AND expected_replier_id = ?
            AND caller_agent_id = ?
            AND status = 'waiting'
            AND expires_at > ?
          ORDER BY created_at ASC
          LIMIT 1
        `).get(resolvedReplyTo, senderId, recipientId, now) as {
          id: string
          fanout_id: string | null
        } | null

        if (activeWait) {
          matchedWaitId = activeWait.id
          matchedFanoutId = activeWait.fanout_id
        }
      }

      this.db.query(`
        INSERT INTO messages (
          id, sender_id, sender_session_id, client_message_id,
          recipient_id, channel_id, fanout_id, body, reply_to_id, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)
      `).run(
        messageId,
        senderId,
        params.senderSessionId ?? null,
        params.clientMessageId ?? null,
        recipientId,
        params.body,
        resolvedReplyTo,
        metadataJson,
        now
      )

      const initialStatus: DeliveryStatus = matchedWaitId ? 'pushing' : 'queued'
      const initialClaim = matchedWaitId ? `wait_reserve:${matchedWaitId}` : null
      const wakeReq = params.wakeRequired === false || recipientId === 'user' ? 0 : 1

      this.db.query(`
        INSERT INTO deliveries (
          message_id, recipient_id, wake_required, status, active_claim_token, next_retry_at
        ) VALUES (?, ?, ?, ?, ?, ?)
      `).run(messageId, recipientId, wakeReq, initialStatus, initialClaim, now)

      if (matchedWaitId) {
        this.db.query(`
          UPDATE reply_waits SET status = 'reserved', reply_id = ? WHERE id = ?
        `).run(messageId, matchedWaitId)
      }

      if (resolvedReplyTo) {
        this.db.query(`
          UPDATE deliveries
          SET status = 'acked',
              acked_at = COALESCE(acked_at, ?),
              ack_source = COALESCE(ack_source, 'correlated_reply')
          WHERE message_id = ? AND recipient_id = ? AND status IN ('queued', 'pushed', 'unknown')
        `).run(now, resolvedReplyTo, senderId)
      }

      let waitId: string | null = null
      if (params.waitSeconds && params.waitSeconds > 0) {
        waitId = randomUUID()
        const boundedWaitSec = Math.min(params.waitSeconds, 120)
        this.db.query(`
          INSERT INTO reply_waits (
            id, request_message_id, fanout_id, caller_agent_id, expected_replier_id, status, expires_at, created_at
          ) VALUES (?, ?, NULL, ?, ?, 'waiting', ?, ?)
        `).run(waitId, messageId, senderId, recipientId, now + boundedWaitSec * 1000, now)
      }

      const message = this.db.query('SELECT * FROM messages WHERE id = ?').get(messageId) as MessageRecord
      const delivery = this.db.query('SELECT * FROM deliveries WHERE message_id = ? AND recipient_id = ?').get(messageId, recipientId) as DeliveryRecord

      return {
        message,
        delivery,
        deduplicated: false,
        waitId,
        matchedWaitId,
        matchedFanoutId,
      }
    })

    return tx()
  }

  /**
   * Sends a coordinated fan-out message to multiple recipient handles atomically.
   */
  sendFanoutMessage(params: {
    senderId: string
    senderSessionId?: string | null
    recipients: string[]
    body: string
    clientMessageId?: string | null
    wakeRequired?: boolean
    waitSeconds?: number
    now?: number
  }): {
    fanoutId: string
    deduplicated: boolean
    messages: MessageRecord[]
    deliveries: DeliveryRecord[]
    waitIds: string[]
  } {
    const now = params.now ?? Date.now()
    const senderId = normalizeHandle(params.senderId)
    this.validateMessageBody(params.body)

    const uniqueRecipients = Array.from(
      new Set((params.recipients ?? []).map(r => normalizeHandle(r)).filter(r => r !== senderId))
    )
    if (uniqueRecipients.length === 0) {
      throw new Error('Fan-out requires at least one distinct recipient handle')
    }

    const tx = this.db.transaction(() => {
      if (!this.getAgent(senderId)) throw new Error(`Unknown sender handle: ${senderId}`)
      for (const r of uniqueRecipients) {
        if (!this.getAgent(r)) throw new Error(`Unknown recipient handle in fan-out: '${r}'`)
      }

      if (params.clientMessageId) {
        const existingFanout = this.db.query(`
          SELECT * FROM fanouts WHERE sender_id = ? AND client_message_id = ?
        `).get(senderId, params.clientMessageId) as { id: string; body: string; recipient_count: number } | null
        if (existingFanout) {
          if (existingFanout.body !== params.body || existingFanout.recipient_count !== uniqueRecipients.length) {
            throw new Error(`Idempotency conflict: fanout client_message_id '${params.clientMessageId}' reused with different parameters`)
          }
          const msgs = this.db.query(`SELECT * FROM messages WHERE fanout_id = ? ORDER BY seq ASC`).all(existingFanout.id) as MessageRecord[]
          const delivs = msgs.map(
            m => this.db.query(`SELECT * FROM deliveries WHERE message_id = ?`).get(m.id) as DeliveryRecord
          )
          const waits = (
            this.db.query(`SELECT id FROM reply_waits WHERE fanout_id = ?`).all(existingFanout.id) as Array<{ id: string }>
          ).map(w => w.id)
          return {
            fanoutId: existingFanout.id,
            deduplicated: true,
            messages: msgs,
            deliveries: delivs,
            waitIds: waits,
          }
        }
      }

      const fanoutId = randomUUID()
      const wakeFlag = params.wakeRequired === false ? 0 : 1
      this.db.query(`
        INSERT INTO fanouts (id, sender_id, client_message_id, body, wake_required, recipient_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(fanoutId, senderId, params.clientMessageId ?? null, params.body, wakeFlag, uniqueRecipients.length, now)

      const messages: MessageRecord[] = []
      const deliveries: DeliveryRecord[] = []
      const waitIds: string[] = []
      const boundedWaitSec = params.waitSeconds && params.waitSeconds > 0 ? Math.min(params.waitSeconds, 120) : 0

      for (const recipientId of uniqueRecipients) {
        const msgId = randomUUID()
        const perMsgClientId = params.clientMessageId ? `${params.clientMessageId}:${recipientId}` : null
        this.db.query(`
          INSERT INTO messages (
            id, sender_id, sender_session_id, client_message_id,
            recipient_id, channel_id, fanout_id, body, reply_to_id, metadata, created_at
          ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, NULL, ?, ?)
        `).run(
          msgId,
          senderId,
          params.senderSessionId ?? null,
          perMsgClientId,
          recipientId,
          fanoutId,
          params.body,
          JSON.stringify({ fanoutId, recipients: uniqueRecipients }),
          now
        )

        const recWake = wakeFlag === 1 && recipientId !== 'user' ? 1 : 0
        this.db.query(`
          INSERT INTO deliveries (
            message_id, recipient_id, wake_required, status, active_claim_token, next_retry_at
          ) VALUES (?, ?, ?, 'queued', NULL, ?)
        `).run(msgId, recipientId, recWake, now)

        if (boundedWaitSec > 0) {
          const waitId = randomUUID()
          this.db.query(`
            INSERT INTO reply_waits (
              id, request_message_id, fanout_id, caller_agent_id, expected_replier_id, status, expires_at, created_at
            ) VALUES (?, ?, ?, ?, ?, 'waiting', ?, ?)
          `).run(waitId, msgId, fanoutId, senderId, recipientId, now + boundedWaitSec * 1000, now)
          waitIds.push(waitId)
        }

        messages.push(this.db.query(`SELECT * FROM messages WHERE id = ?`).get(msgId) as MessageRecord)
        deliveries.push(
          this.db.query(`SELECT * FROM deliveries WHERE message_id = ? AND recipient_id = ?`).get(msgId, recipientId) as DeliveryRecord
        )
      }

      return {
        fanoutId,
        deduplicated: false,
        messages,
        deliveries,
        waitIds,
      }
    })

    return tx()
  }

  getFanoutStatus(params: { fanoutId: string; callerId?: string; autoAck?: boolean; now?: number }) {
    const now = params.now ?? Date.now()
    const tx = this.db.transaction(() => {
      const fanout = this.db.query(`SELECT * FROM fanouts WHERE id = ?`).get(params.fanoutId) as {
        id: string
        sender_id: string
        body: string
        wake_required: number
        recipient_count: number
        created_at: number
      } | null
      if (!fanout) throw new Error(`Unknown fanout_id: '${params.fanoutId}'`)

      const childMsgs = this.db.query(`SELECT * FROM messages WHERE fanout_id = ? ORDER BY seq ASC`).all(fanout.id) as MessageRecord[]
      const deliveries: Array<{
        recipient_id: string
        message_id: string
        status: DeliveryStatus
        last_error: string | null
        replied: boolean
      }> = []
      const replies: Record<
        string,
        { message_id: string; from: string; reply_to_id: string; body: string; created_at: number }
      > = {}
      const pendingRecipients: string[] = []

      for (const m of childMsgs) {
        const recId = m.recipient_id!
        const deliv = this.db.query(`SELECT * FROM deliveries WHERE message_id = ? AND recipient_id = ?`).get(m.id, recId) as DeliveryRecord
        const replyRow = this.db.query(`
          SELECT * FROM messages WHERE reply_to_id = ? AND sender_id = ? ORDER BY seq ASC LIMIT 1
        `).get(m.id, recId) as MessageRecord | null

        if (replyRow) {
          replies[recId] = {
            message_id: replyRow.id,
            from: replyRow.sender_id,
            reply_to_id: m.id,
            body: replyRow.body,
            created_at: replyRow.created_at,
          }
          if (params.autoAck && params.callerId && params.callerId.toLowerCase() === fanout.sender_id) {
            this.db.query(`
              UPDATE deliveries
              SET status = 'acked',
                  active_claim_token = NULL,
                  acked_at = COALESCE(acked_at, ?),
                  ack_source = COALESCE(ack_source, 'fanout_collect')
              WHERE message_id = ? AND recipient_id = ? AND status IN ('queued', 'pushing', 'pushed', 'unknown')
            `).run(now, replyRow.id, fanout.sender_id)
            this.db.query(`
              UPDATE reply_waits SET status = 'completed'
              WHERE request_message_id = ? AND status IN ('waiting', 'reserved', 'returning')
            `).run(m.id)
          }
        } else {
          pendingRecipients.push(recId)
        }

        deliveries.push({
          recipient_id: recId,
          message_id: m.id,
          status: deliv.status,
          last_error: deliv.last_error,
          replied: Boolean(replyRow),
        })
      }

      const repliedCount = Object.keys(replies).length
      const status =
        repliedCount === childMsgs.length
          ? 'complete'
          : repliedCount > 0
            ? 'partial'
            : 'pending'

      return {
        ok: true,
        fanout_id: fanout.id,
        from: fanout.sender_id,
        body: fanout.body,
        created_at: fanout.created_at,
        status,
        replied_count: repliedCount,
        total_recipients: childMsgs.length,
        deliveries,
        replies,
        pending_recipients: pendingRecipients,
      }
    })
    return tx()
  }

  claimReservedWaitReply(waitId: string, now = Date.now()): MessageRecord | null {
    const tx = this.db.transaction(() => {
      const wait = this.db.query('SELECT * FROM reply_waits WHERE id = ?').get(waitId) as {
        id: string
        status: string
        reply_id: string | null
      } | null
      if (!wait || wait.status !== 'reserved' || !wait.reply_id) return null

      this.db.query(`UPDATE reply_waits SET status = 'returning' WHERE id = ?`).run(waitId)
      return this.db.query('SELECT * FROM messages WHERE id = ?').get(wait.reply_id) as MessageRecord
    })
    return tx()
  }

  completeWaitReplyHandoff(waitId: string, now = Date.now()): void {
    const tx = this.db.transaction(() => {
      const wait = this.db.query('SELECT * FROM reply_waits WHERE id = ?').get(waitId) as {
        id: string
        caller_agent_id: string
        reply_id: string | null
      } | null
      if (!wait || !wait.reply_id) return

      this.db.query(`UPDATE reply_waits SET status = 'completed' WHERE id = ?`).run(waitId)
      this.db.query(`
        UPDATE deliveries
        SET status = 'acked',
            active_claim_token = NULL,
            acked_at = ?,
            ack_source = 'inline_wait_return'
        WHERE message_id = ? AND recipient_id = ?
      `).run(now, wait.reply_id, wait.caller_agent_id)
    })
    tx()
  }

  cancelOrExpireWait(waitId: string, reason: 'expired' | 'cancelled' | 'unknown', now = Date.now()): void {
    const tx = this.db.transaction(() => {
      const wait = this.db.query('SELECT * FROM reply_waits WHERE id = ?').get(waitId) as {
        id: string
        caller_agent_id: string
        status: string
        reply_id: string | null
      } | null
      if (!wait || wait.status === 'completed') return

      if (wait.status === 'returning' && wait.reply_id) {
        this.db.query(`UPDATE reply_waits SET status = 'unknown' WHERE id = ?`).run(waitId)
        this.db.query(`
          UPDATE deliveries
          SET status = 'unknown',
              active_claim_token = NULL,
              last_error = 'Caller disconnected while inline wait reply was returning'
          WHERE message_id = ? AND recipient_id = ?
        `).run(wait.reply_id, wait.caller_agent_id)
        return
      }

      if (wait.status === 'reserved' && wait.reply_id) {
        this.db.query(`UPDATE reply_waits SET status = ? WHERE id = ?`).run(reason, waitId)
        this.db.query(`
          UPDATE deliveries
          SET status = 'queued',
              active_claim_token = NULL,
              next_retry_at = ?
          WHERE message_id = ? AND recipient_id = ? AND active_claim_token = ?
        `).run(now, wait.reply_id, wait.caller_agent_id, `wait_reserve:${waitId}`)
        return
      }

      this.db.query(`UPDATE reply_waits SET status = ? WHERE id = ?`).run(reason, waitId)
    })
    tx()
  }

  // ===========================================================================
  // Push Dispatcher Claim & Attempt State Machine
  // ===========================================================================
  claimDeliveriesForDispatch(params: {
    recipientId?: string
    limit?: number
    route?: string
    now?: number
  } = {}): ClaimedDelivery[] {
    const now = params.now ?? Date.now()
    const limit = params.limit ?? 5
    const route = params.route ?? 'native_push'

    const tx = this.db.transaction(() => {
      const recipientClause = params.recipientId ? 'AND d.recipient_id = ?' : ''
      const queryArgs = params.recipientId ? [now, params.recipientId.toLowerCase(), limit] : [now, limit]

      const rows = this.db.query(`
        SELECT d.*
        FROM deliveries d
        WHERE d.wake_required = 1
          AND d.status = 'queued'
          AND d.active_claim_token IS NULL
          AND (d.next_retry_at IS NULL OR d.next_retry_at <= ?)
          ${recipientClause}
        ORDER BY d.id ASC
        LIMIT ?
      `).all(...queryArgs) as DeliveryRecord[]

      const claimed: ClaimedDelivery[] = []
      for (const d of rows) {
        const claimToken = randomUUID()
        const attemptNum = d.attempt_count + 1
        const nativeClientMessageId = `${d.id}:${attemptNum}`
        const session = this.getCurrentSession(d.recipient_id, now)

        const upd = this.db.query(`
          UPDATE deliveries
          SET status = 'pushing',
              active_claim_token = ?,
              attempt_count = ?,
              last_attempt_at = ?
          WHERE id = ? AND status = 'queued' AND active_claim_token IS NULL
        `).run(claimToken, attemptNum, now, d.id)
        if (upd.changes !== 1) continue

        this.db.query(`
          INSERT INTO delivery_attempts (
            delivery_id, claim_token, route, target_session_id, target_generation,
            native_client_message_id, stage, outcome, started_at
          ) VALUES (?, ?, ?, ?, ?, ?, 'claimed', 'in_progress', ?)
        `).run(d.id, claimToken, route, session?.id ?? null, session?.generation ?? null, nativeClientMessageId, now)

        const msg = this.db.query('SELECT * FROM messages WHERE id = ?').get(d.message_id) as MessageRecord
        const updatedDeliv = this.db.query('SELECT * FROM deliveries WHERE id = ?').get(d.id) as DeliveryRecord
        claimed.push({
          delivery: updatedDeliv,
          message: msg,
          claimToken,
          nativeClientMessageId,
          session,
        })
      }
      return claimed
    })

    return tx()
  }

  checkpointAttemptStage(params: {
    claimToken: string
    stage: string
    nativeReceipt?: Record<string, unknown> | null
  }): void {
    this.db.query(`
      UPDATE delivery_attempts
      SET stage = ?,
          native_receipt = COALESCE(?, native_receipt)
      WHERE claim_token = ? AND outcome = 'in_progress'
    `).run(params.stage, params.nativeReceipt ? JSON.stringify(params.nativeReceipt) : null, params.claimToken)
  }

  completeDeliveryAttempt(params: {
    deliveryId: number
    claimToken: string
    outcome: Exclude<AttemptOutcome, 'in_progress'>
    stage?: string
    nativeReceipt?: Record<string, unknown> | null
    error?: string | null
    retryDelayMs?: number | null
    now?: number
  }): DeliveryRecord | null {
    const now = params.now ?? Date.now()
    const tx = this.db.transaction(() => {
      const d = this.db.query('SELECT * FROM deliveries WHERE id = ?').get(params.deliveryId) as DeliveryRecord | null
      if (!d || d.active_claim_token !== params.claimToken) return d

      this.db.query(`
        UPDATE delivery_attempts
        SET outcome = ?,
            stage = COALESCE(?, stage),
            native_receipt = COALESCE(?, native_receipt),
            error = ?,
            ended_at = ?
        WHERE claim_token = ?
      `).run(
        params.outcome,
        params.stage ?? null,
        params.nativeReceipt ? JSON.stringify(params.nativeReceipt) : null,
        params.error ?? null,
        now,
        params.claimToken
      )

      if (params.outcome === 'accepted') {
        this.db.query(`
          UPDATE deliveries
          SET status = 'pushed',
              active_claim_token = NULL,
              pushed_at = ?,
              last_error = NULL
          WHERE id = ? AND active_claim_token = ?
        `).run(now, params.deliveryId, params.claimToken)
      } else if (params.outcome === 'unknown') {
        this.db.query(`
          UPDATE deliveries
          SET status = 'unknown',
              active_claim_token = NULL,
              last_error = ?
          WHERE id = ? AND active_claim_token = ?
        `).run(params.error ?? 'Ambiguous transport outcome after request sent', params.deliveryId, params.claimToken)
      } else {
        const exhausted = d.attempt_count >= MAX_PUSH_ATTEMPTS && (params.retryDelayMs ?? 5000) > 0
        const nextStatus: DeliveryStatus = exhausted ? 'failed' : 'queued'
        const retryAt = exhausted ? null : now + (params.retryDelayMs ?? Math.min(30_000, 1000 * 2 ** d.attempt_count))
        this.db.query(`
          UPDATE deliveries
          SET status = ?,
              active_claim_token = NULL,
              next_retry_at = ?,
              last_error = ?
          WHERE id = ? AND active_claim_token = ?
        `).run(nextStatus, retryAt, params.error ?? null, params.deliveryId, params.claimToken)
      }

      return this.db.query('SELECT * FROM deliveries WHERE id = ?').get(params.deliveryId) as DeliveryRecord
    })
    return tx()
  }

  // ===========================================================================
  // Inbox Read & Explicit Acknowledge
  // ===========================================================================
  readInbox(params: {
    recipientId: string
    fromSenderId?: string
    limit?: number
    afterSeq?: number
    includeUnknown?: boolean
    autoAck?: boolean
    ackSource?: string
    now?: number
  }): Array<{ message: MessageRecord; delivery: DeliveryRecord }> {
    const now = params.now ?? Date.now()
    const recipientId = normalizeHandle(params.recipientId)
    const limit = Math.min(Math.max(params.limit ?? 25, 1), 100)

    const tx = this.db.transaction(() => {
      const statuses = params.includeUnknown !== false ? `('queued', 'pushed', 'unknown')` : `('queued', 'pushed')`
      let sql = `
        SELECT d.id as d_id
        FROM deliveries d
        JOIN messages m ON m.id = d.message_id
        WHERE d.recipient_id = ?
          AND d.status IN ${statuses}
      `
      const args: Array<string | number> = [recipientId]
      if (params.fromSenderId) {
        sql += ` AND m.sender_id = ?`
        args.push(normalizeHandle(params.fromSenderId))
      }
      if (params.afterSeq !== undefined) {
        sql += ` AND m.seq > ?`
        args.push(params.afterSeq)
      }
      sql += ` ORDER BY m.seq ASC LIMIT ?`
      args.push(limit)

      const dIds = this.db.query(sql).all(...args) as Array<{ d_id: number }>
      const results: Array<{ message: MessageRecord; delivery: DeliveryRecord }> = []

      for (const { d_id } of dIds) {
        if (params.autoAck) {
          this.db.query(`
            UPDATE deliveries
            SET status = 'acked',
                acked_at = COALESCE(acked_at, ?),
                ack_source = COALESCE(ack_source, ?)
            WHERE id = ? AND status IN ('queued', 'pushed', 'unknown')
          `).run(now, params.ackSource ?? 'inbox_read', d_id)
        }
        const deliv = this.db.query('SELECT * FROM deliveries WHERE id = ?').get(d_id) as DeliveryRecord
        const msg = this.db.query('SELECT * FROM messages WHERE id = ?').get(deliv.message_id) as MessageRecord
        results.push({ message: msg, delivery: deliv })
      }
      return results
    })

    return tx()
  }

  ackMessages(recipientId: string, messageIds: string[], ackSource = 'explicit_ack', now = Date.now()): number {
    const rec = normalizeHandle(recipientId)
    let count = 0
    const tx = this.db.transaction(() => {
      for (const mId of messageIds) {
        const res = this.db.query(`
          UPDATE deliveries
          SET status = 'acked',
              acked_at = COALESCE(acked_at, ?),
              ack_source = COALESCE(ack_source, ?)
          WHERE recipient_id = ? AND message_id = ? AND status IN ('queued', 'pushed', 'unknown')
        `).run(now, ackSource, rec, mId)
        count += res.changes
      }
    })
    tx()
    return count
  }

  // ===========================================================================
  // Group Channels Post & Read
  // ===========================================================================
  postChannelMessage(params: {
    senderId: string
    senderSessionId?: string | null
    channelId: string
    body: string
    replyToId?: string | null
    clientMessageId?: string | null
    allowMentionAll?: boolean
    now?: number
  }): {
    message: MessageRecord
    deliveries: DeliveryRecord[]
    mentions: string[]
    deduplicated: boolean
  } {
    const now = params.now ?? Date.now()
    const senderId = normalizeHandle(params.senderId)
    const channelId = normalizeChannelId(params.channelId)
    this.validateMessageBody(params.body)

    const tx = this.db.transaction(() => {
      if (!this.getAgent(senderId)) throw new Error(`Unknown sender handle: ${senderId}`)
      const chan = this.db.query('SELECT * FROM channels WHERE id = ?').get(channelId) as { id: string; archived_at: number | null } | null
      if (!chan) throw new Error(`Unknown channel: ${channelId}`)
      if (chan.archived_at !== null) throw new Error(`Channel '${channelId}' is archived`)

      const memberRows = this.db.query(`
        SELECT agent_id, wake_mode FROM channel_members WHERE channel_id = ?
      `).all(channelId) as Array<{ agent_id: string; wake_mode: WakeMode }>

      const memberMap = new Map<string, WakeMode>()
      for (const m of memberRows) memberMap.set(m.agent_id, m.wake_mode)

      if (!memberMap.has(senderId) && senderId !== 'user') {
        throw new Error(`Agent '${senderId}' is not a member of channel '${channelId}' (call join_channel first)`)
      }

      if (params.clientMessageId) {
        const existingMsg = this.db.query(`
          SELECT * FROM messages WHERE sender_id = ? AND client_message_id = ?
        `).get(senderId, params.clientMessageId) as MessageRecord | null
        if (existingMsg) {
          if (existingMsg.channel_id !== channelId || existingMsg.body !== params.body) {
            throw new Error(`Idempotency conflict: client_message_id '${params.clientMessageId}' reused with different channel or body`)
          }
          const existingDeliveries = this.db.query('SELECT * FROM deliveries WHERE message_id = ? ORDER BY id ASC').all(existingMsg.id) as DeliveryRecord[]
          const parsedMeta = existingMsg.metadata ? JSON.parse(existingMsg.metadata) : {}
          return {
            message: existingMsg,
            deliveries: existingDeliveries,
            mentions: parsedMeta.mentions ?? [],
            deduplicated: true,
          }
        }
      }

      const validMentionsSet = new Set(Array.from(memberMap.keys()).filter(id => id !== 'user'))
      const parsed = parseChannelMentions(params.body, validMentionsSet)
      if (parsed.mentionAll && !params.allowMentionAll && senderId !== 'user') {
        throw new Error(`Channel broadcast (@all / @channel) requires operator permission`)
      }

      const finalMentions = parsed.mentionAll
        ? Array.from(validMentionsSet).filter(id => id !== senderId).sort()
        : parsed.mentions.filter(id => id !== senderId)
      const mentionedSet = new Set(finalMentions)

      const messageId = randomUUID()
      this.db.query(`
        INSERT INTO messages (
          id, sender_id, sender_session_id, client_message_id,
          recipient_id, channel_id, fanout_id, body, reply_to_id, metadata, created_at
        ) VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?)
      `).run(
        messageId,
        senderId,
        params.senderSessionId ?? null,
        params.clientMessageId ?? null,
        channelId,
        params.body,
        params.replyToId ?? null,
        JSON.stringify({ mentions: finalMentions, mentionAll: parsed.mentionAll }),
        now
      )

      const deliveries: DeliveryRecord[] = []
      for (const [memberId, wakeMode] of memberMap.entries()) {
        if (memberId === senderId) continue
        const shouldWake =
          memberId !== 'user' &&
          (wakeMode === 'always' || (wakeMode === 'mention' && mentionedSet.has(memberId)))

        this.db.query(`
          INSERT INTO deliveries (
            message_id, recipient_id, wake_required, status, next_retry_at
          ) VALUES (?, ?, ?, 'queued', ?)
        `).run(messageId, memberId, shouldWake ? 1 : 0, shouldWake ? now : null)

        const deliv = this.db.query('SELECT * FROM deliveries WHERE message_id = ? AND recipient_id = ?').get(messageId, memberId) as DeliveryRecord
        deliveries.push(deliv)
      }

      const message = this.db.query('SELECT * FROM messages WHERE id = ?').get(messageId) as MessageRecord
      return {
        message,
        deliveries,
        mentions: finalMentions,
        deduplicated: false,
      }
    })

    return tx()
  }

  readChannelHistory(params: {
    callerId: string
    channelId: string
    limit?: number
    afterSeq?: number
    beforeSeq?: number
    ackReturnedForCaller?: boolean
    now?: number
  }): MessageRecord[] {
    const callerId = normalizeHandle(params.callerId)
    const channelId = normalizeChannelId(params.channelId)
    const limit = Math.min(Math.max(params.limit ?? 20, 1), 100)
    const now = params.now ?? Date.now()

    const tx = this.db.transaction(() => {
      if (callerId !== 'user') {
        const member = this.db.query(`
          SELECT 1 FROM channel_members WHERE channel_id = ? AND agent_id = ?
        `).get(channelId, callerId)
        if (!member) {
          throw new Error(`Access denied: '${callerId}' is not a member of '${channelId}'`)
        }
      }

      let sql = 'SELECT * FROM messages WHERE channel_id = ?'
      const args: Array<string | number> = [channelId]
      if (params.afterSeq !== undefined) {
        sql += ' AND seq > ?'
        args.push(params.afterSeq)
      }
      if (params.beforeSeq !== undefined) {
        sql += ' AND seq < ?'
        args.push(params.beforeSeq)
      }
      const forwardCursor = params.afterSeq !== undefined && params.beforeSeq === undefined
      sql += forwardCursor ? ' ORDER BY seq ASC LIMIT ?' : ' ORDER BY seq DESC LIMIT ?'
      args.push(limit)

      const rawRows = this.db.query(sql).all(...args) as MessageRecord[]
      const rows = forwardCursor ? rawRows : rawRows.reverse()

      if (params.ackReturnedForCaller && callerId !== 'user' && rows.length > 0) {
        for (const msg of rows) {
          this.db.query(`
            UPDATE deliveries
            SET status = 'acked',
                acked_at = COALESCE(acked_at, ?),
                ack_source = COALESCE(ack_source, 'channel_read')
            WHERE message_id = ? AND recipient_id = ? AND status IN ('queued', 'pushed', 'unknown')
          `).run(now, msg.id, callerId)
        }
      }

      return rows
    })

    return tx()
  }

  addMessageRevision(params: {
    editorId: string
    messageId: string
    newBody: string
    now?: number
  }): { messageId: string; revisionId: number } {
    const editorId = normalizeHandle(params.editorId)
    const now = params.now ?? Date.now()
    this.validateMessageBody(params.newBody)
    const tx = this.db.transaction(() => {
      const msg = this.db.query('SELECT * FROM messages WHERE id = ?').get(params.messageId) as MessageRecord | null
      if (!msg) throw new Error(`Unknown message_id: ${params.messageId}`)
      if (msg.sender_id !== editorId && editorId !== 'user') {
        throw new Error(`Only the original sender ('${msg.sender_id}') may append a revision`)
      }
      const res = this.db.query(`
        INSERT INTO message_revisions (message_id, editor_id, body, created_at)
        VALUES (?, ?, ?, ?)
      `).run(msg.id, editorId, params.newBody, now)
      return { messageId: msg.id, revisionId: Number(res.lastInsertRowid) }
    })
    return tx()
  }
}
