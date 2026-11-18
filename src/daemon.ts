#!/usr/bin/env bun
import { EventEmitter } from 'node:events'
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { existsSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  authenticateRequest,
  AuthResult,
  BusAuthConfig,
  ensureStateDir,
  getDefaultStateDir,
  initBusAuth,
  safeTokenEquals,
  validateLoopbackHostAndOrigin,
} from './auth.ts'
import { MessageRecord, ToolType, TurnBusDB, WakeMode } from './db.ts'
import { ClaudeChannelNotificationPayload } from './adapters/claude.ts'
import { AgyCommandRunner } from './adapters/agy.ts'
import { CodexRpcCaller } from './adapters/codex.ts'
import { PushDispatcher } from './push-dispatcher.ts'
import {
  hasClaudeChannelServerFlag,
  readProcCmdline,
  RosterManager,
  scanAllLiveClaudeSessions,
} from './roster.ts'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PKG_VERSION: string = (() => {
  try {
    const pkg = JSON.parse(readFileSync(join(__dirname, '..', 'package.json'), 'utf-8'))
    return typeof pkg.version === 'string' ? pkg.version : '1.0.0'
  } catch {
    return '1.0.0'
  }
})()

export interface DaemonOptions {
  port?: number
  hostname?: string
  stateDir?: string
  dbPath?: string
  seedDefaultChannel?: boolean
  codexRpc?: CodexRpcCaller
  agyRunner?: AgyCommandRunner
  reconcileIntervalMs?: number
  maxWakesPerMinute?: number
  maxAgentReplyDepth?: number
}

interface WsClientData {
  id: string
  role: 'unauthenticated' | 'ui' | 'adapter'
  agentId?: string
  sessionId?: string
  connectionEpoch?: string
  channelCapable?: boolean
  authTimer?: Timer
}

export interface RunningDaemon {
  readonly port: number
  readonly instanceId: string
  readonly db: TurnBusDB
  readonly roster: RosterManager
  readonly dispatcher: PushDispatcher
  readonly auth: BusAuthConfig
  stop(): Promise<void>
}

function hashSessionToken(token: string): string {
  return createHash('sha256').update(token, 'utf-8').digest('hex')
}

async function readJsonBody<T = any>(req: Request, maxBytes = 512 * 1024): Promise<T> {
  const contentLength = Number(req.headers.get('content-length') ?? 0)
  if (contentLength > maxBytes) {
    throw Object.assign(new Error(`Payload Too Large (max ${maxBytes} bytes)`), { status: 413 })
  }
  if (!req.body) return {} as T
  const reader = req.body.getReader()
  const chunks: Uint8Array[] = []
  let totalBytes = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) {
        totalBytes += value.byteLength
        if (totalBytes > maxBytes) {
          await reader.cancel().catch(() => {})
          throw Object.assign(new Error(`Payload Too Large (max ${maxBytes} bytes)`), { status: 413 })
        }
        chunks.push(value)
      }
    }
  } finally {
    reader.releaseLock()
  }
  if (totalBytes === 0) return {} as T
  const raw = Buffer.concat(chunks, totalBytes).toString('utf-8')
  return (raw ? JSON.parse(raw) : {}) as T
}

function resolveChannelTargetAgent(role: string | undefined, callerId: string, bodyAgentId?: string): string {
  return role === 'ui' && bodyAgentId ? bodyAgentId : callerId
}

export function startTurnBusDaemon(options: DaemonOptions = {}): RunningDaemon {
  const port = options.port ?? Number(process.env.TURNBUS_PORT ?? 8790)
  const hostname = options.hostname ?? '127.0.0.1'
  const stateDir = ensureStateDir(options.stateDir ?? getDefaultStateDir())
  const auth = initBusAuth(stateDir)
  const db = new TurnBusDB(options.dbPath ?? join(stateDir, 'turnbus.sqlite'), {
    seedDefaultChannel: options.seedDefaultChannel,
  })
  const roster = new RosterManager(db, stateDir)
  const instanceId = randomUUID()
  const events = new EventEmitter()
  events.setMaxListeners(200)

  const adapterSockets = new Map<string, Set<any>>()
  const uiSockets = new Set<any>()
  const unauthSockets = new Set<any>()
  const sessionTokens = new Map<string, { agentId: string; sessionId: string | null }>()
  const agentSessionTokens = new Map<string, string>()
  const pendingWsPushAcks = new Map<
    string,
    { resolve: (res: { sent: boolean; ackedByAdapter: boolean; error?: string }) => void; timer: Timer; ws: any }
  >()

  const getActiveAdapterSocket = (agentId: string, requireChannelCapable = false): any | null => {
    const target = agentId.toLowerCase()
    const set = adapterSockets.get(target)
    if (!set || set.size === 0) return null
    const arr = Array.from(set).reverse()
    if (requireChannelCapable) {
      const currentSess = db.getCurrentSession(target)
      if (currentSess?.pid) {
        const liveCmdline = readProcCmdline(currentSess.pid)
        if (liveCmdline !== null && !hasClaudeChannelServerFlag(liveCmdline, 'turnbus')) {
          return null
        }
      }
      return arr.find(s => s.data?.channelCapable) ?? null
    }
    return arr[0] ?? null
  }

  const verifySessionToken = (rawToken: string): { agentId: string; sessionId: string | null } | null => {
    const tok = rawToken.trim()
    if (!tok) return null
    const mem = sessionTokens.get(tok)
    if (mem) {
      const cur = db.getCurrentSession(mem.agentId)
      if (mem.sessionId && (!cur || cur.lease_expires_at <= Date.now())) {
        sessionTokens.delete(tok)
        agentSessionTokens.delete(mem.agentId)
        return null
      }
      return mem
    }
    const tokenHash = hashSessionToken(tok)
    const persisted = db.findCurrentSessionByTokenHash(tokenHash)
    if (persisted) {
      const entry = { agentId: persisted.agent_id, sessionId: persisted.id }
      sessionTokens.set(tok, entry)
      agentSessionTokens.set(persisted.agent_id, tok)
      return entry
    }
    return null
  }

  const hasActiveBoundSession = (agentId: string): boolean => {
    const target = agentId.toLowerCase()
    const cur = db.getCurrentSession(target)
    if (cur && cur.lease_expires_at > Date.now()) {
      return true
    }
    const tok = agentSessionTokens.get(target)
    if (tok) {
      sessionTokens.delete(tok)
      agentSessionTokens.delete(target)
    }
    return false
  }

  const matchesHandleSessionToken = (agentId: string, providedToken: string | undefined): boolean => {
    if (!providedToken) return false
    const verified = verifySessionToken(providedToken)
    return Boolean(verified && verified.agentId === agentId.toLowerCase())
  }

  const resolveCallerIdentity = (
    req: Request,
    bodyCallerId?: string,
    role?: string
  ): { ok: boolean; callerId: string; error?: string } => {
    if (role === 'ui') {
      const explicitFor = (req.headers.get('x-turnbus-caller') || bodyCallerId || 'user').trim().toLowerCase()
      return { ok: true, callerId: explicitFor || 'user' }
    }
    const sessTok = req.headers.get('x-turnbus-session-token')?.trim()
    if (sessTok) {
      const bound = verifySessionToken(sessTok)
      if (!bound) {
        return { ok: false, callerId: '', error: 'Invalid or expired x-turnbus-session-token' }
      }
      const claimed = (req.headers.get('x-turnbus-caller') || bodyCallerId || '').trim().toLowerCase()
      if (claimed && claimed !== bound.agentId) {
        return {
          ok: false,
          callerId: '',
          error: `Caller identity '${claimed}' does not match session token bound to '${bound.agentId}'`,
        }
      }
      return { ok: true, callerId: bound.agentId }
    }
    const callerId = (req.headers.get('x-turnbus-caller') || bodyCallerId || '').trim().toLowerCase()
    if (!callerId) return { ok: false, callerId: '', error: 'Missing caller_id' }
    if (hasActiveBoundSession(callerId)) {
      return {
        ok: false,
        callerId: '',
        error: `Agent '${callerId}' has an active bound session and requires x-turnbus-session-token`,
      }
    }
    return { ok: true, callerId }
  }

  const notifyUiStateChanged = () => {
    const frame = JSON.stringify({ type: 'state_changed', ts: Date.now() })
    for (const ws of uiSockets) {
      try {
        ws.send(frame)
      } catch {}
    }
  }

  const sendViaClaudeAdapterWs = async (
    agentId: string,
    payload: ClaudeChannelNotificationPayload
  ): Promise<{ sent: boolean; ackedByAdapter: boolean; error?: string }> => {
    const target = agentId.toLowerCase()
    const ws = getActiveAdapterSocket(target, true)
    if (!ws || !ws.data?.channelCapable) {
      return {
        sent: false,
        ackedByAdapter: false,
        error: `Claude agent '${target}' does not have an active channel-capable adapter socket (requires --dangerously-load-development-channels server:turnbus)`,
      }
    }

    const requestId = randomUUID()
    return new Promise(resolve => {
      const timer = setTimeout(() => {
        pendingWsPushAcks.delete(requestId)
        resolve({
          sent: true,
          ackedByAdapter: false,
          error: 'Timed out waiting for stdio MCP adapter channel notification ack',
        })
      }, 4000)

      pendingWsPushAcks.set(requestId, { resolve, timer, ws })
      try {
        ws.send(JSON.stringify({ type: 'claude_channel_push', requestId, payload }))
      } catch (err: any) {
        clearTimeout(timer)
        pendingWsPushAcks.delete(requestId)
        resolve({ sent: false, ackedByAdapter: false, error: err?.message ?? String(err) })
      }
    })
  }

  const dispatcher = new PushDispatcher({
    db,
    roster,
    sendViaClaudeAdapterWs,
    codexRpc: options.codexRpc,
    agyRunner: options.agyRunner,
    maxWakesPerMinute: options.maxWakesPerMinute,
    maxAgentReplyDepth: options.maxAgentReplyDepth,
    onDeliveryStateChange: (_deliveryId, _messageId, recipientId) => {
      events.emit(`inbox:${recipientId}`)
      notifyUiStateChanged()
    },
  })

  const runReconciliation = async () => {
    try {
      db.expireStaleLeases()
      await dispatcher.reconcileCodexBindings()
      const claudeSessions = scanAllLiveClaudeSessions()
      for (const cs of claudeSessions) {
        const resolved = roster.resolveAgentIdentity({
          tool: 'claude',
          workspaceCwd: cs.cwd,
          nativeSessionId: cs.sessionId,
        })
        if (resolved.matchedBy === 'session' || resolved.matchedBy === 'workspace_rule') {
          db.bindAgentSession({
            agentId: resolved.agentId,
            endpointKey: 'claude:local',
            nativeSessionId: cs.sessionId,
            workspace: cs.cwd,
            pid: cs.pid,
            processStartedAt: cs.procStart,
            nativeStatus: cs.status,
            capabilities: {
              push: cs.channelPushEnabled ? 'channel' : 'none',
              version: cs.version,
            },
          })
        }
      }
      for (const a of db.listAgents().filter(ag => ag.tool === 'agy')) {
        const ws = getActiveAdapterSocket(a.id)
        const agyCreds = roster.getPrivateAgyCredentials(a.id)
        const curSess = db.getCurrentSession(a.id)
        const nativeSessionId = agyCreds?.conversationId ?? curSess?.native_session_id ?? null
        if (nativeSessionId && (ws || agyCreds)) {
          db.bindAgentSession({
            agentId: a.id,
            endpointKey: 'agy:local',
            nativeSessionId,
            workspace: curSess?.workspace ?? a.workspace,
            nativeStatus: 'online',
            capabilities: { push: agyCreds ? 'agentapi' : 'none' },
            pushConfig: agyCreds ? { lsAddress: agyCreds.lsAddress } : {},
          })
        }
      }
      await dispatcher.dispatchPending()
    } catch {}
  }

  const reconcileIntervalMs = options.reconcileIntervalMs ?? 5000
  const reconcileTimer = reconcileIntervalMs > 0 ? setInterval(runReconciliation, reconcileIntervalMs) : null
  if (reconcileIntervalMs > 0) void runReconciliation()

  function buildAgentRosterSummary() {
    const now = Date.now()
    return db.listAgents().map(a => {
      const sess = db.getCurrentSession(a.id)
      const isBound = Boolean(sess && sess.lease_expires_at > now)
      const wsConnected = Boolean(getActiveAdapterSocket(a.id))
      let pushCapability = 'none'
      if (a.tool === 'claude') {
        const capObj = sess ? JSON.parse(sess.capabilities || '{}') : {}
        pushCapability = Boolean(getActiveAdapterSocket(a.id, true)) || capObj.push === 'channel' ? 'channel' : 'none'
      } else if (a.tool === 'codex') {
        pushCapability = isBound ? 'codex_control' : 'none'
      } else if (a.tool === 'agy') {
        pushCapability = roster.getPrivateAgyCredentials(a.id) ? 'agentapi' : 'none'
      }

      const unreadRow = db.db
        .query(`SELECT COUNT(*) as c FROM deliveries WHERE recipient_id = ? AND status IN ('queued','pushed','unknown')`)
        .get(a.id) as { c: number }

      let parsedAliases: string[] = []
      try {
        parsedAliases = JSON.parse(a.aliases || '[]')
      } catch {}

      return {
        id: a.id,
        displayName: a.display_name,
        tool: a.tool,
        workspace: sess?.workspace ?? a.workspace,
        aliases: parsedAliases,
        bound: isBound,
        connected: wsConnected,
        nativeSessionId: sess?.native_session_id ?? null,
        nativeStatus: isBound ? sess!.native_status : 'offline',
        pushCapability,
        unreadCount: unreadRow?.c ?? 0,
        lastSeenAt: sess?.last_seen_at ?? null,
      }
    })
  }

  async function waitForReplyInline(
    waitId: string,
    waitSeconds: number,
    abortSignal?: AbortSignal
  ): Promise<MessageRecord | null> {
    const deadline = Date.now() + Math.min(waitSeconds, 120) * 1000
    const immediate = db.claimReservedWaitReply(waitId)
    if (immediate) return immediate

    while (Date.now() < deadline) {
      if (abortSignal?.aborted) {
        db.cancelOrExpireWait(waitId, 'cancelled')
        return null
      }
      const remainingMs = Math.max(50, deadline - Date.now())
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => {
          cleanup()
          resolve()
        }, Math.min(remainingMs, 1000))
        const onReady = () => {
          cleanup()
          resolve()
        }
        const cleanup = () => {
          clearTimeout(timer)
          events.off(`wait:${waitId}`, onReady)
          abortSignal?.removeEventListener('abort', onReady)
        }
        events.once(`wait:${waitId}`, onReady)
        abortSignal?.addEventListener('abort', onReady, { once: true })
      })

      if (abortSignal?.aborted) {
        db.cancelOrExpireWait(waitId, 'cancelled')
        return null
      }

      const claimed = db.claimReservedWaitReply(waitId)
      if (claimed) return claimed
    }

    db.cancelOrExpireWait(waitId, 'expired')
    return null
  }

  async function waitForFanoutRepliesInline(params: {
    fanoutId: string
    senderId: string
    waitIds: string[]
    waitSeconds: number
    waitFor: 'all' | 'any' | number
    abortSignal?: AbortSignal
  }) {
    const total = params.waitIds.length
    const needed =
      params.waitFor === 'any'
        ? 1
        : typeof params.waitFor === 'number'
          ? Math.max(1, Math.min(total, params.waitFor))
          : total
    const deadline = Date.now() + Math.min(params.waitSeconds, 120) * 1000

    while (Date.now() < deadline && !params.abortSignal?.aborted) {
      const snap = db.getFanoutStatus({ fanoutId: params.fanoutId, callerId: params.senderId, autoAck: false })
      if (snap.replied_count >= needed) break

      const rem = Math.max(50, deadline - Date.now())
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => {
          cleanup()
          resolve()
        }, Math.min(rem, 1000))
        const onReady = () => {
          cleanup()
          resolve()
        }
        const cleanup = () => {
          clearTimeout(timer)
          events.off(`fanout:${params.fanoutId}`, onReady)
          params.abortSignal?.removeEventListener('abort', onReady)
        }
        events.once(`fanout:${params.fanoutId}`, onReady)
        params.abortSignal?.addEventListener('abort', onReady, { once: true })
      })
    }

    if (params.abortSignal?.aborted) {
      for (const wId of params.waitIds) db.cancelOrExpireWait(wId, 'cancelled')
      return null
    }

    const finalSnap = db.getFanoutStatus({ fanoutId: params.fanoutId, callerId: params.senderId, autoAck: true })
    for (const wId of params.waitIds) {
      db.cancelOrExpireWait(wId, 'expired')
    }
    return finalSnap
  }

  type RouteHandler = (req: Request, url: URL, authRes: AuthResult) => Promise<Response> | Response

  const routes: Record<string, RouteHandler> = {
    'POST /api/agents/register': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        tool: ToolType
        workspaceCwd?: string | null
        explicitAgentId?: string | null
        displayName?: string | null
        setWorkspaceDefault?: boolean
        nativeSessionId?: string | null
        endpointKey?: string | null
        pid?: number | null
        processStartedAt?: string | null
        nativeStatus?: string
        channelCapable?: boolean
        lsAddress?: string | null
        csrfToken?: string | null
        agentApiExe?: string | null
        allowRebind?: boolean
        issueSessionToken?: boolean
      }>(req)

      if (body.tool === 'user' && authRes.role !== 'ui') {
        return Response.json({ error: "Only UI token may act as 'user'" }, { status: 403 })
      }

      const resolved = roster.resolveAgentIdentity({
        tool: body.tool,
        workspaceCwd: body.workspaceCwd,
        explicitAgentId: body.explicitAgentId,
        displayName: body.displayName,
        nativeSessionId: body.nativeSessionId,
        setWorkspaceDefault: body.setWorkspaceDefault,
      })

      const existingAgyCreds = body.tool === 'agy' ? roster.getPrivateAgyCredentials(resolved.agentId) : null
      const currentBoundSess = db.getCurrentSession(resolved.agentId)
      const isHandleBound = hasActiveBoundSession(resolved.agentId)
      const providedHeaderTok = req.headers.get('x-turnbus-session-token')?.trim()
      const hasMatchingHandleToken =
        authRes.role === 'ui' || matchesHandleSessionToken(resolved.agentId, providedHeaderTok)

      if (
        isHandleBound &&
        body.allowRebind &&
        body.nativeSessionId &&
        currentBoundSess?.native_session_id &&
        body.nativeSessionId !== currentBoundSess.native_session_id &&
        !hasMatchingHandleToken
      ) {
        return Response.json(
          { error: `Rebinding active handle '${resolved.agentId}' requires its x-turnbus-session-token or UI role` },
          { status: 403 }
        )
      }

      let effectiveChannelCapable = Boolean(body.channelCapable)
      if (body.tool === 'claude' && body.pid) {
        const liveCmdline = readProcCmdline(body.pid)
        if (liveCmdline !== null) {
          effectiveChannelCapable = hasClaudeChannelServerFlag(liveCmdline, 'turnbus')
        }
      }

      let bindResult = null
      const effectiveNativeSessionId = body.nativeSessionId ?? currentBoundSess?.native_session_id ?? null
      if (effectiveNativeSessionId) {
        bindResult = db.bindAgentSession({
          agentId: resolved.agentId,
          endpointKey: body.endpointKey ?? `${body.tool}:local`,
          nativeSessionId: effectiveNativeSessionId,
          workspace: body.workspaceCwd,
          pid: body.pid ?? null,
          processStartedAt: body.processStartedAt ?? null,
          nativeStatus: body.nativeStatus ?? 'online',
          capabilities: {
            push:
              body.tool === 'claude'
                ? effectiveChannelCapable
                  ? 'channel'
                  : 'none'
                : body.tool === 'codex'
                  ? 'codex_control'
                  : body.tool === 'agy' && ((body.lsAddress && body.csrfToken) || existingAgyCreds)
                    ? 'agentapi'
                    : 'none',
          },
          pushConfig: body.lsAddress
            ? { lsAddress: body.lsAddress }
            : existingAgyCreds
              ? { lsAddress: existingAgyCreds.lsAddress }
              : {},
          connectionEpoch: randomUUID(),
          allowRebind: body.allowRebind && (!isHandleBound || hasMatchingHandleToken),
        })
      }

      const bindOk = bindResult ? bindResult.ok : true
      if (bindOk && body.tool === 'agy' && body.lsAddress && body.csrfToken && effectiveNativeSessionId) {
        roster.setPrivateAgyCredentials(resolved.agentId, {
          conversationId: effectiveNativeSessionId,
          lsAddress: body.lsAddress,
          csrfToken: body.csrfToken,
          agentApiExe: body.agentApiExe ?? undefined,
          updatedAt: Date.now(),
        })
      }

      let mintedSessionToken: string | null = null
      if (bindOk) {
        const prevTok = agentSessionTokens.get(resolved.agentId)
        if (prevTok && !bindResult?.rebound) {
          mintedSessionToken = prevTok
        } else if (
          providedHeaderTok &&
          !bindResult?.rebound &&
          matchesHandleSessionToken(resolved.agentId, providedHeaderTok)
        ) {
          mintedSessionToken = providedHeaderTok
        } else {
          if (prevTok) sessionTokens.delete(prevTok)
          mintedSessionToken = randomBytes(24).toString('hex')
        }
        if (body.issueSessionToken !== false) {
          agentSessionTokens.set(resolved.agentId, mintedSessionToken)
          db.setSessionTokenHash(resolved.agentId, hashSessionToken(mintedSessionToken))
        }
        sessionTokens.set(mintedSessionToken, {
          agentId: resolved.agentId,
          sessionId: bindResult?.session?.id ?? db.getCurrentSession(resolved.agentId)?.id ?? null,
        })
      }

      notifyUiStateChanged()
      if (bindOk) void dispatcher.dispatchPending(resolved.agentId)

      return Response.json({
        ok: bindOk,
        agentId: resolved.agentId,
        displayName: resolved.displayName,
        matchedBy: resolved.matchedBy,
        conflict: bindResult?.conflict ?? null,
        session: db.getCurrentSession(resolved.agentId) ?? bindResult?.session ?? null,
        sessionToken: mintedSessionToken,
      })
    },

    'POST /api/agents/rename': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        old_handle?: string
        new_handle?: string
        display_name?: string
        aliases?: string[]
        set_workspace_default?: boolean
      }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id ?? body.old_handle, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const targetOld = (body.old_handle || callerCheck.callerId).trim().toLowerCase()
      if (authRes.role !== 'ui' && callerCheck.callerId !== targetOld) {
        return Response.json({ error: `Agent '${callerCheck.callerId}' may only rename its own handle` }, { status: 403 })
      }
      const updated = db.renameAgent({
        oldId: targetOld,
        newId: body.new_handle,
        displayName: body.display_name,
        aliases: body.aliases,
        setWorkspaceDefault: body.set_workspace_default,
      })
      if (body.new_handle && updated.id !== targetOld) {
        const tok = agentSessionTokens.get(targetOld)
        if (tok) {
          agentSessionTokens.delete(targetOld)
          agentSessionTokens.set(updated.id, tok)
          const st = sessionTokens.get(tok)
          if (st) st.agentId = updated.id
        }
        const socks = adapterSockets.get(targetOld)
        if (socks) {
          adapterSockets.delete(targetOld)
          for (const s of socks) s.data.agentId = updated.id
          adapterSockets.set(updated.id, socks)
        }
        const creds = roster.getPrivateAgyCredentials(targetOld)
        if (creds) roster.setPrivateAgyCredentials(updated.id, creds)
      }
      notifyUiStateChanged()
      return Response.json({ ok: true, agent: updated })
    },

    'GET /api/agents': async () => {
      await dispatcher.reconcileCodexBindings()
      return Response.json({ agents: buildAgentRosterSummary(), workspaceRules: db.listWorkspaceRules() })
    },

    'POST /api/messages/send': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        caller_session_id?: string | null
        to: string
        message: string
        reply_to_id?: string | null
        client_message_id?: string
        wait_seconds?: number
        wake?: boolean
        metadata?: Record<string, unknown>
      }>(req)

      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const senderId = callerCheck.callerId
      if (senderId === 'user' && authRes.role !== 'ui') {
        return Response.json({ error: "Agent token may not impersonate 'user'" }, { status: 403 })
      }
      if (!body.to) {
        return Response.json({ error: "Missing 'to' recipient handle" }, { status: 400 })
      }

      const recipientId = roster.resolveTargetHandle(body.to)
      const waitSeconds = Math.max(0, Math.min(Number(body.wait_seconds ?? 0), 120))
      const sent = db.sendDirectMessage({
        senderId,
        senderSessionId: body.caller_session_id ?? null,
        recipientId,
        body: body.message,
        clientMessageId: body.client_message_id,
        replyToId: body.reply_to_id ?? null,
        metadata: body.metadata,
        wakeRequired: body.wake !== false,
        waitSeconds,
      })

      if (sent.matchedWaitId) {
        events.emit(`wait:${sent.matchedWaitId}`)
        if (sent.matchedFanoutId) {
          events.emit(`fanout:${sent.matchedFanoutId}`)
        }
      } else {
        events.emit(`inbox:${recipientId}`)
        void dispatcher.dispatchPending(recipientId)
      }

      notifyUiStateChanged()

      if (waitSeconds > 0 && sent.waitId) {
        const replyMsg = await waitForReplyInline(sent.waitId, waitSeconds, req.signal)
        if (replyMsg) {
          if (req.signal.aborted) {
            db.cancelOrExpireWait(sent.waitId, 'unknown')
            return Response.json({ error: 'Client disconnected during reply handoff' }, { status: 499 })
          }
          db.completeWaitReplyHandoff(sent.waitId)
          notifyUiStateChanged()
          const latestDeliv = db.db
            .query('SELECT status FROM deliveries WHERE id = ?')
            .get(sent.delivery.id) as { status: string } | null
          return Response.json({
            ok: true,
            status: 'replied',
            message_id: sent.message.id,
            client_message_id: sent.message.client_message_id,
            from: senderId,
            to: recipientId,
            delivery_status: latestDeliv?.status ?? sent.delivery.status,
            reply: {
              message_id: replyMsg.id,
              from: replyMsg.sender_id,
              body: replyMsg.body,
              created_at: replyMsg.created_at,
            },
          })
        }
      }

      const latestDeliv = db.db
        .query('SELECT status, last_error FROM deliveries WHERE id = ?')
        .get(sent.delivery.id) as { status: string; last_error: string | null } | null

      return Response.json({
        ok: true,
        status: waitSeconds > 0 ? 'pending' : 'accepted',
        message_id: sent.message.id,
        client_message_id: sent.message.client_message_id,
        from: senderId,
        to: recipientId,
        delivery_status: latestDeliv?.status ?? sent.delivery.status,
        last_error: latestDeliv?.last_error ?? null,
        deduplicated: sent.deduplicated,
      })
    },

    'POST /api/messages/fanout': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        caller_session_id?: string | null
        to: string[]
        message: string
        client_message_id?: string
        wake?: boolean
        wait_seconds?: number
        wait_for?: 'all' | 'any' | number
      }>(req)

      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const senderId = callerCheck.callerId
      if (!Array.isArray(body.to) || body.to.length === 0) {
        return Response.json({ error: "Missing 'to' array of recipient handles" }, { status: 400 })
      }

      const resolvedRecipients = body.to.map(t => roster.resolveTargetHandle(t))
      const waitSeconds = Math.max(0, Math.min(Number(body.wait_seconds ?? 0), 120))
      const fanout = db.sendFanoutMessage({
        senderId,
        senderSessionId: body.caller_session_id ?? null,
        recipients: resolvedRecipients,
        body: body.message,
        clientMessageId: body.client_message_id,
        wakeRequired: body.wake !== false,
        waitSeconds,
      })

      for (const d of fanout.deliveries) {
        events.emit(`inbox:${d.recipient_id}`)
        if (d.wake_required === 1) {
          void dispatcher.dispatchPending(d.recipient_id)
        }
      }
      notifyUiStateChanged()

      if (waitSeconds > 0 && fanout.waitIds.length > 0) {
        const snap = await waitForFanoutRepliesInline({
          fanoutId: fanout.fanoutId,
          senderId,
          waitIds: fanout.waitIds,
          waitSeconds,
          waitFor: body.wait_for ?? 'all',
          abortSignal: req.signal,
        })
        if (!snap) {
          return Response.json({ error: 'Client disconnected during fan-out wait' }, { status: 499 })
        }
        notifyUiStateChanged()
        return Response.json(snap)
      }

      return Response.json(db.getFanoutStatus({ fanoutId: fanout.fanoutId, callerId: senderId, autoAck: false }))
    },

    'GET /api/messages/fanout-status': async (req, url, authRes) => {
      return handleFanoutStatus(req, url, authRes)
    },
    'POST /api/messages/fanout-status': async (req, url, authRes) => {
      return handleFanoutStatus(req, url, authRes)
    },

    'POST /api/messages/inbox': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        from?: string
        limit?: number
        after_seq?: number
        wait_seconds?: number
        auto_ack?: boolean
        ack_source?: string
      }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const callerId = callerCheck.callerId
      const waitSeconds = Math.max(0, Math.min(Number(body.wait_seconds ?? 0), 110))
      const deadline = Date.now() + waitSeconds * 1000

      let items = db.readInbox({
        recipientId: callerId,
        fromSenderId: body.from,
        limit: body.limit,
        afterSeq: body.after_seq,
        autoAck: Boolean(body.auto_ack),
        ackSource: body.ack_source ?? 'inbox_read',
      })

      while (items.length === 0 && Date.now() < deadline && !req.signal.aborted) {
        const rem = Math.max(50, deadline - Date.now())
        await new Promise<void>(resolve => {
          const t = setTimeout(() => {
            events.off(`inbox:${callerId}`, onInbox)
            resolve()
          }, Math.min(rem, 1000))
          const onInbox = () => {
            clearTimeout(t)
            resolve()
          }
          events.once(`inbox:${callerId}`, onInbox)
        })
        items = db.readInbox({
          recipientId: callerId,
          fromSenderId: body.from,
          limit: body.limit,
          afterSeq: body.after_seq,
          autoAck: Boolean(body.auto_ack),
          ackSource: body.ack_source ?? 'inbox_read',
        })
      }

      if (items.length > 0 && body.auto_ack) notifyUiStateChanged()

      return Response.json({
        ok: true,
        recipient: callerId,
        messages: items.map(({ message, delivery }) => ({
          seq: message.seq,
          message_id: message.id,
          from: message.sender_id,
          to: message.recipient_id,
          channel_id: message.channel_id,
          fanout_id: message.fanout_id,
          body: message.body,
          reply_to_id: message.reply_to_id,
          created_at: message.created_at,
          delivery_status: delivery.status,
          wake_required: delivery.wake_required === 1,
        })),
      })
    },

    'POST /api/messages/ack': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; ids: string[]; ack_source?: string }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const count = db.ackMessages(
        callerCheck.callerId,
        Array.isArray(body.ids) ? body.ids : [],
        body.ack_source ?? 'mcp_ack'
      )
      notifyUiStateChanged()
      return Response.json({ ok: true, acked_count: count })
    },

    'GET /api/messages/lookup': (_req, url) => {
      const rawId = url.searchParams.get('id')
      if (!rawId) return Response.json({ error: 'Missing id' }, { status: 400 })
      const msg = db.db.query('SELECT * FROM messages WHERE id = ?').get(rawId) as MessageRecord | null
      if (!msg) return Response.json({ error: `Unknown message_id: ${rawId}` }, { status: 404 })
      return Response.json({ ok: true, message: msg })
    },

    'POST /api/messages/edit': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; message_id: string; text: string }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const rev = db.addMessageRevision({
        editorId: callerCheck.callerId,
        messageId: body.message_id,
        newBody: body.text,
      })
      notifyUiStateChanged()
      return Response.json({ ok: true, ...rev })
    },

    'GET /api/channels': (_req, url) => {
      const includeArchived = url.searchParams.get('include_archived') === 'true'
      return Response.json({ ok: true, channels: db.listChannels(includeArchived) })
    },

    'POST /api/channels/create': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        channel: string
        description?: string
        wake_mode?: WakeMode
        members?: string[]
      }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const initialMembers: Array<{ agentId: string; wakeMode?: WakeMode }> = [
        {
          agentId: callerCheck.callerId,
          wakeMode: callerCheck.callerId === 'user' ? 'passive' : (body.wake_mode ?? 'mention'),
        },
      ]
      for (const m of body.members ?? []) {
        initialMembers.push({ agentId: m, wakeMode: 'mention' })
      }
      const chan = db.createChannel({
        id: body.channel,
        description: body.description,
        members: initialMembers,
      })
      notifyUiStateChanged()
      return Response.json({ ok: true, channel: chan })
    },

    'POST /api/channels/join': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; channel: string; agent_id?: string; wake_mode?: WakeMode }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const targetAgent = resolveChannelTargetAgent(authRes.role, callerCheck.callerId, body.agent_id)
      const joined = db.joinChannel(body.channel, targetAgent, body.wake_mode)
      notifyUiStateChanged()
      return Response.json({ ok: true, ...joined })
    },

    'POST /api/channels/leave': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; channel: string; agent_id?: string }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const targetAgent = resolveChannelTargetAgent(authRes.role, callerCheck.callerId, body.agent_id)
      const left = db.leaveChannel(body.channel, targetAgent)
      notifyUiStateChanged()
      return Response.json({ ok: true, removed: left })
    },

    'POST /api/channels/wake-mode': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; channel: string; agent_id?: string; wake_mode: WakeMode }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const targetAgent = resolveChannelTargetAgent(authRes.role, callerCheck.callerId, body.agent_id)
      db.setChannelWakeMode(body.channel, targetAgent, body.wake_mode)
      notifyUiStateChanged()
      return Response.json({ ok: true, channel: body.channel, agent_id: targetAgent, wake_mode: body.wake_mode })
    },

    'POST /api/channels/archive': async (req, _url, authRes) => {
      const body = await readJsonBody<{ caller_id?: string; channel: string; archived?: boolean }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      db.archiveChannel(body.channel, body.archived !== false)
      notifyUiStateChanged()
      return Response.json({ ok: true, channel: body.channel, archived: body.archived !== false })
    },

    'POST /api/channels/post': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        channel: string
        message: string
        reply_to_id?: string | null
        client_message_id?: string
      }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const senderId = callerCheck.callerId
      if (senderId === 'user' && authRes.role !== 'ui') {
        return Response.json({ error: "Agent token may not impersonate 'user'" }, { status: 403 })
      }

      const res = db.postChannelMessage({
        senderId,
        channelId: body.channel,
        body: body.message,
        replyToId: body.reply_to_id ?? null,
        clientMessageId: body.client_message_id,
        allowMentionAll: authRes.role === 'ui',
      })

      for (const d of res.deliveries) {
        events.emit(`inbox:${d.recipient_id}`)
        if (d.wake_required === 1) {
          void dispatcher.dispatchPending(d.recipient_id)
        }
      }
      notifyUiStateChanged()

      return Response.json({
        ok: true,
        message_id: res.message.id,
        channel: res.message.channel_id,
        mentions: res.mentions,
        deliveries: res.deliveries.map(d => ({
          recipient_id: d.recipient_id,
          wake_required: d.wake_required === 1,
          status: d.status,
        })),
      })
    },

    'POST /api/channels/read': async (req, _url, authRes) => {
      const body = await readJsonBody<{
        caller_id?: string
        channel: string
        limit?: number
        after_seq?: number
        before_seq?: number
        ack_returned?: boolean
      }>(req)
      const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
      if (!callerCheck.ok) {
        return Response.json({ error: callerCheck.error ?? 'Invalid caller identity' }, { status: 403 })
      }
      const messages = db.readChannelHistory({
        callerId: callerCheck.callerId,
        channelId: body.channel,
        limit: body.limit,
        afterSeq: body.after_seq,
        beforeSeq: body.before_seq,
        ackReturnedForCaller: body.ack_returned !== false,
      })
      return Response.json({ ok: true, channel: body.channel, messages })
    },

    'GET /api/ui/state': (_req, _url, authRes) => {
      if (authRes.role !== 'ui') {
        return Response.json({ error: 'UI token required for firehose view' }, { status: 403 })
      }
      const agents = buildAgentRosterSummary()
      const chans = db.listChannels(false)
      const recentMsgs = (
        db.db.query('SELECT * FROM messages ORDER BY seq DESC LIMIT 120').all() as MessageRecord[]
      ).reverse()
      const enrichedMsgs = recentMsgs.map(m => {
        const delivs = db.db
          .query('SELECT recipient_id, status, last_error FROM deliveries WHERE message_id = ?')
          .all(m.id)
        return { ...m, deliveries: delivs }
      })
      return Response.json({
        paused: dispatcher.paused,
        agents,
        channels: chans,
        messages: enrichedMsgs,
      })
    },

    'POST /api/ui/pause': async (req, _url, authRes) => {
      if (authRes.role !== 'ui') {
        return Response.json({ error: 'UI token required to toggle dispatcher pause state' }, { status: 403 })
      }
      const body = await readJsonBody<{ paused?: boolean }>(req)
      dispatcher.paused = Boolean(body.paused)
      notifyUiStateChanged()
      return Response.json({ ok: true, paused: dispatcher.paused })
    },
  }

  async function handleFanoutStatus(req: Request, url: URL, authRes: AuthResult): Promise<Response> {
    const body = req.method === 'POST' ? await readJsonBody<any>(req) : {}
    const fanoutId = String(url.searchParams.get('id') || body.fanout_id || '')
    if (!fanoutId) return Response.json({ error: 'Missing fanout_id' }, { status: 400 })
    const callerCheck = resolveCallerIdentity(req, body.caller_id, authRes.role)
    const callerId = callerCheck.ok ? callerCheck.callerId : undefined
    const waitSec = Math.max(0, Math.min(Number(body.wait_seconds ?? 0), 110))
    const autoAck = body.auto_ack !== false

    if (waitSec > 0) {
      const deadline = Date.now() + waitSec * 1000
      while (Date.now() < deadline && !req.signal.aborted) {
        const s = db.getFanoutStatus({ fanoutId, callerId, autoAck: false })
        if (s.status === 'complete') break
        const rem = Math.max(50, deadline - Date.now())
        await new Promise<void>(resolve => {
          const t = setTimeout(() => {
            events.off(`fanout:${fanoutId}`, onEv)
            resolve()
          }, Math.min(rem, 1000))
          const onEv = () => {
            clearTimeout(t)
            resolve()
          }
          events.once(`fanout:${fanoutId}`, onEv)
        })
      }
    }

    const res = db.getFanoutStatus({ fanoutId, callerId, autoAck })
    if (autoAck) notifyUiStateChanged()
    return Response.json(res)
  }

  const server = Bun.serve<WsClientData>({
    hostname,
    port,
    async fetch(req, srv) {
      const actualPort = srv.port!
      const url = new URL(req.url)
      const path = url.pathname

      const hostOriginCheck = validateLoopbackHostAndOrigin(req, actualPort)
      if (!hostOriginCheck.ok) {
        return Response.json({ error: hostOriginCheck.reason }, { status: 403 })
      }

      if (path === '/ws') {
        const upgraded = srv.upgrade(req, {
          data: { id: randomUUID(), role: 'unauthenticated' },
        })
        if (upgraded) return undefined
        return Response.json({ error: 'WebSocket upgrade failed' }, { status: 400 })
      }

      if (path === '/api/health' && req.method === 'GET') {
        return Response.json({
          ok: true,
          product: 'turnbus',
          version: PKG_VERSION,
          instanceId,
          port: actualPort,
        })
      }

      const contentLength = Number(req.headers.get('content-length') ?? 0)
      if (contentLength > 512 * 1024) {
        return Response.json({ error: 'Payload Too Large (max 512 KiB)' }, { status: 413 })
      }

      if (path === '/' && req.method === 'GET') {
        const uiHtmlPath = join(__dirname, 'ui.html')
        const rawHtml = existsSync(uiHtmlPath) ? readFileSync(uiHtmlPath, 'utf-8') : '<h1>turnbus</h1>'
        return new Response(rawHtml, {
          headers: {
            'Content-Type': 'text/html; charset=utf-8',
            'Cache-Control': 'no-store',
          },
        })
      }

      const authRes = authenticateRequest(req, auth, actualPort)
      if (!authRes.ok) {
        return Response.json({ error: authRes.reason ?? 'Unauthorized' }, { status: 401 })
      }

      try {
        const handler = routes[`${req.method} ${path}`]
        if (handler) {
          return await handler(req, url, authRes)
        }
        return Response.json({ error: `Not found: ${path}` }, { status: 404 })
      } catch (err: any) {
        const status = typeof err?.status === 'number' ? err.status : 400
        return Response.json({ error: err?.message ?? String(err) }, { status })
      }
    },

    websocket: {
      maxPayloadLength: 1024 * 1024,
      idleTimeout: 120,
      open(ws) {
        if (unauthSockets.size >= 32) {
          ws.close(1008, 'Too many unauthenticated sockets')
          return
        }
        unauthSockets.add(ws)
        ws.data.authTimer = setTimeout(() => {
          if (ws.data.role === 'unauthenticated') {
            try {
              ws.close(1008, 'Authentication timeout')
            } catch {}
          }
        }, 5000)
      },
      message(ws, raw) {
        let msg: any
        try {
          msg = JSON.parse(typeof raw === 'string' ? raw : Buffer.from(raw).toString('utf-8'))
        } catch {
          ws.close(1008, 'Malformed JSON')
          return
        }

        if (msg.type === 'auth_ui') {
          if (!safeTokenEquals(msg.token, auth.uiToken)) {
            ws.close(1008, 'Invalid UI token')
            return
          }
          if (ws.data.authTimer) clearTimeout(ws.data.authTimer)
          unauthSockets.delete(ws)
          ws.data.role = 'ui'
          uiSockets.add(ws)
          ws.send(JSON.stringify({ type: 'auth_ok', role: 'ui' }))
          return
        }

        if (msg.type === 'auth_adapter') {
          if (!safeTokenEquals(msg.token, auth.agentToken)) {
            ws.close(1008, 'Invalid agent token')
            return
          }
          let agentId = String(msg.agentId ?? '').trim().toLowerCase()
          const isBound = agentId ? hasActiveBoundSession(agentId) : false
          if (msg.sessionToken && typeof msg.sessionToken === 'string') {
            const bound = verifySessionToken(msg.sessionToken)
            if (!bound || (agentId && agentId !== bound.agentId)) {
              ws.close(1008, 'Invalid adapter sessionToken')
              return
            }
            agentId = bound.agentId
          } else if (isBound) {
            ws.close(1008, `Session token required for bound agent '${agentId}'`)
            return
          }
          if (!agentId || agentId === 'user') {
            ws.close(1008, 'Invalid adapter agentId')
            return
          }
          if (ws.data.authTimer) clearTimeout(ws.data.authTimer)
          unauthSockets.delete(ws)
          let wsChannelCapable = Boolean(msg.channelCapable)
          const currentSess = db.getCurrentSession(agentId)
          if (currentSess?.pid) {
            const liveCmdline = readProcCmdline(currentSess.pid)
            if (liveCmdline !== null) {
              wsChannelCapable = hasClaudeChannelServerFlag(liveCmdline, 'turnbus')
            }
          }
          ws.data.role = 'adapter'
          ws.data.agentId = agentId
          ws.data.sessionId = msg.sessionId ? String(msg.sessionId) : undefined
          ws.data.connectionEpoch = msg.connectionEpoch ? String(msg.connectionEpoch) : undefined
          ws.data.channelCapable = wsChannelCapable

          let sockSet = adapterSockets.get(agentId)
          if (!sockSet) {
            sockSet = new Set()
            adapterSockets.set(agentId, sockSet)
          }
          sockSet.add(ws)
          ws.send(JSON.stringify({ type: 'auth_ok', role: 'adapter', agentId }))
          notifyUiStateChanged()
          void dispatcher.dispatchPending(agentId)
          return
        }

        if (ws.data.role === 'adapter' && msg.type === 'claude_channel_ack' && typeof msg.requestId === 'string') {
          const pending = pendingWsPushAcks.get(msg.requestId)
          if (pending && pending.ws === ws) {
            clearTimeout(pending.timer)
            pendingWsPushAcks.delete(msg.requestId)
            pending.resolve({
              sent: true,
              ackedByAdapter: Boolean(msg.ok),
              error: msg.error ? String(msg.error) : undefined,
            })
          }
          return
        }
      },
      close(ws) {
        if (ws.data.authTimer) clearTimeout(ws.data.authTimer)
        unauthSockets.delete(ws)
        if (ws.data.role === 'ui') {
          uiSockets.delete(ws)
        } else if (ws.data.role === 'adapter' && ws.data.agentId) {
          const sockSet = adapterSockets.get(ws.data.agentId)
          if (sockSet) {
            sockSet.delete(ws)
            if (sockSet.size === 0) adapterSockets.delete(ws.data.agentId)
            notifyUiStateChanged()
          }
        }
      },
    },
  })

  return {
    port: server.port!,
    instanceId,
    db,
    roster,
    dispatcher,
    auth,
    async stop() {
      if (reconcileTimer) clearInterval(reconcileTimer)
      for (const [, p] of pendingWsPushAcks) clearTimeout(p.timer)
      pendingWsPushAcks.clear()
      server.stop(true)
      db.close()
    },
  }
}

if (import.meta.main) {
  const stateDir = ensureStateDir(getDefaultStateDir())
  const lockPath = join(stateDir, 'daemon.lock')
  if (existsSync(lockPath)) {
    try {
      const oldPid = Number(readFileSync(lockPath, 'utf-8').trim())
      if (oldPid > 1 && oldPid !== process.pid) {
        process.kill(oldPid, 0)
        console.error(`[turnbus] daemon already running (pid=${oldPid})`)
        process.exit(0)
      }
    } catch {
      try {
        unlinkSync(lockPath)
      } catch {}
    }
  }
  writeFileSync(lockPath, `${process.pid}\n`, { mode: 0o600 })

  const daemon = startTurnBusDaemon()
  console.error(`[turnbus] daemon listening on http://127.0.0.1:${daemon.port} (instance=${daemon.instanceId})`)

  const cleanup = async () => {
    try {
      unlinkSync(lockPath)
    } catch {}
    await daemon.stop()
    process.exit(0)
  }
  process.on('SIGINT', cleanup)
  process.on('SIGTERM', cleanup)
}
