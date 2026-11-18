import { ClaimedDelivery, TurnBusDB } from './db.ts'
import { canonicalizePath, isSameOrSubpath, RosterManager } from './roster.ts'
import { AgyCommandRunner, deliverToAgyConversation } from './adapters/agy.ts'
import { ClaudeAdapterSender, deliverToClaudeChannel } from './adapters/claude.ts'
import { CodexRpcCaller, deliverToCodexThread, discoverLoadedCodexThreads } from './adapters/codex.ts'

export interface PushDispatcherDeps {
  db: TurnBusDB
  roster: RosterManager
  sendViaClaudeAdapterWs: ClaudeAdapterSender
  codexRpc?: CodexRpcCaller
  agyRunner?: AgyCommandRunner
  maxWakesPerMinute?: number
  maxAgentReplyDepth?: number
  onDeliveryStateChange?: (deliveryId: number, messageId: string, recipientId: string, status: string) => void
}

export class PushDispatcher {
  private readonly deps: PushDispatcherDeps
  private readonly activeRecipientLocks = new Set<string>()
  private readonly recipientWakeHistory = new Map<string, number[]>()
  private lastCodexMcpReloadAt = 0
  public paused = false

  constructor(deps: PushDispatcherDeps) {
    this.deps = deps
  }

  private exceedsAgentReplyChainDepth(messageId: string): boolean {
    const maxDepth = this.deps.maxAgentReplyDepth ?? 10
    let currentId: string | null = messageId
    let depth = 0
    const seen = new Set<string>()
    while (currentId && !seen.has(currentId)) {
      seen.add(currentId)
      const row = this.deps.db.db
        .query(`SELECT sender_id, reply_to_id FROM messages WHERE id = ? LIMIT 1`)
        .get(currentId) as { sender_id: string; reply_to_id: string | null } | null
      if (!row || row.sender_id === 'user') return false
      depth++
      if (depth > maxDepth) return true
      currentId = row.reply_to_id
    }
    return false
  }

  private checkAndRecordWakeBudget(recipientId: string): boolean {
    const maxWakes = this.deps.maxWakesPerMinute ?? 15
    const now = Date.now()
    const cutoff = now - 60_000
    const prev = (this.recipientWakeHistory.get(recipientId) ?? []).filter(ts => ts > cutoff)
    if (prev.length >= maxWakes) {
      this.recipientWakeHistory.set(recipientId, prev)
      return false
    }
    prev.push(now)
    this.recipientWakeHistory.set(recipientId, prev)
    return true
  }

  async reconcileCodexBindings(): Promise<void> {
    let threads = []
    try {
      threads = await discoverLoadedCodexThreads({ rpc: this.deps.codexRpc })
    } catch {
      return
    }

    if (!this.deps.codexRpc && threads.length > 0 && Date.now() - this.lastCodexMcpReloadAt > 60_000) {
      try {
        const { codexControlRpc } = await import('./adapters/codex.ts')
        let needsReload = false
        for (const t of threads) {
          const st = await codexControlRpc('mcpServerStatus/list', { threadId: t.threadId }, { timeoutMs: 2500 })
          const servers = Array.isArray(st?.result?.data) ? st.result.data : []
          const busEntry = servers.find((s: any) => s?.name === 'turnbus')
          if (busEntry && busEntry.runtimeStatus === null) {
            needsReload = true
            break
          }
        }
        if (needsReload) {
          this.lastCodexMcpReloadAt = Date.now()
          await codexControlRpc('config/mcpServer/reload', {}, { timeoutMs: 5000 })
        }
      } catch {}
    }

    const codexAgents = this.deps.db.listAgents().filter(a => a.tool === 'codex')
    for (const agent of codexAgents) {
      try {
        const currentSess = this.deps.db.getCurrentSession(agent.id)
        if (currentSess) {
          const matchedLoaded = threads.find(t => t.threadId === currentSess.native_session_id)
          if (matchedLoaded) {
            this.deps.db.bindAgentSession({
              agentId: agent.id,
              endpointKey: matchedLoaded.endpointKey,
              nativeSessionId: matchedLoaded.threadId,
              workspace: matchedLoaded.cwd || agent.workspace,
              nativeStatus: matchedLoaded.statusType,
              capabilities: {
                push: 'codex_control',
                activeFlags: matchedLoaded.activeFlags,
              },
            })
            continue
          }
        }

        if (agent.workspace) {
          const canonWs = canonicalizePath(agent.workspace)
          const matchingThreads = threads.filter(t => {
            const canonThreadCwd = canonicalizePath(t.cwd)
            if (!canonWs || !canonThreadCwd || !isSameOrSubpath(canonThreadCwd, canonWs).match) {
              return false
            }
            const existingOwner = this.deps.db.db
              .query(`SELECT agent_id FROM agent_sessions WHERE native_session_id = ? LIMIT 1`)
              .get(t.threadId) as { agent_id: string } | null
            return !existingOwner || existingOwner.agent_id === agent.id
          })
          if (matchingThreads.length === 1) {
            const t = matchingThreads[0]
            this.deps.db.bindAgentSession({
              agentId: agent.id,
              endpointKey: t.endpointKey,
              nativeSessionId: t.threadId,
              workspace: t.cwd || agent.workspace,
              nativeStatus: t.statusType,
              capabilities: {
                push: 'codex_control',
                activeFlags: t.activeFlags,
              },
            })
          }
        }
      } catch {}
    }
  }

  async dispatchPending(recipientId?: string): Promise<number> {
    const targets = recipientId
      ? [recipientId.toLowerCase()]
      : this.deps.db
          .listAgents()
          .filter(a => a.tool !== 'user')
          .map(a => a.id)

    let processedCount = 0
    await Promise.all(
      targets.map(async target => {
        if (this.activeRecipientLocks.has(target)) return
        this.activeRecipientLocks.add(target)
        try {
          for (let step = 0; step < 5; step++) {
            const claimed = this.deps.db.claimDeliveriesForDispatch({
              recipientId: target,
              limit: 1,
              route: 'native_push',
            })
            if (claimed.length === 0) break
            await this.dispatchOne(claimed[0])
            processedCount++
          }
        } finally {
          this.activeRecipientLocks.delete(target)
        }
      })
    )
    return processedCount
  }

  private async dispatchOne(item: ClaimedDelivery): Promise<void> {
    if (this.paused) {
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: 'definitely_not_accepted',
        stage: 'dispatcher_paused',
        error: 'Push dispatcher is paused; message held in inbox',
        retryDelayMs: 15_000,
      })
      return
    }

    const agent = this.deps.db.getAgent(item.delivery.recipient_id)
    if (!agent || agent.tool === 'user') {
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: 'definitely_not_accepted',
        stage: 'no_push_for_user',
        retryDelayMs: 60_000,
      })
      return
    }

    if (this.exceedsAgentReplyChainDepth(item.message.id)) {
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: 'definitely_not_accepted',
        stage: 'agent_chain_depth_exceeded',
        error: `Agent-only reply chain depth exceeded max (${this.deps.maxAgentReplyDepth ?? 10}); held in inbox`,
        retryDelayMs: 60_000,
      })
      return
    }

    if (!this.checkAndRecordWakeBudget(agent.id)) {
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: 'definitely_not_accepted',
        stage: 'wake_rate_limited',
        error: `Wake rate limit exceeded for '${agent.id}' (>${this.deps.maxWakesPerMinute ?? 15} wakes/min); held in inbox`,
        retryDelayMs: 30_000,
      })
      return
    }

    if (agent.tool === 'claude') {
      const res = await deliverToClaudeChannel({
        recipientId: agent.id,
        message: item.message,
        sendViaAdapterWs: this.deps.sendViaClaudeAdapterWs,
      })
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: res.outcome,
        stage: res.stage,
        nativeReceipt: res.receipt,
        error: res.error ?? null,
        retryDelayMs: res.outcome === 'definitely_not_accepted' ? 15_000 : null,
      })
      this.deps.onDeliveryStateChange?.(
        item.delivery.id,
        item.message.id,
        agent.id,
        res.outcome === 'accepted' ? 'pushed' : res.outcome === 'unknown' ? 'unknown' : 'queued'
      )
      return
    }

    if (agent.tool === 'codex') {
      const latestSession = this.deps.db.getCurrentSession(agent.id)
      if (
        item.session &&
        (!latestSession ||
          latestSession.id !== item.session.id ||
          latestSession.native_session_id !== item.session.native_session_id ||
          latestSession.generation !== item.session.generation)
      ) {
        this.deps.db.completeDeliveryAttempt({
          deliveryId: item.delivery.id,
          claimToken: item.claimToken,
          outcome: 'definitely_not_accepted',
          stage: 'session_superseded_before_send',
          error: `Codex agent '${agent.id}' rebound to a newer session before send started`,
          retryDelayMs: 0,
        })
        return
      }

      let session = latestSession ?? item.session
      if (!session) {
        await this.reconcileCodexBindings()
        session = this.deps.db.getCurrentSession(agent.id)
      }
      if (!session || !session.native_session_id) {
        this.deps.db.completeDeliveryAttempt({
          deliveryId: item.delivery.id,
          claimToken: item.claimToken,
          outcome: 'definitely_not_accepted',
          stage: 'codex_unbound',
          error: `Codex agent '${agent.id}' has no bound loaded thread; message held in durable inbox`,
          retryDelayMs: 15_000,
        })
        return
      }

      const res = await deliverToCodexThread({
        threadId: session.native_session_id,
        message: item.message,
        nativeClientMessageId: item.nativeClientMessageId,
        rpc: this.deps.codexRpc,
        onStageCheckpoint: (stage, receipt) => {
          this.deps.db.checkpointAttemptStage({
            claimToken: item.claimToken,
            stage,
            nativeReceipt: receipt,
          })
        },
      })

      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: res.outcome,
        stage: res.stage,
        nativeReceipt: res.receipt,
        error: res.error ?? null,
        retryDelayMs: res.outcome === 'definitely_not_accepted' ? 15_000 : null,
      })
      this.deps.onDeliveryStateChange?.(
        item.delivery.id,
        item.message.id,
        agent.id,
        res.outcome === 'accepted' ? 'pushed' : res.outcome === 'unknown' ? 'unknown' : 'queued'
      )
      return
    }

    if (agent.tool === 'agy') {
      const currentSession = this.deps.db.getCurrentSession(agent.id)
      const creds = this.deps.roster.getPrivateAgyCredentials(agent.id)
      if (
        currentSession?.native_session_id &&
        creds?.conversationId &&
        creds.conversationId !== currentSession.native_session_id
      ) {
        this.deps.db.completeDeliveryAttempt({
          deliveryId: item.delivery.id,
          claimToken: item.claimToken,
          outcome: 'definitely_not_accepted',
          stage: 'agy_credential_session_mismatch',
          error: `Antigravity credentials conversationId (${creds.conversationId}) does not match bound session (${currentSession.native_session_id})`,
          retryDelayMs: 15_000,
        })
        return
      }
      const res = await deliverToAgyConversation({
        creds,
        message: item.message,
        runner: this.deps.agyRunner,
      })
      this.deps.db.completeDeliveryAttempt({
        deliveryId: item.delivery.id,
        claimToken: item.claimToken,
        outcome: res.outcome,
        stage: res.stage,
        nativeReceipt: res.receipt,
        error: res.error ?? null,
        retryDelayMs: res.outcome === 'definitely_not_accepted' ? 15_000 : null,
      })
      this.deps.onDeliveryStateChange?.(
        item.delivery.id,
        item.message.id,
        agent.id,
        res.outcome === 'accepted' ? 'pushed' : res.outcome === 'unknown' ? 'unknown' : 'queued'
      )
    }
  }
}
