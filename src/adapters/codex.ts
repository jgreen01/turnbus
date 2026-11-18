import { existsSync } from 'node:fs'
import { createConnection } from 'node:net'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { AttemptOutcome, MessageRecord } from '../db.ts'
import { formatPeerEnvelopeText } from './claude.ts'

export interface CodexRpcOptions {
  socketPath?: string
  timeoutMs?: number
}

export interface DiscoveredCodexThread {
  threadId: string
  cwd: string
  statusType: string
  activeFlags: string[]
  name: string | null
  endpointKey: string
}

export interface CodexDispatchResult {
  outcome: Exclude<AttemptOutcome, 'in_progress'>
  stage: string
  receipt?: Record<string, unknown>
  error?: string
}

export function getDefaultCodexSocketPath(): string {
  return process.env.CODEX_CONTROL_SOCK ?? join(homedir(), '.codex', 'app-server-control', 'app-server-control.sock')
}

export async function codexControlRpc(
  method: string,
  params: unknown,
  options: CodexRpcOptions = {}
): Promise<{ result: any; requestSent: boolean }> {
  const socketPath = options.socketPath ?? getDefaultCodexSocketPath()
  const timeoutMs = options.timeoutMs ?? 6000

  if (!existsSync(socketPath)) {
    const err = new Error(`Codex control socket not found at ${socketPath}`) as Error & { requestSent?: boolean }
    err.requestSent = false
    throw err
  }

  return new Promise((resolve, reject) => {
    let requestSent = false
    let settled = false
    let buffer = ''
    const reqId = `turnbus-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

    const socket = createConnection(socketPath)
    const timer = setTimeout(() => {
      if (settled) return
      settled = true
      socket.destroy()
      const err = new Error(`Codex RPC '${method}' timed out after ${timeoutMs}ms`) as Error & { requestSent?: boolean }
      err.requestSent = requestSent
      reject(err)
    }, timeoutMs)

    socket.on('connect', () => {
      const payload = JSON.stringify({ jsonrpc: '2.0', id: reqId, method, params }) + '\n'
      socket.write(payload, err => {
        if (!err) requestSent = true
      })
    })

    socket.on('data', chunk => {
      buffer += chunk.toString('utf-8')
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed) continue
        try {
          const msg = JSON.parse(trimmed)
          if (msg.id === reqId) {
            settled = true
            clearTimeout(timer)
            socket.end()
            if (msg.error) {
              const err = new Error(`Codex ${method}: ${msg.error.message ?? JSON.stringify(msg.error)}`) as Error & {
                requestSent?: boolean
                rpcErrorCode?: number
              }
              err.requestSent = true
              err.rpcErrorCode = msg.error.code
              reject(err)
            } else {
              resolve({ result: msg.result, requestSent: true })
            }
            return
          }
        } catch (parseErr: any) {
          settled = true
          clearTimeout(timer)
          socket.destroy()
          const err = new Error(`Malformed JSON from Codex control socket: ${parseErr?.message}`) as Error & {
            requestSent?: boolean
          }
          err.requestSent = requestSent
          reject(err)
          return
        }
      }
    })

    socket.on('error', (err: any) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      err.requestSent = requestSent
      reject(err)
    })

    socket.on('close', () => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      const err = new Error(`Codex control socket closed before responding to '${method}'`) as Error & {
        requestSent?: boolean
      }
      err.requestSent = requestSent
      reject(err)
    })
  })
}

export type CodexRpcCaller = (
  method: string,
  params: unknown,
  options?: CodexRpcOptions
) => Promise<{ result: any; requestSent: boolean }>

export async function discoverLoadedCodexThreads(
  options: {
    socketPath?: string
    rpc?: CodexRpcCaller
    endpointKey?: string
  } = {}
): Promise<DiscoveredCodexThread[]> {
  const rpc = options.rpc ?? codexControlRpc
  const endpointKey = options.endpointKey ?? `codex:${options.socketPath ?? getDefaultCodexSocketPath()}`
  const loadedIds: string[] = []

  let cursor: string | null = null
  for (let page = 0; page < 10; page++) {
    const { result } = await rpc('thread/loaded/list', cursor ? { cursor } : {}, {
      socketPath: options.socketPath,
      timeoutMs: 4000,
    })
    const ids = Array.isArray(result?.data) ? result.data : []
    for (const id of ids) {
      if (typeof id === 'string') loadedIds.push(id)
    }
    if (!result?.nextCursor) break
    cursor = String(result.nextCursor)
  }

  const threads: DiscoveredCodexThread[] = []
  for (const threadId of loadedIds) {
    try {
      const { result } = await rpc('thread/read', { threadId }, { socketPath: options.socketPath, timeoutMs: 4000 })
      const t = result?.thread
      if (t && typeof t.id === 'string') {
        threads.push({
          threadId: t.id,
          cwd: typeof t.cwd === 'string' ? t.cwd : '',
          statusType: String(t.status?.type ?? 'unknown'),
          activeFlags: Array.isArray(t.status?.activeFlags) ? t.status.activeFlags.map(String) : [],
          name: typeof t.name === 'string' ? t.name : null,
          endpointKey,
        })
      }
    } catch {}
  }
  return threads
}

export function formatCodexEnvelope(message: MessageRecord): string {
  return formatPeerEnvelopeText({
    message,
    recipientId: message.recipient_id ?? 'codex',
  })
}

function findInProgressTurnId(threadObj: any): string | null {
  const turns = Array.isArray(threadObj?.turns) ? threadObj.turns : []
  for (let i = turns.length - 1; i >= 0; i--) {
    const turn = turns[i]
    if (turn && (turn.status === 'inProgress' || turn.status?.type === 'inProgress') && typeof turn.id === 'string') {
      return turn.id
    }
  }
  return null
}

function extractQueuedSubmissionId(addResult: any): string | null {
  if (typeof addResult?.queuedSubmission?.id === 'string' && addResult.queuedSubmission.id.length > 0) {
    return addResult.queuedSubmission.id
  }
  if (typeof addResult?.queuedSubmissionId === 'string' && addResult.queuedSubmissionId.length > 0) {
    return addResult.queuedSubmissionId
  }
  return null
}

function isAmbiguousPostSendError(err: any): boolean {
  return Boolean(err?.requestSent) && typeof err?.rpcErrorCode !== 'number'
}

export async function deliverToCodexThread(params: {
  threadId: string
  message: MessageRecord
  nativeClientMessageId: string
  rpc?: CodexRpcCaller
  socketPath?: string
  onStageCheckpoint?: (stage: string, receipt?: Record<string, unknown>) => void
}): Promise<CodexDispatchResult> {
  const rpc = params.rpc ?? codexControlRpc
  const envelopeText = formatCodexEnvelope(params.message)
  const inputItems = [{ type: 'text', text: envelopeText }]

  let thread: any
  try {
    const { result } = await rpc('thread/read', { threadId: params.threadId }, { socketPath: params.socketPath })
    thread = result?.thread
  } catch (err: any) {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'thread_read_failed',
      error: err?.message ?? String(err),
    }
  }

  if (!thread || typeof thread.id !== 'string') {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'thread_not_found',
      error: `Codex thread ${params.threadId} not found in loaded threads`,
    }
  }

  const statusType = String(thread.status?.type ?? 'unknown')
  const activeFlags: string[] = Array.isArray(thread.status?.activeFlags) ? thread.status.activeFlags.map(String) : []

  if (statusType === 'notLoaded') {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'thread_not_loaded',
      error: `Codex thread ${params.threadId} is notLoaded`,
    }
  }

  if (activeFlags.includes('waitingOnApproval') || activeFlags.includes('waitingOnUserInput')) {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'blocked_on_approval_or_input',
      error: `Codex thread ${params.threadId} is blocked on ${activeFlags.join(',')}`,
    }
  }

  if (statusType === 'active') {
    const expectedTurnId = findInProgressTurnId(thread)
    if (expectedTurnId) {
      params.onStageCheckpoint?.('steer_sending', { expectedTurnId, clientUserMessageId: params.nativeClientMessageId })
      try {
        const { result: steerRes } = await rpc(
          'turn/steer',
          {
            threadId: params.threadId,
            expectedTurnId,
            clientUserMessageId: params.nativeClientMessageId,
            input: inputItems,
          },
          { socketPath: params.socketPath }
        )
        return {
          outcome: 'accepted',
          stage: 'steer_accepted',
          receipt: {
            route: 'turn/steer',
            threadId: params.threadId,
            expectedTurnId,
            clientUserMessageId: params.nativeClientMessageId,
            turnId: steerRes?.turnId ?? expectedTurnId,
          },
        }
      } catch (err: any) {
        const msg = String(err?.message ?? err)
        const isPreconditionEnded =
          /no active turn|expectedTurnId|turn.*ended|not.*inProgress|precondition/i.test(msg)
        if (!isPreconditionEnded) {
          if (isAmbiguousPostSendError(err)) {
            return { outcome: 'unknown', stage: 'steer_ambiguous', error: msg }
          }
          return { outcome: 'definitely_not_accepted', stage: 'steer_rejected', error: msg }
        }
        try {
          const reread = await rpc('thread/read', { threadId: params.threadId }, { socketPath: params.socketPath })
          thread = reread.result?.thread
          if (!thread || typeof thread.id !== 'string') {
            return {
              outcome: 'definitely_not_accepted',
              stage: 'thread_not_found_after_steer',
              error: `Codex thread ${params.threadId} unavailable after turn/steer precondition check`,
            }
          }
          const rereadStatus = String(thread.status?.type ?? 'unknown')
          const rereadFlags: string[] = Array.isArray(thread.status?.activeFlags)
            ? thread.status.activeFlags.map(String)
            : []
          if (rereadStatus === 'notLoaded') {
            return {
              outcome: 'definitely_not_accepted',
              stage: 'thread_not_loaded_after_steer',
              error: `Codex thread ${params.threadId} became notLoaded`,
            }
          }
          if (rereadFlags.includes('waitingOnApproval') || rereadFlags.includes('waitingOnUserInput')) {
            return {
              outcome: 'definitely_not_accepted',
              stage: 'blocked_on_approval_or_input',
              error: `Codex thread ${params.threadId} is blocked on ${rereadFlags.join(',')}`,
            }
          }
        } catch (rereadErr: any) {
          return {
            outcome: 'definitely_not_accepted',
            stage: 'thread_reread_failed',
            error: rereadErr?.message ?? String(rereadErr),
          }
        }
      }
    }
  }

  params.onStageCheckpoint?.('queue_adding', { clientUserMessageId: params.nativeClientMessageId })
  let queuedSubmissionId: string | null = null
  try {
    const { result: addRes } = await rpc(
      'thread/queue/add',
      {
        threadId: params.threadId,
        clientUserMessageId: params.nativeClientMessageId,
        turnParams: { input: inputItems },
      },
      { socketPath: params.socketPath }
    )
    queuedSubmissionId = extractQueuedSubmissionId(addRes)
    params.onStageCheckpoint?.('queue_accepted', {
      clientUserMessageId: params.nativeClientMessageId,
      queuedSubmissionId,
    })
  } catch (err: any) {
    const msg = String(err?.message ?? err)
    if (isAmbiguousPostSendError(err)) {
      return { outcome: 'unknown', stage: 'queue_add_ambiguous', error: msg }
    }
    return { outcome: 'definitely_not_accepted', stage: 'queue_add_rejected', error: msg }
  }

  if (!queuedSubmissionId) {
    return {
      outcome: 'unknown',
      stage: 'queue_accepted_missing_submission_id',
      receipt: {
        route: 'thread/queue/add',
        threadId: params.threadId,
        clientUserMessageId: params.nativeClientMessageId,
      },
      error: 'thread/queue/add succeeded without a valid queuedSubmission.id; refusing blind thread/queue/start',
    }
  }

  params.onStageCheckpoint?.('queue_starting', {
    clientUserMessageId: params.nativeClientMessageId,
    queuedSubmissionId,
  })
  try {
    const { result: startRes } = await rpc(
      'thread/queue/start',
      {
        threadId: params.threadId,
        queuedSubmissionId,
      },
      { socketPath: params.socketPath }
    )
    return {
      outcome: 'accepted',
      stage: 'queue_started',
      receipt: {
        route: 'thread/queue/add+start',
        threadId: params.threadId,
        clientUserMessageId: params.nativeClientMessageId,
        queuedSubmissionId,
        startedTurnId: startRes?.turn?.id ?? startRes?.turnId ?? null,
      },
    }
  } catch (err: any) {
    const msg = String(err?.message ?? err)
    if (/queue is empty|queued submission not found/i.test(msg)) {
      return {
        outcome: 'accepted',
        stage: 'queue_auto_started',
        receipt: {
          route: 'thread/queue/add (auto-started)',
          threadId: params.threadId,
          clientUserMessageId: params.nativeClientMessageId,
          queuedSubmissionId,
        },
      }
    }
    return {
      outcome: 'unknown',
      stage: 'queue_accepted_start_unconfirmed',
      receipt: {
        route: 'thread/queue/add',
        threadId: params.threadId,
        clientUserMessageId: params.nativeClientMessageId,
        queuedSubmissionId,
      },
      error: `Item already queued (queuedSubmissionId=${queuedSubmissionId}), but thread/queue/start failed: ${msg}`,
    }
  }
}
