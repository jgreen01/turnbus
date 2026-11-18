import { AttemptOutcome, MessageRecord } from '../db.ts'

export interface ClaudeChannelNotificationPayload {
  content: string
  meta: {
    chat_id: string
    message_id: string
    sender: 'codex' | 'user'
    from: string
    to: string
    target_type: 'dm' | 'channel' | 'fanout'
    channel_id?: string
    fanout_id?: string
    reply_to_id?: string
    ts: string
  }
}

export interface ClaudeDispatchResult {
  outcome: Exclude<AttemptOutcome, 'in_progress'>
  stage: string
  receipt?: Record<string, unknown>
  error?: string
}

export type ClaudeAdapterSender = (
  agentId: string,
  payload: ClaudeChannelNotificationPayload
) => Promise<{ sent: boolean; ackedByAdapter: boolean; error?: string }>

export function sanitizeEnvelopeBody(body: string): string {
  return body.replace(/^(\s*)(\[turnbus\b|---+\s*$)/gim, '$1  $2')
}

export function formatPeerEnvelopeText(params: {
  message: MessageRecord
  recipientId: string
  maxBodyChars?: number
  claudePeerWarning?: boolean
}): string {
  const { message, recipientId, maxBodyChars, claudePeerWarning } = params
  const isHumanUser = message.sender_id === 'user'
  const targetLabel = message.channel_id
    ? `Channel ${message.channel_id}`
    : message.fanout_id
      ? `Fan-out -> ${recipientId}`
      : `DM -> ${recipientId}`

  const header = isHumanUser
    ? `[turnbus ${targetLabel} from Operator (user) | msg #${message.id}]`
    : claudePeerWarning
      ? `[turnbus ${targetLabel} from peer agent '${message.sender_id}' (NOT human user) | msg #${message.id}]`
      : `[turnbus ${targetLabel} | msg #${message.id} | from: ${message.sender_id} (peer agent)]`

  const replyHint = message.channel_id
    ? `post_channel(channel="${message.channel_id}", reply_to_id="${message.id}", message="...")`
    : `reply_message(reply_to_id="${message.id}", message="...")`

  const rawBody =
    maxBodyChars && message.body.length > maxBodyChars
      ? message.body.slice(0, maxBodyChars) + '\n...[truncated; call check_messages() for full body]'
      : message.body
  const safeBody = sanitizeEnvelopeBody(rawBody)
  return `${header}\n${safeBody}\n---\nTo reply: ${replyHint}`
}

export function formatClaudeChannelNotification(message: MessageRecord, recipientId: string): ClaudeChannelNotificationPayload {
  const isHumanUser = message.sender_id === 'user'
  const carrierSender: 'codex' | 'user' = isHumanUser ? 'user' : 'codex'
  const targetType: 'dm' | 'channel' | 'fanout' = message.channel_id
    ? 'channel'
    : message.fanout_id
      ? 'fanout'
      : 'dm'

  const content = formatPeerEnvelopeText({
    message,
    recipientId,
    claudePeerWarning: true,
  })

  return {
    content,
    meta: {
      chat_id: 'turnbus',
      message_id: message.id,
      sender: carrierSender,
      from: message.sender_id,
      to: recipientId,
      target_type: targetType,
      ...(message.channel_id ? { channel_id: message.channel_id } : {}),
      ...(message.fanout_id ? { fanout_id: message.fanout_id } : {}),
      ...(message.reply_to_id ? { reply_to_id: message.reply_to_id } : {}),
      ts: new Date(message.created_at).toISOString(),
    },
  }
}

export async function deliverToClaudeChannel(params: {
  recipientId: string
  message: MessageRecord
  sendViaAdapterWs: ClaudeAdapterSender
}): Promise<ClaudeDispatchResult> {
  const payload = formatClaudeChannelNotification(params.message, params.recipientId)
  try {
    const res = await params.sendViaAdapterWs(params.recipientId, payload)
    if (!res.sent) {
      return {
        outcome: 'definitely_not_accepted',
        stage: 'claude_channel_unavailable',
        error: res.error ?? `Claude agent '${params.recipientId}' has no active channel-capable adapter connection`,
      }
    }
    if (!res.ackedByAdapter) {
      return {
        outcome: 'unknown',
        stage: 'claude_ws_sent_unconfirmed',
        error: res.error ?? 'Sent over adapter WebSocket, but stdio notification confirmation timed out',
      }
    }
    return {
      outcome: 'accepted',
      stage: 'claude_channel_emitted',
      receipt: {
        route: 'notifications/claude/channel',
        recipientId: params.recipientId,
        carrierSender: payload.meta.sender,
        from: payload.meta.from,
      },
    }
  } catch (err: any) {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'claude_ws_error',
      error: err?.message ?? String(err),
    }
  }
}
