import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { redactSecrets } from '../auth.ts'
import { AttemptOutcome, MessageRecord } from '../db.ts'
import { PrivateAgyCredentials } from '../roster.ts'
import { formatPeerEnvelopeText } from './claude.ts'

export interface AgyDispatchResult {
  outcome: Exclude<AttemptOutcome, 'in_progress'>
  stage: string
  receipt?: Record<string, unknown>
  error?: string
}

export type AgyCommandRunner = (
  args: string[],
  env: Record<string, string>,
  timeoutMs: number
) => Promise<{ exitCode: number; stdout: string; stderr: string; timedOut: boolean }>

const MAX_AGY_MESSAGE_CHARS = 32_000

export function formatAgyEnvelope(message: MessageRecord): { title: string; content: string } {
  const targetLabel = message.channel_id
    ? `channel ${message.channel_id}`
    : message.fanout_id
      ? `fanout -> ${message.recipient_id}`
      : `DM -> ${message.recipient_id}`
  const title = `turnbus ${targetLabel} from ${message.sender_id} (#${message.id})`
  const content = formatPeerEnvelopeText({
    message,
    recipientId: message.recipient_id ?? 'agy',
    maxBodyChars: MAX_AGY_MESSAGE_CHARS,
  })
  return { title, content }
}

export const defaultAgyCommandRunner: AgyCommandRunner = async (args, env, timeoutMs) => {
  const proc = Bun.spawn(args, {
    env: {
      PATH: process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin',
      HOME: process.env.HOME ?? homedir(),
      ...env,
    },
    stdout: 'pipe',
    stderr: 'pipe',
  })

  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    try {
      proc.kill()
    } catch {}
  }, timeoutMs)

  try {
    const [stdoutBuf, stderrBuf, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    return {
      exitCode,
      stdout: stdoutBuf.slice(0, 16_384),
      stderr: stderrBuf.slice(0, 16_384),
      timedOut,
    }
  } finally {
    clearTimeout(timer)
  }
}

export async function deliverToAgyConversation(params: {
  creds: PrivateAgyCredentials | null
  message: MessageRecord
  runner?: AgyCommandRunner
  timeoutMs?: number
}): Promise<AgyDispatchResult> {
  const creds = params.creds
  if (!creds || !creds.conversationId || !creds.lsAddress || !creds.csrfToken) {
    return {
      outcome: 'definitely_not_accepted',
      stage: 'missing_agy_credentials',
      error: 'Antigravity session LS credentials not registered yet; message held in durable inbox',
    }
  }

  const runner = params.runner ?? defaultAgyCommandRunner
  const timeoutMs = params.timeoutMs ?? 8000
  const { title, content } = formatAgyEnvelope(params.message)

  const defaultBin = join(homedir(), '.gemini', 'antigravity-cli', 'bin', 'agentapi')
  const normalizedExe = creds.agentApiExe ? creds.agentApiExe.replace(/\.\d+\.old$/, '') : null
  const effectiveExe =
    normalizedExe && (params.runner || existsSync(normalizedExe))
      ? normalizedExe
      : !params.runner && existsSync(join(homedir(), '.local', 'bin', 'agy'))
        ? join(homedir(), '.local', 'bin', 'agy')
        : null
  const cmdArgs = effectiveExe
    ? [effectiveExe, 'agentapi', 'send-message', '--title', title, creds.conversationId, content]
    : [defaultBin, 'send-message', '--title', title, creds.conversationId, content]

  try {
    const res = await runner(
      cmdArgs,
      {
        ANTIGRAVITY_LS_ADDRESS: creds.lsAddress,
        ANTIGRAVITY_CSRF_TOKEN: creds.csrfToken,
        ANTIGRAVITY_CONVERSATION_ID: creds.conversationId,
      },
      timeoutMs
    )

    if (res.timedOut) {
      return {
        outcome: 'unknown',
        stage: 'agentapi_timeout',
        error: redactSecrets(`agentapi send-message timed out after ${timeoutMs}ms`, [creds.csrfToken]),
      }
    }

    if (res.exitCode !== 0) {
      const combined = redactSecrets(`${res.stderr || res.stdout || `exit code ${res.exitCode}`}`.trim(), [
        creds.csrfToken,
      ])
      return {
        outcome: 'definitely_not_accepted',
        stage: 'agentapi_nonzero_exit',
        error: combined,
      }
    }

    return {
      outcome: 'accepted',
      stage: 'agentapi_sent',
      receipt: {
        route: 'agentapi/send-message',
        conversationId: creds.conversationId,
      },
    }
  } catch (err: any) {
    const cleanErr = redactSecrets(String(err?.message ?? err), [creds.csrfToken])
    return {
      outcome: 'definitely_not_accepted',
      stage: 'agentapi_spawn_error',
      error: cleanErr,
    }
  }
}
