import { randomBytes, timingSafeEqual } from 'node:crypto'
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

export interface BusAuthConfig {
  stateDir: string
  agentToken: string
  uiToken: string
}

export function getDefaultStateDir(): string {
  return process.env.TURNBUS_STATE_DIR ?? join(homedir(), '.local', 'share', 'turnbus')
}

export function ensureStateDir(stateDir = getDefaultStateDir()): string {
  if (!existsSync(stateDir)) {
    mkdirSync(stateDir, { recursive: true, mode: 0o700 })
  }
  try {
    chmodSync(stateDir, 0o700)
  } catch {}
  return stateDir
}

function loadOrCreateToken(filePath: string): string {
  if (existsSync(filePath)) {
    try {
      const existing = readFileSync(filePath, 'utf-8').trim()
      if (existing.length >= 32) {
        try { chmodSync(filePath, 0o600) } catch {}
        return existing
      }
    } catch {}
  }
  const token = randomBytes(32).toString('hex')
  try {
    writeFileSync(filePath, token + '\n', { mode: 0o600, flag: 'wx' })
    try { chmodSync(filePath, 0o600) } catch {}
    return token
  } catch {
    try {
      const raced = readFileSync(filePath, 'utf-8').trim()
      if (raced.length >= 32) return raced
    } catch {}
    writeFileSync(filePath, token + '\n', { mode: 0o600 })
    try { chmodSync(filePath, 0o600) } catch {}
    return token
  }
}

export function initBusAuth(stateDir = getDefaultStateDir()): BusAuthConfig {
  ensureStateDir(stateDir)
  const agentToken = process.env.TURNBUS_TOKEN ?? loadOrCreateToken(join(stateDir, 'agent.token'))
  const uiToken = process.env.TURNBUS_UI_TOKEN ?? loadOrCreateToken(join(stateDir, 'ui.token'))
  return { stateDir, agentToken, uiToken }
}

export function readAgentToken(stateDir = getDefaultStateDir()): string {
  if (process.env.TURNBUS_TOKEN) return process.env.TURNBUS_TOKEN
  const tokenPath = join(stateDir, 'agent.token')
  if (existsSync(tokenPath)) {
    return readFileSync(tokenPath, 'utf-8').trim()
  }
  return initBusAuth(stateDir).agentToken
}

export function safeTokenEquals(a: string | null | undefined, b: string | null | undefined): boolean {
  if (!a || !b) return false
  const bufA = Buffer.from(a)
  const bufB = Buffer.from(b)
  if (bufA.length !== bufB.length) return false
  return timingSafeEqual(bufA, bufB)
}

export function validateLoopbackHostAndOrigin(req: Request, expectedPort?: number): { ok: boolean; reason?: string } {
  const host = req.headers.get('host')
  if (!host) {
    return { ok: false, reason: 'Missing Host header' }
  }
  const allowedHosts = expectedPort
    ? new Set([`127.0.0.1:${expectedPort}`, `localhost:${expectedPort}`])
    : new Set([host.toLowerCase()])
  if (expectedPort && !allowedHosts.has(host.toLowerCase())) {
    return { ok: false, reason: `Disallowed Host header: ${host}` }
  }

  const secFetchSite = req.headers.get('sec-fetch-site')?.trim().toLowerCase()
  if (secFetchSite && secFetchSite !== 'same-origin' && secFetchSite !== 'none') {
    return { ok: false, reason: `Cross-site Sec-Fetch-Site rejected: ${secFetchSite}` }
  }

  const origin = req.headers.get('origin')
  if (origin !== null) {
    try {
      const url = new URL(origin)
      const isLoopback = url.hostname === '127.0.0.1' || url.hostname === 'localhost'
      const portMatches = !expectedPort || url.port === String(expectedPort)
      if (!isLoopback || !portMatches || (url.protocol !== 'http:' && url.protocol !== 'https:')) {
        return { ok: false, reason: `Cross-origin request rejected: ${origin}` }
      }
    } catch {
      return { ok: false, reason: `Malformed Origin header: ${origin}` }
    }
  }

  return { ok: true }
}

export type AuthRole = 'agent' | 'ui' | 'none'

export function authenticateRequest(req: Request, auth: BusAuthConfig, expectedPort?: number): {
  ok: boolean
  role: AuthRole
  reason?: string
} {
  const hostCheck = validateLoopbackHostAndOrigin(req, expectedPort)
  if (!hostCheck.ok) {
    return { ok: false, role: 'none', reason: hostCheck.reason }
  }

  const bearer = req.headers.get('authorization')?.replace(/^Bearer\s+/i, '').trim()
  const headerToken = req.headers.get('x-turnbus-token')?.trim()
  const uiHeaderToken = req.headers.get('x-turnbus-ui-token')?.trim()
  const candidate = bearer || headerToken

  if (uiHeaderToken && safeTokenEquals(uiHeaderToken, auth.uiToken)) {
    return { ok: true, role: 'ui' }
  }
  if (candidate && safeTokenEquals(candidate, auth.uiToken)) {
    return { ok: true, role: 'ui' }
  }
  if (candidate && safeTokenEquals(candidate, auth.agentToken)) {
    return { ok: true, role: 'agent' }
  }

  return { ok: false, role: 'none', reason: 'Missing or invalid authentication token' }
}

export function redactSecrets(input: string, secrets: Array<string | undefined | null> = []): string {
  let out = input
  for (const s of secrets) {
    if (s && s.length >= 6) {
      out = out.split(s).join('[REDACTED]')
    }
  }
  out = out.replace(/x-codeium-csrf-token:\s*[^\s"']+/gi, 'x-codeium-csrf-token: [REDACTED]')
  out = out.replace(/ANTIGRAVITY_CSRF_TOKEN=[^\s"']+/gi, 'ANTIGRAVITY_CSRF_TOKEN=[REDACTED]')
  return out
}
