import { createHash } from 'node:crypto'
import { chmodSync, existsSync, readdirSync, readFileSync, realpathSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, join, resolve } from 'node:path'
import { normalizeHandle, ToolType, TurnBusDB } from './db.ts'

export interface DiscoveredClaudeSession {
  pid: number
  sessionId: string
  cwd: string
  procStart: string
  version: string
  status: string
  channelPushEnabled: boolean
}

export interface PrivateAgyCredentials {
  conversationId: string
  lsAddress: string
  csrfToken: string
  agentApiExe?: string
  updatedAt: number
}

export function canonicalizePath(inputPath: string | null | undefined): string | null {
  if (!inputPath) return null
  const trimmed = inputPath.trim()
  if (!trimmed) return null
  try {
    if (existsSync(trimmed)) {
      return realpathSync(trimmed)
    }
  } catch {}
  const resolved = resolve(trimmed)
  return resolved.length > 1 ? resolved.replace(/\/+$/, '') : resolved
}

export function isSameOrSubpath(childPath: string, parentWorkspace: string): { match: boolean; exact: boolean } {
  const canonChild = canonicalizePath(childPath)
  const canonParent = canonicalizePath(parentWorkspace)
  if (!canonChild || !canonParent) return { match: false, exact: false }
  if (canonChild === canonParent) return { match: true, exact: true }
  if (canonChild.startsWith(canonParent + '/')) return { match: true, exact: false }
  return { match: false, exact: false }
}

export function readProcStat(pid: number, procRoot = '/proc'): { ppid: number; starttime: string } | null {
  const statPath = join(procRoot, String(pid), 'stat')
  if (!existsSync(statPath)) return null
  try {
    const raw = readFileSync(statPath, 'utf-8')
    const closeParenIdx = raw.lastIndexOf(')')
    if (closeParenIdx === -1) return null
    const afterComm = raw.slice(closeParenIdx + 2).trim().split(/\s+/)
    const ppid = Number(afterComm[1])
    const starttime = afterComm[19]
    if (!Number.isFinite(ppid) || !starttime) return null
    return { ppid, starttime }
  } catch {
    return null
  }
}

export function readProcCmdline(pid: number, procRoot = '/proc'): string | null {
  const cmdPath = join(procRoot, String(pid), 'cmdline')
  if (!existsSync(cmdPath)) return null
  try {
    return readFileSync(cmdPath, 'utf-8').replace(/\0/g, ' ').trim()
  } catch {
    return null
  }
}

export function hasClaudeChannelServerFlag(cmdline: string, serverKey = 'turnbus'): boolean {
  if (!cmdline.includes('--dangerously-load-development-channels')) return false
  const escaped = serverKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return new RegExp(`(?:^|\\s|=|,)server:${escaped}(?:$|\\s|,)`).test(cmdline)
}

export function discoverClaudeSessionByParentWalk(
  startPid: number,
  options: {
    claudeSessionsDir?: string
    procRoot?: string
    maxDepth?: number
    channelServerKey?: string
  } = {}
): DiscoveredClaudeSession | null {
  const sessionsDir = options.claudeSessionsDir ?? join(homedir(), '.claude', 'sessions')
  const procRoot = options.procRoot ?? '/proc'
  const maxDepth = options.maxDepth ?? 6
  const channelServerKey = options.channelServerKey ?? 'turnbus'

  let currentPid = startPid
  for (let depth = 0; depth < maxDepth && currentPid > 1; depth++) {
    const sessionFile = join(sessionsDir, `${currentPid}.json`)
    const stat = readProcStat(currentPid, procRoot)
    if (existsSync(sessionFile) && stat) {
      try {
        const parsed = JSON.parse(readFileSync(sessionFile, 'utf-8'))
        if (
          parsed &&
          typeof parsed.sessionId === 'string' &&
          typeof parsed.cwd === 'string' &&
          String(parsed.procStart) === String(stat.starttime)
        ) {
          const cmdline = readProcCmdline(currentPid, procRoot) ?? ''
          return {
            pid: currentPid,
            sessionId: parsed.sessionId,
            cwd: parsed.cwd,
            procStart: String(parsed.procStart),
            version: String(parsed.version ?? 'unknown'),
            status: String(parsed.status ?? 'online'),
            channelPushEnabled: hasClaudeChannelServerFlag(cmdline, channelServerKey),
          }
        }
      } catch {}
    }
    if (!stat) break
    currentPid = stat.ppid
  }
  return null
}

export function scanAllLiveClaudeSessions(options: {
  claudeSessionsDir?: string
  procRoot?: string
  channelServerKey?: string
} = {}): DiscoveredClaudeSession[] {
  const sessionsDir = options.claudeSessionsDir ?? join(homedir(), '.claude', 'sessions')
  const procRoot = options.procRoot ?? '/proc'
  const channelServerKey = options.channelServerKey ?? 'turnbus'
  if (!existsSync(sessionsDir)) return []

  const results: DiscoveredClaudeSession[] = []
  try {
    const files = readdirSync(sessionsDir).filter(f => /^\d+\.json$/.test(f))
    for (const file of files) {
      const pid = Number(file.slice(0, -5))
      const stat = readProcStat(pid, procRoot)
      if (!stat) continue
      try {
        const parsed = JSON.parse(readFileSync(join(sessionsDir, file), 'utf-8'))
        if (
          parsed &&
          typeof parsed.sessionId === 'string' &&
          typeof parsed.cwd === 'string' &&
          String(parsed.procStart) === String(stat.starttime)
        ) {
          const cmdline = readProcCmdline(pid, procRoot) ?? ''
          results.push({
            pid,
            sessionId: parsed.sessionId,
            cwd: parsed.cwd,
            procStart: String(parsed.procStart),
            version: String(parsed.version ?? 'unknown'),
            status: String(parsed.status ?? 'online'),
            channelPushEnabled: hasClaudeChannelServerFlag(cmdline, channelServerKey),
          })
        }
      } catch {}
    }
  } catch {}
  return results
}

export class RosterManager {
  private readonly db: TurnBusDB
  private readonly agyCredsPath: string | null
  private readonly privateAgyCredentials = new Map<string, PrivateAgyCredentials>()

  constructor(db: TurnBusDB, stateDir?: string) {
    this.db = db
    this.agyCredsPath = stateDir ? join(stateDir, 'agy-creds.json') : null
    this.loadPersistedAgyCredentials()
  }

  private loadPersistedAgyCredentials(): void {
    if (!this.agyCredsPath || !existsSync(this.agyCredsPath)) return
    try {
      const raw = JSON.parse(readFileSync(this.agyCredsPath, 'utf-8')) as Record<string, PrivateAgyCredentials>
      for (const [id, creds] of Object.entries(raw)) {
        if (creds?.conversationId && creds?.lsAddress && creds?.csrfToken) {
          this.privateAgyCredentials.set(id.toLowerCase(), creds)
        }
      }
    } catch {}
  }

  private savePersistedAgyCredentials(): void {
    if (!this.agyCredsPath) return
    try {
      const obj = Object.fromEntries(this.privateAgyCredentials.entries())
      writeFileSync(this.agyCredsPath, JSON.stringify(obj, null, 2), { mode: 0o600 })
      chmodSync(this.agyCredsPath, 0o600)
    } catch {}
  }

  setPrivateAgyCredentials(agentId: string, creds: PrivateAgyCredentials): void {
    this.privateAgyCredentials.set(agentId.toLowerCase(), creds)
    this.savePersistedAgyCredentials()
  }

  getPrivateAgyCredentials(agentId: string): PrivateAgyCredentials | null {
    return this.privateAgyCredentials.get(agentId.toLowerCase()) ?? null
  }

  /**
   * Resolves an agent handle dynamically from (tool, workspaceCwd, explicitAgentId?, displayName?, nativeSessionId?).
   * Zero hardcoded team handles or paths.
   */
  resolveAgentIdentity(params: {
    tool: ToolType
    workspaceCwd?: string | null
    explicitAgentId?: string | null
    displayName?: string | null
    nativeSessionId?: string | null
    setWorkspaceDefault?: boolean
  }): {
    agentId: string
    displayName: string
    matchedBy: 'explicit' | 'session' | 'workspace_rule' | 'auto'
  } {
    const canonCwd = canonicalizePath(params.workspaceCwd)

    if (params.explicitAgentId) {
      const explicit = normalizeHandle(params.explicitAgentId)
      if (explicit === 'user' && params.tool !== 'user') {
        throw new Error(`Agent tool '${params.tool}' cannot claim reserved 'user' identity`)
      }
      const existing = this.db.getAgent(explicit)
      if (existing && existing.tool !== params.tool) {
        throw new Error(
          `Agent handle '${explicit}' is registered for tool '${existing.tool}', cannot bind as '${params.tool}'`
        )
      }
      const saved = this.db.upsertAgent({
        id: explicit,
        displayName: params.displayName ?? undefined,
        tool: params.tool,
        workspace: canonCwd,
      })
      if (params.setWorkspaceDefault && canonCwd && params.tool !== 'user') {
        this.db.setWorkspaceRule(canonCwd, params.tool, saved.id)
      }
      return {
        agentId: saved.id,
        displayName: saved.display_name,
        matchedBy: 'explicit',
      }
    }

    if (params.nativeSessionId) {
      const boundRow = this.db.db
        .query(`SELECT agent_id FROM agent_sessions WHERE native_session_id = ? AND is_current = 1 LIMIT 1`)
        .get(params.nativeSessionId) as { agent_id: string } | null
      if (boundRow) {
        const existing = this.db.getAgent(boundRow.agent_id)
        if (existing && existing.tool === params.tool) {
          if (params.displayName) {
            const upd = this.db.upsertAgent({
              id: existing.id,
              displayName: params.displayName,
              tool: params.tool,
              workspace: canonCwd,
            })
            return { agentId: upd.id, displayName: upd.display_name, matchedBy: 'session' }
          }
          return { agentId: existing.id, displayName: existing.display_name, matchedBy: 'session' }
        }
      }
    }

    if (canonCwd && params.tool !== 'user') {
      const rules = this.db.listWorkspaceRules().filter(r => r.tool === params.tool)
      for (const r of rules) {
        if (isSameOrSubpath(canonCwd, r.workspace).exact) {
          const agent = this.db.upsertAgent({
            id: r.handle,
            displayName: params.displayName ?? undefined,
            tool: params.tool,
            workspace: canonCwd,
          })
          return { agentId: agent.id, displayName: agent.display_name, matchedBy: 'workspace_rule' }
        }
      }
      const subpathMatches = rules.filter(r => {
        const rel = isSameOrSubpath(canonCwd, r.workspace)
        return rel.match && !rel.exact
      })
      if (subpathMatches.length === 1) {
        const agent = this.db.upsertAgent({
          id: subpathMatches[0].handle,
          displayName: params.displayName ?? undefined,
          tool: params.tool,
          workspace: canonCwd,
        })
        return { agentId: agent.id, displayName: agent.display_name, matchedBy: 'workspace_rule' }
      }
    }

    const folderRaw = canonCwd
      ? basename(canonCwd)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-+|-+$/g, '')
      : 'session'
    const baseFolder = folderRaw.slice(0, Math.max(8, 40 - params.tool.length)).replace(/-+$/g, '') || 'session'
    const cleanBaseId = `${params.tool}-${baseFolder}`.replace(/-+/g, '-').slice(0, 48)

    // Use clean <tool>-<folder> if free or already owned by this session/unbound; otherwise append 6-char hash
    const existingBase = this.db.getAgent(cleanBaseId)
    const activeBaseSess = existingBase ? this.db.getCurrentSession(cleanBaseId) : null
    const baseConflict =
      (existingBase && existingBase.tool !== params.tool) ||
      (activeBaseSess && params.nativeSessionId && activeBaseSess.native_session_id !== params.nativeSessionId)

    let targetId = cleanBaseId
    let defaultDisplay = params.displayName?.trim() || `${params.tool}:${baseFolder}`
    if (baseConflict) {
      const seed = params.nativeSessionId || `${canonCwd}:${Date.now()}`
      const shortHash = createHash('sha256').update(seed).digest('hex').slice(0, 6)
      const maxFolderLen = Math.max(8, 48 - params.tool.length - shortHash.length - 2)
      const folder = folderRaw.slice(0, maxFolderLen).replace(/-+$/g, '') || 'session'
      targetId = `${params.tool}-${folder}-${shortHash}`.replace(/-+/g, '-').slice(0, 48)
      defaultDisplay = params.displayName?.trim() || `${params.tool}:${folder} (${shortHash})`
    }

    const created = this.db.upsertAgent({
      id: targetId,
      displayName: defaultDisplay,
      tool: params.tool,
      workspace: canonCwd,
    })
    return {
      agentId: created.id,
      displayName: created.display_name,
      matchedBy: 'auto',
    }
  }

  /**
   * Resolves a target handle by exact ID, alias, or '<tool>:<folder>' selector.
   */
  resolveTargetHandle(target: string): string {
    const clean = target.trim().toLowerCase().replace(/^@+/, '')
    if (this.db.getAgent(clean)) return clean

    for (const a of this.db.listAgents()) {
      try {
        const aliases = JSON.parse(a.aliases || '[]') as string[]
        if (aliases.includes(clean)) return a.id
      } catch {}
    }

    if (clean.includes(':')) {
      const [toolPart, wsPart] = clean.split(':', 2)
      const matches = this.db
        .listAgents()
        .filter(a => a.tool === toolPart && a.workspace && basename(a.workspace).toLowerCase() === wsPart)
        .map(a => a.id)
      if (matches.length === 1) return matches[0]
      if (matches.length > 1) {
        throw new Error(`Ambiguous workspace target '${target}': matches [${matches.join(', ')}]`)
      }
    }
    throw new Error(`Unknown recipient handle: '${target}'`)
  }
}
