/**
 * GitLab platform adapter using REST API v4 and Webhooks
 * Handles issue and MR comments with @mention detection
 *
 * Community forge adapter — see packages/adapters/src/community/forge/README.md
 */
import { readdir, access } from 'fs/promises';
import { join } from 'path';
import type { IPlatformAdapter, MessageMetadata } from '@archon/core';
import type { IsolationHints } from '@archon/isolation';
import {
  ConversationNotFoundError,
  handleMessage,
  classifyAndFormatError,
  toError,
  onConversationClosed,
  ConversationLockManager,
} from '@archon/core';
import {
  ensureProjectStructure,
  getCommandFolderSearchPaths,
  getProjectSourcePath,
  createLogger,
} from '@archon/paths';
import {
  syncRepository,
  addSafeDirectory,
  toRepoPath,
  toBranchName,
  isWorktreePath,
  execFileAsync,
} from '@archon/git';
import * as db from '@archon/core/db/conversations';
import * as codebaseDb from '@archon/core/db/codebases';
import * as userDb from '@archon/core/db/users';
import { resolveDefaultAssistant } from '@archon/core/config/resolve-assistant';
import { parseAllowedUsers, isGitLabUserAuthorized, verifyWebhookToken } from './auth';
import { splitIntoParagraphChunks } from '../../../utils/message-splitting';
import type { GitLabWebhookEvent, GitLabIssue, GitLabMergeRequest } from './types';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('adapter.gitlab');
  return cachedLog;
}

const MAX_LENGTH = 65000; // Practical limit for GitLab notes

/** Hidden marker added to bot comments to prevent self-triggering loops */
const BOT_RESPONSE_MARKER = '<!-- archon-bot-response -->';

interface GitLabParsedEvent {
  projectPath: string;
  iid: number;
  comment: string;
  eventType: 'note' | 'issue' | 'merge_request';
  isMR: boolean;
  issue?: GitLabIssue;
  mergeRequest?: GitLabMergeRequest;
  isCloseEvent?: boolean;
  isMerged?: boolean;
}

interface GitLabWebhookMessage {
  conversationId: string;
  finalMessage: string;
  contextToAppend?: string;
  isolationHints: IsolationHints;
  archonUserId?: string;
}

export class GitLabAdapter implements IPlatformAdapter {
  private readonly gitlabUrl: string;
  private readonly token: string;
  private readonly webhookSecret: string;
  private readonly allowedUsers: string[];
  private readonly botMention: string;
  private readonly lockManager: ConversationLockManager;

  constructor(
    token: string,
    webhookSecret: string,
    lockManager: ConversationLockManager,
    gitlabUrl?: string,
    botMention?: string
  ) {
    if (!token) {
      throw new Error('GitLabAdapter requires a non-empty token');
    }
    if (!webhookSecret) {
      throw new Error('GitLabAdapter requires a non-empty webhookSecret');
    }

    this.gitlabUrl = (gitlabUrl ?? 'https://gitlab.com').replace(/\/+$/, '');
    this.token = token;
    this.webhookSecret = webhookSecret;
    this.lockManager = lockManager;
    this.botMention = botMention ?? 'Archon';

    this.allowedUsers = parseAllowedUsers(process.env.GITLAB_ALLOWED_USERS);
    if (this.allowedUsers.length > 0) {
      getLog().info({ userCount: this.allowedUsers.length }, 'gitlab.whitelist_enabled');
    } else {
      getLog().info('gitlab.whitelist_disabled');
    }

    getLog().info(
      { botMention: this.botMention, gitlabUrl: this.gitlabUrl },
      'gitlab.adapter_initialized'
    );
  }

  // ---------------------------------------------------------------------------
  // IPlatformAdapter methods
  // ---------------------------------------------------------------------------

  async sendMessage(
    conversationId: string,
    message: string,
    _metadata?: MessageMetadata
  ): Promise<void> {
    const parsed = this.parseConversationId(conversationId);
    if (!parsed) {
      getLog().error({ conversationId }, 'gitlab.invalid_conversation_id');
      return;
    }

    getLog().debug({ conversationId, messageLength: message.length }, 'gitlab.send_message');

    if (message.length <= MAX_LENGTH) {
      await this.postComment(parsed, message);
    } else {
      getLog().debug({ messageLength: message.length }, 'gitlab.message_splitting');
      const chunks = splitIntoParagraphChunks(message, MAX_LENGTH - 500);

      for (let i = 0; i < chunks.length; i++) {
        try {
          await this.postComment(parsed, chunks[i]);
        } catch (error) {
          const err = error as Error;
          getLog().error(
            { err, chunkIndex: i + 1, totalChunks: chunks.length, conversationId },
            'gitlab.chunk_post_failed'
          );
          const partialError = new Error(
            `Failed to post comment chunk ${String(i + 1)}/${String(chunks.length)}. ` +
              `${String(i)} chunk(s) were posted before failure.`
          );
          partialError.cause = error;
          throw partialError;
        }
      }
    }
  }

  getStreamingMode(): 'batch' {
    return 'batch';
  }

  getPlatformType(): string {
    return 'gitlab';
  }

  async start(): Promise<void> {
    getLog().info('gitlab.webhook_adapter_ready');
  }

  stop(): void {
    getLog().info('gitlab.adapter_stopped');
  }

  async ensureThread(originalConversationId: string, _messageContext?: unknown): Promise<string> {
    return originalConversationId;
  }

  // ---------------------------------------------------------------------------
  // GitLab REST API helper
  // ---------------------------------------------------------------------------

  private async gitlabApi<T>(method: string, path: string, body?: unknown): Promise<T> {
    const url = `${this.gitlabUrl}/api/v4${path}`;
    const response = await fetch(url, {
      method,
      headers: {
        'PRIVATE-TOKEN': this.token,
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(
        `GitLab API ${method} ${path}: ${String(response.status)} ${response.statusText} - ${text}`
      );
    }
    return response.json() as Promise<T>;
  }

  // ---------------------------------------------------------------------------
  // Comment posting with retry
  // ---------------------------------------------------------------------------

  private isRetryableError(error: unknown): boolean {
    const err = error as Error | undefined;
    const message = err?.message ?? '';
    const causeErr = (error as { cause?: Error }).cause;
    const cause = causeErr?.message ?? '';
    const combined = `${message} ${cause}`.toLowerCase();

    return (
      combined.includes('timeout') ||
      combined.includes('econnrefused') ||
      combined.includes('econnreset') ||
      combined.includes('etimedout') ||
      combined.includes('fetch failed') ||
      combined.includes('429') ||
      combined.includes('502') ||
      combined.includes('503') ||
      combined.includes('504')
    );
  }

  private async postComment(
    parsed: { projectPath: string; iid: number; isMR: boolean },
    message: string
  ): Promise<void> {
    const markedMessage = `${message}\n\n${BOT_RESPONSE_MARKER}`;
    const maxRetries = 3;
    const conversationId = this.buildConversationId(parsed.projectPath, parsed.iid, parsed.isMR);
    const encodedProject = encodeURIComponent(parsed.projectPath);

    const notesPath = parsed.isMR
      ? `/projects/${encodedProject}/merge_requests/${String(parsed.iid)}/notes`
      : `/projects/${encodedProject}/issues/${String(parsed.iid)}/notes`;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.gitlabApi('POST', notesPath, { body: markedMessage });
        getLog().debug({ conversationId }, 'gitlab.comment_posted');
        return;
      } catch (error) {
        const isRetryable = this.isRetryableError(error);
        if (attempt < maxRetries && isRetryable) {
          const delay = 1000 * attempt;
          getLog().warn(
            { attempt, maxRetries, conversationId, delayMs: delay },
            'gitlab.comment_post_retry'
          );
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        getLog().error(
          {
            err: error,
            conversationId,
            attempt,
            maxRetries,
            wasRetryable: isRetryable,
            messageLength: message.length,
          },
          'gitlab.comment_post_failed'
        );
        throw error;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Conversation ID: group/project#123 (issues), group/project!456 (MRs)
  // ---------------------------------------------------------------------------

  private buildConversationId(projectPath: string, iid: number, isMR: boolean): string {
    const separator = isMR ? '!' : '#';
    return `${projectPath}${separator}${String(iid)}`;
  }

  private parseConversationId(
    conversationId: string
  ): { projectPath: string; iid: number; isMR: boolean } | null {
    const match = /^(.+?)([#!])(\d+)$/.exec(conversationId);
    if (!match) return null;
    return {
      projectPath: match[1],
      iid: parseInt(match[3], 10),
      isMR: match[2] === '!',
    };
  }

  // ---------------------------------------------------------------------------
  // @mention detection
  // ---------------------------------------------------------------------------

  private hasMention(text: string): boolean {
    const pattern = new RegExp(`@${this.botMention}(?:[\\s,:;]|$)`, 'i');
    return pattern.test(text);
  }

  private stripMention(text: string): string {
    // `+` consumes all trailing separators (e.g. "@archon, " not just "@archon")
    const pattern = new RegExp(`@${this.botMention}(?:[\\s,:;]+|$)`, 'gi');
    return text.replace(pattern, '').trim();
  }

  // ---------------------------------------------------------------------------
  // Event parsing
  // ---------------------------------------------------------------------------

  private parseEvent(event: GitLabWebhookEvent): GitLabParsedEvent | null {
    const projectPath = event.project.path_with_namespace;

    // Issue closed
    if (event.object_kind === 'issue' && event.object_attributes.action === 'close') {
      return {
        projectPath,
        iid: event.object_attributes.iid,
        comment: '',
        eventType: 'issue',
        isMR: false,
        isCloseEvent: true,
      };
    }

    // MR closed or merged
    if (event.object_kind === 'merge_request') {
      const action = event.object_attributes.action;
      if (action === 'close' || action === 'merge') {
        return {
          projectPath,
          iid: event.object_attributes.iid,
          comment: '',
          eventType: 'merge_request',
          isMR: true,
          isCloseEvent: true,
          isMerged: action === 'merge',
        };
      }
    }

    // Note (comment) on issue or MR
    if (event.object_kind === 'note') {
      const noteType = event.object_attributes.noteable_type;
      const isMR = noteType === 'MergeRequest';
      const iid = isMR ? event.merge_request?.iid : event.issue?.iid;

      if (!iid) return null;

      return {
        projectPath,
        iid,
        comment: event.object_attributes.note,
        eventType: 'note',
        isMR,
        issue: event.issue,
        mergeRequest: event.merge_request,
      };
    }

    return null;
  }

  // ---------------------------------------------------------------------------
  // Comment history
  // ---------------------------------------------------------------------------

  private async fetchCommentHistory(
    projectPath: string,
    iid: number,
    isMR: boolean
  ): Promise<string[]> {
    try {
      const encodedProject = encodeURIComponent(projectPath);
      const notesPath = isMR
        ? `/projects/${encodedProject}/merge_requests/${String(iid)}/notes?per_page=20&sort=asc`
        : `/projects/${encodedProject}/issues/${String(iid)}/notes?per_page=20&sort=asc`;

      const notes = await this.gitlabApi<
        { author?: { username?: string } | null; body?: string | null }[]
      >('GET', notesPath);

      return notes.slice(-20).map(note => {
        const author = note.author?.username ?? 'unknown';
        const body = note.body ?? '';
        return `${author}: ${body}`;
      });
    } catch (error) {
      getLog().error({ err: error, projectPath, iid, isMR }, 'gitlab.comment_history_fetch_failed');
      return [];
    }
  }

  // ---------------------------------------------------------------------------
  // Context builders
  // ---------------------------------------------------------------------------

  private buildIssueContext(issue: GitLabIssue, userComment: string): string {
    const labels = issue.labels.map(l => l.title).join(', ');
    return `[GitLab Issue Context]
Issue #${String(issue.iid)}: "${issue.title}"
Labels: ${labels}
Status: ${issue.state}

Description:
${issue.description ?? ''}

---

${userComment}

Use 'glab issue view ${String(issue.iid)}' for full details if needed.`;
  }

  private buildMRContext(mr: GitLabMergeRequest, userComment: string): string {
    return `[GitLab Merge Request Context]
MR !${String(mr.iid)}: "${mr.title}"
Status: ${mr.state}
Source: ${mr.source_branch} → ${mr.target_branch}

Description:
${mr.description ?? ''}

---

${userComment}

Use 'glab mr view ${String(mr.iid)}' for full details and 'glab mr diff ${String(mr.iid)}' for the diff.`;
  }

  // ---------------------------------------------------------------------------
  // Repository management
  // ---------------------------------------------------------------------------

  private async ensureRepoReady(
    projectPath: string,
    defaultBranch: string,
    repoPath: string,
    shouldSync: boolean
  ): Promise<void> {
    let directoryExists = false;
    try {
      await access(repoPath);
      directoryExists = true;
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code !== 'ENOENT') {
        getLog().error({ repoPath, errorCode: err.code, err }, 'gitlab.repo_path_access_failed');
        throw new Error(
          `Cannot access repository at ${repoPath}: ${err.code ?? err.message}. ` +
            'Check permissions and disk health.'
        );
      }
    }

    if (directoryExists) {
      if (shouldSync) {
        getLog().info({ repoPath, defaultBranch }, 'gitlab.repo_syncing');
        const syncResult = await syncRepository(toRepoPath(repoPath), toBranchName(defaultBranch));
        if (!syncResult.ok) {
          getLog().error(
            { error: syncResult.error, repoPath, defaultBranch },
            'gitlab.repo_sync_failed'
          );
          throw new Error(
            `Failed to sync repository to ${defaultBranch}. ` +
              'Try /reset or check if the branch exists.'
          );
        }
      }
      return;
    }

    // Clone the repository
    // GitLab self-hosted instances need oauth2:token auth and credential helper disabled
    // to prevent macOS Keychain from intercepting and blocking the clone
    getLog().info({ projectPath, repoPath }, 'gitlab.repo_cloning');

    // Create project structure (source/, worktrees/, artifacts/, logs/) before
    // cloning so worktree paths resolve correctly on first webhook clone.
    // For nested namespaces (group/subgroup/repo), the namespace becomes the
    // owner and the leaf segment becomes the repo.
    const cloneSegments = projectPath.split('/');
    const cloneRepo = cloneSegments[cloneSegments.length - 1];
    const cloneOwner = cloneSegments.slice(0, -1).join('/');
    await ensureProjectStructure(cloneOwner, cloneRepo);

    const urlObj = new URL(this.gitlabUrl);
    const repoUrl = `${urlObj.protocol}//oauth2:${this.token}@${urlObj.host}/${projectPath}.git`;

    try {
      await execFileAsync('git', ['-c', 'credential.helper=', 'clone', repoUrl, repoPath], {
        timeout: 120000,
      });
    } catch (error) {
      const err = error as Error;
      // Sanitize token from all error properties (message, stack, cause)
      const sanitize = (s: string): string => s.replaceAll(this.token, '***');
      const sanitized = sanitize(err.message);
      const msg = sanitized.toLowerCase();

      const sanitizedError: Record<string, unknown> = { message: sanitized };
      if (err.stack) sanitizedError.stack = sanitize(err.stack);
      if (err.cause && typeof (err.cause as Error).message === 'string') {
        sanitizedError.cause = sanitize((err.cause as Error).message);
      }
      const errRecord = err as unknown as Record<string, unknown>;
      if (typeof errRecord.stdout === 'string') sanitizedError.stdout = sanitize(errRecord.stdout);
      if (typeof errRecord.stderr === 'string') sanitizedError.stderr = sanitize(errRecord.stderr);

      getLog().error({ projectPath, repoPath, error: sanitizedError }, 'gitlab.repo_clone_failed');

      if (msg.includes('not found') || msg.includes('404')) {
        throw new Error(
          `Repository ${projectPath} not found or is private. Check repository access.`
        );
      }
      if (
        msg.includes('authentication failed') ||
        msg.includes('could not read') ||
        msg.includes('403')
      ) {
        throw new Error(
          `Authentication failed for ${projectPath}. Check GITLAB_TOKEN permissions.`
        );
      }
      throw new Error(`Failed to clone ${projectPath}: ${sanitized}`);
    }

    await addSafeDirectory(toRepoPath(repoPath));
  }

  private async autoDetectAndLoadCommands(repoPath: string, codebaseId: string): Promise<void> {
    const commandFolders = getCommandFolderSearchPaths();

    for (const folder of commandFolders) {
      try {
        const fullPath = join(repoPath, folder);
        await access(fullPath);

        const files = (await readdir(fullPath)).filter(f => f.endsWith('.md'));
        if (files.length === 0) continue;

        const commands = await codebaseDb.getCodebaseCommands(codebaseId);
        files.forEach(file => {
          commands[file.replace('.md', '')] = {
            path: join(folder, file),
            description: `From ${folder}`,
          };
        });

        await codebaseDb.updateCodebaseCommands(codebaseId, commands);
        getLog().info({ commandCount: files.length, folder }, 'gitlab.commands_loaded');
        return;
      } catch (error) {
        const err = error as NodeJS.ErrnoException;
        if (err.code === 'ENOENT') {
          continue;
        }
        getLog().error({ err, folder, errorCode: err.code }, 'gitlab.commands_load_error');
        continue;
      }
    }
  }

  private async getOrCreateCodebaseForRepo(projectPath: string): Promise<{
    codebase: { id: string; name: string; default_cwd: string };
    repoPath: string;
    isNew: boolean;
  }> {
    const repoUrlNoGit = `${this.gitlabUrl}/${projectPath}`;
    const repoUrlWithGit = `${repoUrlNoGit}.git`;

    let existing = await codebaseDb.findCodebaseByRepoUrl(repoUrlNoGit);
    existing ??= await codebaseDb.findCodebaseByRepoUrl(repoUrlWithGit);

    // Canonical path uses the project source/ subdirectory so that worktrees/,
    // artifacts/, and logs/ live as siblings of the cloned repo (not nested
    // inside it). For nested GitLab namespaces (group/subgroup/repo), the
    // namespace becomes the owner, the leaf segment becomes the repo. Mirrors
    // the CLI /clone path; see issue #1547.
    const segments = projectPath.split('/');
    const gitlabRepo = segments[segments.length - 1];
    const gitlabOwner = segments.slice(0, -1).join('/');
    const canonicalPath = getProjectSourcePath(gitlabOwner, gitlabRepo);

    if (existing) {
      const looksLikeWorktreePath = existing.default_cwd.includes('/worktrees/');
      if (looksLikeWorktreePath || (await isWorktreePath(existing.default_cwd))) {
        getLog().info(
          { codebaseName: existing.name, canonicalPath },
          'gitlab.stale_worktree_path_fixed'
        );
        await codebaseDb.updateCodebase(existing.id, { default_cwd: canonicalPath });
        existing.default_cwd = canonicalPath;
      }

      getLog().info(
        { codebaseName: existing.name, path: existing.default_cwd },
        'gitlab.existing_codebase_found'
      );
      return { codebase: existing, repoPath: existing.default_cwd, isNew: false };
    }

    const codebase = await codebaseDb.createCodebase({
      name: projectPath,
      repository_url: repoUrlNoGit,
      default_cwd: canonicalPath,
      ai_assistant_type: await resolveDefaultAssistant(canonicalPath),
    });

    getLog().info({ codebaseName: codebase.name, path: canonicalPath }, 'gitlab.codebase_created');
    return { codebase, repoPath: canonicalPath, isNew: true };
  }

  private async cleanupWorktree(
    projectPath: string,
    iid: number,
    isMR: boolean,
    merged = false
  ): Promise<void> {
    const conversationId = this.buildConversationId(projectPath, iid, isMR);
    getLog().info({ conversationId, merged }, 'gitlab.isolation_cleanup_started');

    try {
      await onConversationClosed('gitlab', conversationId, { merged });
      getLog().info({ conversationId }, 'gitlab.isolation_cleanup_completed');
    } catch (error) {
      const err = error as Error;
      getLog().error({ err, conversationId }, 'gitlab.isolation_cleanup_failed');
    }
  }

  // ---------------------------------------------------------------------------
  // Webhook handler
  // ---------------------------------------------------------------------------

  private parseAuthorizedWebhook(payload: string, token: string): GitLabWebhookEvent | undefined {
    if (!verifyWebhookToken(token, this.webhookSecret)) {
      getLog().error(
        { tokenPrefix: token?.substring(0, 8) + '...', payloadSize: payload.length },
        'gitlab.invalid_webhook_token'
      );
      return undefined;
    }

    let event: GitLabWebhookEvent;
    try {
      event = JSON.parse(payload) as GitLabWebhookEvent;
    } catch (error) {
      getLog().error({ err: error, payloadSize: payload.length }, 'gitlab.webhook_parse_failed');
      return undefined;
    }

    const senderUsername = event.user?.username;
    if (!isGitLabUserAuthorized(senderUsername, this.allowedUsers)) {
      const maskedUser = senderUsername ? `${senderUsername.slice(0, 3)}***` : 'unknown';
      getLog().info({ maskedUser }, 'gitlab.unauthorized_webhook');
      return undefined;
    }
    return event;
  }

  private shouldIgnoreGitLabComment(event: GitLabWebhookEvent, comment: string): boolean {
    if (comment.includes(BOT_RESPONSE_MARKER)) {
      getLog().debug({ commentAuthor: event.user?.username }, 'gitlab.ignoring_marked_comment');
      return true;
    }
    if (event.user?.username?.toLowerCase() === this.botMention.toLowerCase()) {
      getLog().debug({ commentAuthor: event.user.username }, 'gitlab.ignoring_own_comment');
      return true;
    }
    return !this.hasMention(comment);
  }

  private async resolveGitLabUserId(
    senderUsername: string | undefined
  ): Promise<string | undefined> {
    if (!senderUsername) return undefined;
    try {
      const user = await userDb.findOrCreateUserByPlatformIdentity(
        'gitlab',
        senderUsername,
        senderUsername
      );
      return user.id;
    } catch (err) {
      getLog().warn(
        { err: toError(err), gitlabUsername: senderUsername },
        'gitlab.user_resolve_failed'
      );
      return undefined;
    }
  }

  private async linkGitLabConversation(
    conversationId: string,
    codebaseId: string,
    repoPath: string,
    isNewConversation: boolean
  ): Promise<void> {
    if (!isNewConversation) return;
    try {
      await db.updateConversation(conversationId, { codebase_id: codebaseId, cwd: repoPath });
    } catch (updateError) {
      if (updateError instanceof ConversationNotFoundError) {
        getLog().error({ conversationId, codebaseId }, 'gitlab.conversation_codebase_link_failed');
        throw new Error('Failed to set up GitLab conversation - please try again');
      }
      throw updateError;
    }
  }

  private async buildGitLabIsolationHints(parsed: GitLabParsedEvent): Promise<IsolationHints> {
    const isolationHints: IsolationHints = {
      workflowType: parsed.isMR ? 'pr' : 'issue',
      workflowId: String(parsed.iid),
    };
    if (parsed.isMR && parsed.mergeRequest) {
      isolationHints.prBranch = toBranchName(parsed.mergeRequest.source_branch);
      isolationHints.isForkPR =
        parsed.mergeRequest.source_project_id !== parsed.mergeRequest.target_project_id;
      getLog().info(
        {
          mrIid: parsed.iid,
          sourceBranch: parsed.mergeRequest.source_branch,
          isFork: isolationHints.isForkPR,
        },
        'gitlab.mr_head_info'
      );
    }
    return isolationHints;
  }

  private buildGitLabMessage(parsed: GitLabParsedEvent): {
    finalMessage: string;
    contextToAppend?: string;
  } {
    const strippedComment = this.stripMention(parsed.comment);
    if (strippedComment.trim().startsWith('/')) {
      const finalMessage = strippedComment.split('\n')[0].trim();
      getLog().debug({ command: finalMessage }, 'gitlab.slash_command_processing');
      return { finalMessage, contextToAppend: this.gitLabReferenceContext(parsed) };
    }
    if (parsed.isMR && parsed.mergeRequest) {
      return {
        finalMessage: this.buildMRContext(parsed.mergeRequest, strippedComment),
        contextToAppend: this.gitLabReferenceContext(parsed),
      };
    }
    if (parsed.issue) {
      return {
        finalMessage: this.buildIssueContext(parsed.issue, strippedComment),
        contextToAppend: this.gitLabReferenceContext(parsed),
      };
    }
    return { finalMessage: strippedComment };
  }

  private gitLabReferenceContext(parsed: GitLabParsedEvent): string | undefined {
    if (parsed.isMR && parsed.mergeRequest) {
      return `GitLab Merge Request !${String(parsed.mergeRequest.iid)}: "${parsed.mergeRequest.title}"
Use 'glab mr view ${String(parsed.mergeRequest.iid)}' for full details if needed.`;
    }
    if (parsed.issue) {
      return `GitLab Issue #${String(parsed.issue.iid)}: "${parsed.issue.title}"
Use 'glab issue view ${String(parsed.issue.iid)}' for full details if needed.`;
    }
    return undefined;
  }

  private async prepareGitLabMessage(
    event: GitLabWebhookEvent,
    parsed: GitLabParsedEvent,
    archonUserId: string | undefined
  ): Promise<GitLabWebhookMessage> {
    const conversationId = this.buildConversationId(parsed.projectPath, parsed.iid, parsed.isMR);
    const existingConv = await db.getOrCreateConversation('gitlab', conversationId);
    const {
      codebase,
      repoPath,
      isNew: isNewCodebase,
    } = await this.getOrCreateCodebaseForRepo(parsed.projectPath);
    await this.linkGitLabConversation(
      existingConv.id,
      codebase.id,
      repoPath,
      !existingConv.codebase_id
    );
    await this.ensureRepoReady(
      parsed.projectPath,
      event.project.default_branch,
      repoPath,
      isNewCodebase
    );
    if (isNewCodebase) await this.autoDetectAndLoadCommands(repoPath, codebase.id);

    const isolationHints = await this.buildGitLabIsolationHints(parsed);
    const { finalMessage, contextToAppend } = this.buildGitLabMessage(parsed);
    return { conversationId, finalMessage, contextToAppend, isolationHints, archonUserId };
  }

  private async dispatchGitLabMessage(message: GitLabWebhookMessage): Promise<void> {
    const commentHistory = await this.fetchCommentHistory(
      message.conversationId.replace(/[#!]\d+$/, ''),
      Number(/[#!](\d+)$/.exec(message.conversationId)?.[1] ?? 0),
      message.isolationHints.workflowType === 'pr'
    );
    const threadContext = commentHistory.length > 0 ? commentHistory.join('\n') : undefined;
    getLog().debug(
      {
        commentCount: threadContext ? commentHistory.length : 0,
        conversationId: message.conversationId,
      },
      'gitlab.thread_context_loaded'
    );
    await this.lockManager.acquireLock(message.conversationId, async () => {
      try {
        await handleMessage(this, message.conversationId, message.finalMessage, {
          issueContext: message.contextToAppend,
          threadContext,
          isolationHints: message.isolationHints,
          userId: message.archonUserId,
        });
      } catch (error) {
        await this.reportGitLabMessageError(message.conversationId, toError(error));
      }
    });
  }

  private async reportGitLabMessageError(conversationId: string, err: Error): Promise<void> {
    getLog().error({ err, conversationId }, 'gitlab.message_handling_error');
    try {
      await this.sendMessage(conversationId, classifyAndFormatError(err));
    } catch (sendError) {
      getLog().error(
        { err: toError(sendError), conversationId },
        'gitlab.error_message_send_failed'
      );
    }
  }

  private async reportGitLabSetupError(parsed: GitLabParsedEvent, error: unknown): Promise<void> {
    const err = toError(error);
    const conversationId = this.buildConversationId(parsed.projectPath, parsed.iid, parsed.isMR);
    getLog().error({ err, conversationId }, 'gitlab.webhook_setup_failed');
    try {
      await this.sendMessage(conversationId, classifyAndFormatError(err));
    } catch (sendError) {
      getLog().error(
        { err: toError(sendError), conversationId },
        'gitlab.webhook_setup_error_send_failed'
      );
    }
  }

  async handleWebhook(payload: string, token: string): Promise<void> {
    const event = this.parseAuthorizedWebhook(payload, token);
    if (!event) return;

    const parsed = this.parseEvent(event);
    if (!parsed) return;

    if (parsed.isCloseEvent) {
      const mergeLabel = parsed.isMerged ? 'merge' : 'close';
      getLog().info(
        { event: mergeLabel, projectPath: parsed.projectPath, iid: parsed.iid },
        'gitlab.close_event_received'
      );
      await this.cleanupWorktree(
        parsed.projectPath,
        parsed.iid,
        parsed.isMR,
        parsed.isMerged ?? false
      );
      return;
    }

    if (this.shouldIgnoreGitLabComment(event, parsed.comment)) return;
    getLog().info(
      {
        eventType: parsed.eventType,
        projectPath: parsed.projectPath,
        iid: parsed.iid,
        isMR: parsed.isMR,
      },
      'gitlab.webhook_processing'
    );

    const archonUserId = await this.resolveGitLabUserId(event.user?.username);
    try {
      const message = await this.prepareGitLabMessage(event, parsed, archonUserId);
      await this.dispatchGitLabMessage(message);
    } catch (error) {
      await this.reportGitLabSetupError(parsed, error);
    }
  }
}
