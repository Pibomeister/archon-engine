/**
 * Gitea platform adapter using REST API and Webhooks
 * Handles issue and PR comments with @mention detection
 *
 * Community forge adapter — see packages/adapters/src/community/forge/README.md
 */
import { createHmac, timingSafeEqual } from 'crypto';
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
import * as userDb from '@archon/core/db/users';
import {
  ensureProjectStructure,
  getCommandFolderSearchPaths,
  getProjectSourcePath,
  createLogger,
} from '@archon/paths';
import {
  cloneRepository,
  syncRepository,
  addSafeDirectory,
  toRepoPath,
  toBranchName,
  isWorktreePath,
} from '@archon/git';
import * as db from '@archon/core/db/conversations';
import * as codebaseDb from '@archon/core/db/codebases';
import { resolveDefaultAssistant } from '@archon/core/config/resolve-assistant';
import { parseAllowedUsers, isGiteaUserAuthorized } from './auth';
import { splitIntoParagraphChunks } from '../../../utils/message-splitting';
import type { WebhookEvent } from './types';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('adapter.gitea');
  return cachedLog;
}

const MAX_LENGTH = 65000; // Gitea comment limit (similar to GitHub)

/** Hidden marker added to bot comments to prevent self-triggering loops */
const BOT_RESPONSE_MARKER = '<!-- archon-bot-response -->';

interface GiteaParsedEvent {
  owner: string;
  repo: string;
  number: number;
  comment: string;
  eventType: 'issue' | 'issue_comment' | 'pull_request';
  isPR: boolean;
  issue?: WebhookEvent['issue'];
  pullRequest?: WebhookEvent['pull_request'];
  isCloseEvent?: boolean;
  isMerged?: boolean;
}

interface GiteaWebhookMessage {
  conversationId: string;
  owner: string;
  repo: string;
  number: number;
  finalMessage: string;
  contextToAppend?: string;
  isolationHints: IsolationHints;
  archonUserId?: string;
}

export class GiteaAdapter implements IPlatformAdapter {
  private baseUrl: string;
  private token: string;
  private webhookSecret: string;
  private allowedUsers: string[];
  private botMention: string;
  private lockManager: ConversationLockManager;
  private readonly retryDelayFn: (attempt: number) => number;

  constructor(
    baseUrl: string,
    token: string,
    webhookSecret: string,
    lockManager: ConversationLockManager,
    botMention?: string,
    options?: { retryDelayMs?: (attempt: number) => number }
  ) {
    // Normalize base URL (remove trailing slash)
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.token = token;
    this.webhookSecret = webhookSecret;
    this.lockManager = lockManager;
    this.botMention = botMention ?? 'Archon';

    // Parse Gitea user whitelist (optional - empty = open access)
    this.allowedUsers = parseAllowedUsers(process.env.GITEA_ALLOWED_USERS);
    if (this.allowedUsers.length > 0) {
      getLog().info({ userCount: this.allowedUsers.length }, 'whitelist_enabled');
    } else {
      getLog().info('whitelist_disabled');
    }

    this.retryDelayFn = options?.retryDelayMs ?? ((attempt: number): number => 1000 * attempt);

    getLog().info({ botMention: this.botMention, baseUrl: this.baseUrl }, 'adapter_initialized');
  }

  /**
   * Check if an error is retryable (transient network issues)
   */
  private isRetryableError(error: unknown): boolean {
    const err = error as Error | undefined;
    const message = err?.message ?? '';
    const causeErr = (error as { cause?: Error }).cause;
    const cause = causeErr?.message ?? '';
    const combined = `${message} ${cause}`.toLowerCase();

    // Retry on transient network errors
    return (
      combined.includes('timeout') ||
      combined.includes('econnrefused') ||
      combined.includes('econnreset') ||
      combined.includes('etimedout') ||
      combined.includes('fetch failed')
    );
  }

  /**
   * Send a message to a Gitea issue or PR.
   * Splits long messages into paragraph-based chunks.
   * Throws on failure so caller can handle appropriately.
   */
  async sendMessage(
    conversationId: string,
    message: string,
    _metadata?: MessageMetadata
  ): Promise<void> {
    const parsed = this.parseConversationId(conversationId);
    if (!parsed) {
      getLog().error({ conversationId }, 'invalid_conversation_id');
      return;
    }

    getLog().debug({ conversationId, messageLength: message.length }, 'send_message');

    // Check if message needs splitting
    if (message.length <= MAX_LENGTH) {
      await this.postComment(parsed, message);
    } else {
      getLog().debug({ messageLength: message.length }, 'message_splitting');
      const chunks = splitIntoParagraphChunks(message, MAX_LENGTH - 500);

      // Fail-fast: if any chunk fails, stop and propagate error with context
      for (let i = 0; i < chunks.length; i++) {
        try {
          await this.postComment(parsed, chunks[i]);
        } catch (error) {
          const err = error as Error;
          getLog().error(
            { err, chunkIndex: i + 1, totalChunks: chunks.length, conversationId },
            'chunk_post_failed'
          );
          // Wrap error with context about partial delivery
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

  /**
   * Post a single comment to a Gitea issue or PR.
   * Uses issues endpoint for both issues and PRs (Gitea API behavior).
   * Includes retry logic with exponential backoff (3 attempts max).
   * Throws on failure after exhausting retries so caller can handle appropriately.
   */
  private async postComment(
    parsed: { owner: string; repo: string; number: number; isPR: boolean },
    message: string
  ): Promise<void> {
    const markedMessage = `${message}\n\n${BOT_RESPONSE_MARKER}`;
    const maxRetries = 3;
    const conversationId = this.buildConversationId(
      parsed.owner,
      parsed.repo,
      parsed.number,
      parsed.isPR
    );

    // Gitea uses issues endpoint for PR comments too
    const url = `${this.baseUrl}/api/v1/repos/${parsed.owner}/${parsed.repo}/issues/${String(parsed.number)}/comments`;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const response = await fetch(url, {
          method: 'POST',
          headers: {
            Authorization: `token ${this.token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ body: markedMessage }),
        });

        if (!response.ok) {
          const body = await response.text();
          throw new Error(
            `Gitea API error: ${String(response.status)} ${response.statusText} - ${body}`
          );
        }

        getLog().debug({ conversationId }, 'comment_posted');
        return;
      } catch (error) {
        const isRetryable = this.isRetryableError(error);
        if (attempt < maxRetries && isRetryable) {
          const delay = this.retryDelayFn(attempt);
          getLog().warn(
            { attempt, maxRetries, conversationId, delayMs: delay },
            'comment_post_retry'
          );
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        // Log with full context for debugging
        getLog().error(
          {
            err: error,
            conversationId,
            attempt,
            maxRetries,
            wasRetryable: isRetryable,
            messageLength: message.length,
          },
          'comment_post_failed'
        );
        // Re-throw so caller can handle (e.g., notify user, stop chunk loop)
        throw error;
      }
    }
  }

  /**
   * Get streaming mode (always batch for Gitea to avoid comment spam)
   */
  getStreamingMode(): 'batch' {
    return 'batch';
  }

  /**
   * Get platform type
   */
  getPlatformType(): string {
    return 'gitea';
  }

  /**
   * Start the adapter (no-op for webhook-based adapter)
   */
  async start(): Promise<void> {
    getLog().info('webhook_adapter_ready');
  }

  /**
   * Stop the adapter (no-op for webhook-based adapter)
   */
  stop(): void {
    getLog().info('adapter_stopped');
  }

  /**
   * Ensure responses go to a thread.
   * Gitea issues/PRs are inherently threaded - all comments go to the issue.
   * Returns original conversation ID unchanged.
   */
  async ensureThread(originalConversationId: string, _messageContext?: unknown): Promise<string> {
    return originalConversationId;
  }

  /**
   * Verify webhook signature using HMAC SHA-256
   * Gitea uses X-Gitea-Signature header with raw hex (no sha256= prefix)
   */
  private verifySignature(payload: string, signature: string): boolean {
    try {
      const hmac = createHmac('sha256', this.webhookSecret);
      const digest = hmac.update(payload).digest('hex');

      const digestBuffer = Buffer.from(digest);
      const signatureBuffer = Buffer.from(signature);

      if (digestBuffer.length !== signatureBuffer.length) {
        getLog().error(
          { receivedLength: signatureBuffer.length, computedLength: digestBuffer.length },
          'signature_length_mismatch'
        );
        return false;
      }

      const isValid = timingSafeEqual(digestBuffer, signatureBuffer);

      if (!isValid) {
        getLog().error(
          {
            receivedPrefix: signature.substring(0, 15) + '...',
            computedPrefix: digest.substring(0, 15) + '...',
          },
          'signature_mismatch'
        );
      }

      return isValid;
    } catch (error) {
      getLog().error({ err: error }, 'signature_verification_error');
      return false;
    }
  }

  /**
   * Parse webhook event and extract relevant data
   *
   * Handles:
   * - issues.closed / pull_request.closed → cleanup (isCloseEvent: true)
   * - issue_comment.created → bot @mention detection
   * - pull_request_comment.created → bot @mention detection on PR review comments
   *
   * Does NOT handle:
   * - issues.opened / pull_request.opened → returns null (descriptions are not commands)
   */
  private parseEvent(event: WebhookEvent): GiteaParsedEvent | null {
    const owner = event.repository.owner.login;
    const repo = event.repository.name;

    // Detect issue closed
    if (event.issue && event.action === 'closed' && !event.issue.pull_request) {
      return {
        owner,
        repo,
        number: event.issue.number,
        comment: '',
        eventType: 'issue',
        isPR: false,
        issue: event.issue,
        isCloseEvent: true,
      };
    }

    // Detect PR merged/closed
    if (event.pull_request && event.action === 'closed') {
      return {
        owner,
        repo,
        number: event.pull_request.number,
        comment: '',
        eventType: 'pull_request',
        isPR: true,
        pullRequest: event.pull_request,
        isCloseEvent: true,
        isMerged: event.pull_request.merged === true,
      };
    }

    // issue_comment (covers both issues and PRs in Gitea)
    if (event.comment) {
      const number = event.issue?.number ?? event.pull_request?.number;
      if (!number) return null;

      // In Gitea, issue.pull_request is an object (not null) when comment is on a PR
      const isPR = !!event.issue?.pull_request || !!event.pull_request;

      return {
        owner,
        repo,
        number,
        comment: event.comment.body,
        eventType: 'issue_comment',
        isPR,
        issue: event.issue,
        pullRequest: event.pull_request,
      };
    }

    return null;
  }

  /**
   * Check if text contains @mention for the configured bot
   */
  private hasMention(text: string): boolean {
    const pattern = new RegExp(`@${this.botMention}[\\s,:;]`, 'i');
    return pattern.test(text) || text.trim().toLowerCase() === `@${this.botMention.toLowerCase()}`;
  }

  /**
   * Strip @mention from text for the configured bot
   */
  private stripMention(text: string): string {
    const pattern = new RegExp(`@${this.botMention}[\\s,:;]+`, 'gi');
    return text.replace(pattern, '').trim();
  }

  /**
   * Fetch comment history from issue or PR
   * Returns comments in chronological order (oldest first)
   */
  private async fetchCommentHistory(
    owner: string,
    repo: string,
    number: number
  ): Promise<string[]> {
    try {
      const url = `${this.baseUrl}/api/v1/repos/${owner}/${repo}/issues/${String(number)}/comments`;
      const response = await fetch(url, {
        headers: {
          Authorization: `token ${this.token}`,
        },
      });

      if (!response.ok) {
        throw new Error(`Gitea API error: ${String(response.status)}`);
      }

      const comments = (await response.json()) as {
        user?: { login: string } | null;
        body?: string | null;
      }[];

      // Gitea returns comments in chronological order by default
      // Take last 20 for context
      return comments.slice(-20).map(comment => {
        const author = comment.user?.login ?? 'unknown';
        const body = comment.body ?? '';
        return `${author}: ${body}`;
      });
    } catch (error) {
      getLog().error(
        { err: error, owner, repo, issueNumber: number },
        'comment_history_fetch_failed'
      );
      return [];
    }
  }

  /**
   * Build conversationId from owner, repo, number, and type
   * Uses # for issues, ! for PRs
   */
  private buildConversationId(owner: string, repo: string, number: number, isPR: boolean): string {
    const separator = isPR ? '!' : '#';
    return `${owner}/${repo}${separator}${String(number)}`;
  }

  /**
   * Parse conversationId into owner, repo, number, and isPR
   */
  private parseConversationId(
    conversationId: string
  ): { owner: string; repo: string; number: number; isPR: boolean } | null {
    // Try PR format first (!)
    const prRegex = /^([^/]+)\/([^!]+)!(\d+)$/;
    const prMatch = prRegex.exec(conversationId);
    if (prMatch) {
      return { owner: prMatch[1], repo: prMatch[2], number: parseInt(prMatch[3], 10), isPR: true };
    }

    // Try issue format (#)
    const issueRegex = /^([^/]+)\/([^#]+)#(\d+)$/;
    const issueMatch = issueRegex.exec(conversationId);
    if (issueMatch) {
      return {
        owner: issueMatch[1],
        repo: issueMatch[2],
        number: parseInt(issueMatch[3], 10),
        isPR: false,
      };
    }

    return null;
  }

  /**
   * Ensure repository is cloned and ready.
   * Uses @archon/git functions for safe, testable git operations.
   *
   * For new codebases: clone (directory won't exist)
   * For existing codebases: sync if shouldSync=true, skip if shouldSync=false
   *
   * @param shouldSync - Whether to sync if directory exists (pass true to ensure latest code)
   */
  private async ensureRepoReady(
    owner: string,
    repo: string,
    defaultBranch: string,
    repoPath: string,
    shouldSync: boolean
  ): Promise<void> {
    // Check if directory exists
    let directoryExists = false;
    try {
      await access(repoPath);
      directoryExists = true;
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code !== 'ENOENT') {
        // Real error - permission denied, I/O failure, etc.
        getLog().error({ repoPath, errorCode: err.code, err }, 'repo_path_access_failed');
        throw new Error(
          `Cannot access repository at ${repoPath}: ${err.code ?? err.message}. ` +
            'Check permissions and disk health.'
        );
      }
      // ENOENT means directory doesn't exist - we'll clone below
    }

    if (directoryExists) {
      if (shouldSync) {
        getLog().info({ repoPath, defaultBranch }, 'repo_syncing');
        const syncResult = await syncRepository(toRepoPath(repoPath), toBranchName(defaultBranch));
        if (!syncResult.ok) {
          getLog().error({ repoPath, defaultBranch }, 'repo_sync_failed');
          throw new Error(
            `Failed to sync repository to ${defaultBranch}. ` +
              'Try /reset or check if the branch exists.'
          );
        }
      }
      return;
    }

    // Directory doesn't exist - clone the repository
    getLog().info({ owner, repo, repoPath }, 'repo_cloning');

    // Create project structure (source/, worktrees/, artifacts/, logs/) before
    // cloning so worktree paths resolve correctly on first webhook clone.
    await ensureProjectStructure(owner, repo);

    // Parse URL to get host for authenticated clone
    const urlObj = new URL(this.baseUrl);
    const repoUrl = `${urlObj.protocol}//${urlObj.host}/${owner}/${repo}.git`;

    const cloneResult = await cloneRepository(repoUrl, toRepoPath(repoPath), {
      token: process.env.GITEA_TOKEN,
    });

    if (!cloneResult.ok) {
      getLog().error({ owner, repo, repoPath, error: cloneResult.error }, 'repo_clone_failed');

      if (cloneResult.error.code === 'not_a_repo') {
        throw new Error(
          `Repository ${owner}/${repo} not found or is private. Check repository access.`
        );
      }
      if (cloneResult.error.code === 'permission_denied') {
        throw new Error(
          `Authentication failed for ${owner}/${repo}. Check GITEA_TOKEN permissions.`
        );
      }
      const unknownMsg = (cloneResult.error as { message?: string }).message ?? 'unknown error';
      throw new Error(`Failed to clone ${owner}/${repo}: ${unknownMsg}`);
    }

    await addSafeDirectory(toRepoPath(repoPath));
  }

  /**
   * Auto-detect and load commands from .archon/commands/ (or configured folder)
   */
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
        getLog().info({ commandCount: files.length, folder }, 'commands_loaded');
        return;
      } catch (error) {
        const err = error as NodeJS.ErrnoException;
        // Folder not existing is expected - silently continue to next folder
        if (err.code === 'ENOENT') {
          continue;
        }
        // Log unexpected errors (database failures, permission issues) but don't fail setup
        getLog().error({ err, folder, errorCode: err.code }, 'commands_load_error');
        continue;
      }
    }
  }

  /**
   * Get or create codebase for repository
   * Returns: codebase record, path to use, and whether it's new
   * Always uses canonical path (not worktree paths) for codebase registration
   */
  private async getOrCreateCodebaseForRepo(
    owner: string,
    repo: string
  ): Promise<{
    codebase: { id: string; name: string; default_cwd: string };
    repoPath: string;
    isNew: boolean;
  }> {
    // Parse Gitea URL for repo URL storage
    const urlObj = new URL(this.baseUrl);
    const repoUrlNoGit = `${urlObj.protocol}//${urlObj.host}/${owner}/${repo}`;
    const repoUrlWithGit = `${repoUrlNoGit}.git`;

    let existing = await codebaseDb.findCodebaseByRepoUrl(repoUrlNoGit);
    existing ??= await codebaseDb.findCodebaseByRepoUrl(repoUrlWithGit);

    // Canonical path uses the project source/ subdirectory so that worktrees/,
    // artifacts/, and logs/ live as siblings of the cloned repo (not nested
    // inside it). Mirrors the CLI /clone path; see issue #1547.
    const canonicalPath = getProjectSourcePath(owner, repo);

    if (existing) {
      // Check if existing codebase points to a worktree path - fix it if so
      const looksLikeWorktreePath = existing.default_cwd.includes('/worktrees/');
      if (looksLikeWorktreePath || (await isWorktreePath(existing.default_cwd))) {
        getLog().info({ codebaseName: existing.name, canonicalPath }, 'stale_worktree_path_fixed');
        await codebaseDb.updateCodebase(existing.id, { default_cwd: canonicalPath });
        existing.default_cwd = canonicalPath;
      }

      getLog().info(
        { codebaseName: existing.name, path: existing.default_cwd },
        'existing_codebase_found'
      );
      return { codebase: existing, repoPath: existing.default_cwd, isNew: false };
    }

    // Include owner in name to distinguish repos with same name from different owners
    const codebase = await codebaseDb.createCodebase({
      name: `${owner}/${repo}`,
      repository_url: repoUrlNoGit,
      default_cwd: canonicalPath,
      ai_assistant_type: await resolveDefaultAssistant(canonicalPath),
    });

    getLog().info({ codebaseName: codebase.name, path: canonicalPath }, 'codebase_created');
    return { codebase, repoPath: canonicalPath, isNew: true };
  }

  /**
   * Clean up worktree when an issue/PR is closed
   * Delegates to cleanup service for unified handling
   */
  private async cleanupWorktree(
    owner: string,
    repo: string,
    number: number,
    isPR: boolean,
    merged = false
  ): Promise<void> {
    const conversationId = this.buildConversationId(owner, repo, number, isPR);
    getLog().info({ conversationId, merged }, 'isolation_cleanup_started');

    try {
      await onConversationClosed('gitea', conversationId, { merged });
      getLog().info({ conversationId }, 'isolation_cleanup_complete');
    } catch (error) {
      const err = error as Error;
      // Log full context for debugging - cleanup failures shouldn't break user flow
      getLog().error({ err, conversationId }, 'isolation_cleanup_failed');
    }
  }

  /**
   * Build context-rich message for issue.
   * Includes a hint to use the `tea` CLI for full issue details.
   */
  private buildIssueContext(issue: WebhookEvent['issue'], userComment: string): string {
    if (!issue) return userComment;
    const labels = issue.labels.map(l => l.name).join(', ');

    return `[Gitea Issue Context]
Issue #${String(issue.number)}: "${issue.title}"
Author: ${issue.user.login}
Labels: ${labels}
Status: ${issue.state}

Description:
${issue.body ?? ''}

---

${userComment}

Use 'tea issue view ${String(issue.number)}' for full details if needed.`;
  }

  /**
   * Build context-rich message for pull request.
   * Includes a hint to use the `tea` CLI for full PR details and diff.
   */
  private buildPRContext(pr: WebhookEvent['pull_request'], userComment: string): string {
    if (!pr) return userComment;
    const stats = pr.changed_files
      ? `Changed files: ${String(pr.changed_files)} (+${String(pr.additions ?? 0)}, -${String(pr.deletions ?? 0)})`
      : '';

    return `[Gitea Pull Request Context]
PR #${String(pr.number)}: "${pr.title}"
Author: ${pr.user.login}
Status: ${pr.state}
${stats}

Description:
${pr.body ?? ''}

---

${userComment}

Use 'tea pr view ${String(pr.number)}' for full details if needed.`;
  }

  /**
   * Handle incoming webhook event
   */

  private parseAuthorizedWebhook(payload: string, signature: string): WebhookEvent | undefined {
    if (!this.verifySignature(payload, signature)) {
      getLog().error(
        { signaturePrefix: signature?.substring(0, 15) + '...', payloadSize: payload.length },
        'invalid_webhook_signature'
      );
      return undefined;
    }

    const event = JSON.parse(payload) as WebhookEvent;
    const senderUsername = event.sender?.login;
    if (!isGiteaUserAuthorized(senderUsername, this.allowedUsers)) {
      const maskedUser = senderUsername ? `${senderUsername.slice(0, 3)}***` : 'unknown';
      getLog().info({ maskedUser }, 'unauthorized_webhook');
      return undefined;
    }
    return event;
  }

  private shouldIgnoreGiteaComment(event: WebhookEvent, comment: string): boolean {
    const commentBody = event.comment?.body ?? '';
    if (commentBody.includes(BOT_RESPONSE_MARKER)) {
      getLog().debug({ commentAuthor: event.comment?.user?.login }, 'ignoring_marked_comment');
      return true;
    }
    const commentAuthor = event.comment?.user?.login;
    if (commentAuthor?.toLowerCase() === this.botMention.toLowerCase()) {
      getLog().debug({ commentAuthor }, 'ignoring_own_comment');
      return true;
    }
    return !this.hasMention(comment);
  }

  private async resolveGiteaUserId(
    attributedLogin: string | undefined
  ): Promise<string | undefined> {
    if (!attributedLogin) return undefined;
    try {
      const user = await userDb.findOrCreateUserByPlatformIdentity(
        'gitea',
        attributedLogin,
        attributedLogin
      );
      return user.id;
    } catch (err) {
      getLog().warn(
        { err: toError(err), giteaLogin: attributedLogin },
        'gitea.user_resolve_failed'
      );
      return undefined;
    }
  }

  private async linkGiteaConversation(
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
        getLog().error({ conversationId, codebaseId }, 'conversation_codebase_link_failed');
        throw new Error('Failed to set up Gitea conversation - please try again');
      }
      throw updateError;
    }
  }

  private buildGiteaIsolationHints(parsed: GiteaParsedEvent): IsolationHints {
    const isolationHints: IsolationHints = {
      workflowType: parsed.isPR ? 'pr' : 'issue',
      workflowId: String(parsed.number),
    };
    if (parsed.isPR && parsed.pullRequest?.head) {
      isolationHints.prBranch = toBranchName(parsed.pullRequest.head.ref);
      isolationHints.prSha = parsed.pullRequest.head.sha;
      isolationHints.isForkPR =
        parsed.pullRequest.head.repo?.full_name !== parsed.pullRequest.base?.repo?.full_name;
      getLog().info(
        {
          prNumber: parsed.number,
          headRef: parsed.pullRequest.head.ref,
          headSha: parsed.pullRequest.head.sha?.substring(0, 7),
          isFork: isolationHints.isForkPR,
        },
        'pr_head_info'
      );
    }
    return isolationHints;
  }

  private buildGiteaMessage(parsed: GiteaParsedEvent): {
    finalMessage: string;
    contextToAppend?: string;
  } {
    const strippedComment = this.stripMention(parsed.comment);
    if (strippedComment.trim().startsWith('/')) {
      const finalMessage = strippedComment.split('\n')[0].trim();
      getLog().debug({ command: finalMessage }, 'slash_command_processing');
      return { finalMessage, contextToAppend: this.giteaReferenceContext(parsed) };
    }
    if (parsed.isPR && parsed.pullRequest) {
      return {
        finalMessage: this.buildPRContext(parsed.pullRequest, strippedComment),
        contextToAppend: this.giteaReferenceContext(parsed),
      };
    }
    if (parsed.issue) {
      return {
        finalMessage: this.buildIssueContext(parsed.issue, strippedComment),
        contextToAppend: this.giteaReferenceContext(parsed),
      };
    }
    return { finalMessage: strippedComment };
  }

  private giteaReferenceContext(parsed: GiteaParsedEvent): string | undefined {
    if (parsed.isPR && parsed.pullRequest) {
      return `Gitea Pull Request #${String(parsed.pullRequest.number)}: "${parsed.pullRequest.title}"
Use 'tea pr view ${String(parsed.pullRequest.number)}' for full details if needed.`;
    }
    if (parsed.issue) {
      return `Gitea Issue #${String(parsed.issue.number)}: "${parsed.issue.title}"
Use 'tea issue view ${String(parsed.issue.number)}' for full details if needed.`;
    }
    return undefined;
  }

  private async prepareGiteaMessage(
    event: WebhookEvent,
    parsed: GiteaParsedEvent,
    archonUserId: string | undefined
  ): Promise<GiteaWebhookMessage> {
    const conversationId = this.buildConversationId(
      parsed.owner,
      parsed.repo,
      parsed.number,
      parsed.isPR
    );
    const existingConv = await db.getOrCreateConversation('gitea', conversationId);
    const {
      codebase,
      repoPath,
      isNew: isNewCodebase,
    } = await this.getOrCreateCodebaseForRepo(parsed.owner, parsed.repo);
    await this.linkGiteaConversation(
      existingConv.id,
      codebase.id,
      repoPath,
      !existingConv.codebase_id
    );
    await this.ensureRepoReady(
      parsed.owner,
      parsed.repo,
      event.repository.default_branch,
      repoPath,
      isNewCodebase
    );
    if (isNewCodebase) await this.autoDetectAndLoadCommands(repoPath, codebase.id);

    const isolationHints = this.buildGiteaIsolationHints(parsed);
    const { finalMessage, contextToAppend } = this.buildGiteaMessage(parsed);
    return {
      conversationId,
      owner: parsed.owner,
      repo: parsed.repo,
      number: parsed.number,
      finalMessage,
      contextToAppend,
      isolationHints,
      archonUserId,
    };
  }

  private async dispatchGiteaMessage(message: GiteaWebhookMessage): Promise<void> {
    const commentHistory = await this.fetchCommentHistory(
      message.owner,
      message.repo,
      message.number
    );
    const threadContext = commentHistory.length > 0 ? commentHistory.join('\n') : undefined;
    getLog().debug(
      {
        commentCount: threadContext ? commentHistory.length : 0,
        conversationId: message.conversationId,
      },
      'thread_context_loaded'
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
        const err = toError(error);
        getLog().error({ err, conversationId: message.conversationId }, 'message_handling_error');
        try {
          await this.sendMessage(message.conversationId, classifyAndFormatError(err));
        } catch (sendError) {
          getLog().error(
            { err: toError(sendError), conversationId: message.conversationId },
            'error_message_send_failed'
          );
        }
      }
    });
  }

  async handleWebhook(payload: string, signature: string): Promise<void> {
    const event = this.parseAuthorizedWebhook(payload, signature);
    if (!event) return;

    const parsed = this.parseEvent(event);
    if (!parsed) return;

    if (parsed.isCloseEvent) {
      const mergeLabel = parsed.isMerged ? 'merge' : 'close';
      getLog().info(
        { event: mergeLabel, owner: parsed.owner, repo: parsed.repo, number: parsed.number },
        'close_event_received'
      );
      await this.cleanupWorktree(
        parsed.owner,
        parsed.repo,
        parsed.number,
        parsed.isPR,
        parsed.isMerged ?? false
      );
      return;
    }

    if (this.shouldIgnoreGiteaComment(event, parsed.comment)) return;
    getLog().info(
      {
        eventType: parsed.eventType,
        owner: parsed.owner,
        repo: parsed.repo,
        number: parsed.number,
        isPR: parsed.isPR,
      },
      'webhook_processing'
    );

    const attributedLogin = event.comment?.user?.login ?? event.sender?.login;
    const archonUserId = await this.resolveGiteaUserId(attributedLogin);
    const message = await this.prepareGiteaMessage(event, parsed, archonUserId);
    await this.dispatchGiteaMessage(message);
  }
}
