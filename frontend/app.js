/**
 * app.js — Real-Time Contest Leaderboard Dashboard  v2
 * ======================================================
 * Features:
 *   - Board selector: All Time / Daily / Weekly
 *   - Server-Sent Events with polling fallback
 *   - Around-me neighbors panel (shown after search)
 *   - Activity log pre-loaded from GET /activity + live via SSE
 *   - System metrics panel polled every 30 s
 *   - Rate-limit (429) and best-score (400) error display
 *   - Progress bars relative to MAX_SCORE (1000)
 */

'use strict';

// ── CONFIG ─────────────────────────────────────────────────────
const API_BASE         = 'https://app-image-latest-feve.onrender.com';
let currentMaxScore    = 1000;    // dynamically updated based on active contest
const POLL_FAST_MS     = 3000;    // polling interval when SSE is offline
const POLL_SLOW_MS     = 3000;   // polling interval when SSE is active
const HEALTH_CHECK_MS  = 15000;   // /health poll
const METRICS_POLL_MS  = 30000;   // /health/metrics poll
const SSE_RETRY_MS     = 5000;    // SSE reconnect delay after drop
const MAX_LOG_ITEMS    = 20;      // max entries shown in activity log

// ── STATE ───────────────────────────────────────────────────────
let currentContest     = 'global';
let currentBoard       = 'all';
let currentLeaderboard = [];
let countdownValue     = POLL_FAST_MS / 1000;
let countdownTimer     = null;
let refreshTimer       = null;
let isFirstLoad        = true;
let isRefreshing       = false;
let sseSource          = null;
let sseActive          = false;

// ── DOM REFS ────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

const dom = {
  // Header
  contestSelector:   $('contestSelector'),
  contestStatusBadge:$('contestStatusBadge'),
  openNewContestBtn: $('openNewContestBtn'),
  connectionStatus:  $('connectionStatus'),
  statusText:        $('statusText'),
  refreshCountdown:  $('refreshCountdown'),

  // Modal
  newContestModal:   $('newContestModal'),
  closeModalBtn:     $('closeModalBtn'),
  cancelModalBtn:    $('cancelModalBtn'),
  newContestForm:    $('newContestForm'),
  ncName:            $('ncName'),
  ncId:              $('ncId'),
  ncExpectedUsers:   $('ncExpectedUsers'),
  ncMaxScore:        $('ncMaxScore'),
  ncStart:           $('ncStart'),
  ncEnd:             $('ncEnd'),
  ncDesc:            $('ncDesc'),
  ncError:           $('ncError'),
  submitContestBtn:  $('submitContestBtn'),

  // Stats row
  totalParticipants: $('totalParticipants'),
  topScore:          $('topScore'),
  leaderName:        $('leaderName'),
  lastUpdated:       $('lastUpdated'),

  // Leaderboard
  leaderboardHeadingText:  $('leaderboardHeadingText'),
  leaderboardSkeleton:     $('leaderboardSkeleton'),
  leaderboardError:        $('leaderboardError'),
  leaderboardErrorMsg:     $('leaderboardErrorMsg'),
  leaderboardEmpty:        $('leaderboardEmpty'),
  leaderboardTableWrapper: $('leaderboardTableWrapper'),
  leaderboardBody:         $('leaderboardBody'),
  podiumSection:           $('podiumSection'),
  podiumCards:             $('podiumCards'),
  manualRefreshBtn:        $('manualRefreshBtn'),
  refreshIcon:             $('refreshIcon'),
  retryBtn:                $('retryBtn'),

  // Submit
  scoreForm:           $('scoreForm'),
  participantIdInput:  $('participantIdInput'),
  scoreInput:          $('scoreInput'),
  submitBtn:           $('submitBtn'),
  submitFeedback:      $('submitFeedback'),
  submitFeedbackIcon:  $('submitFeedbackIcon'),
  submitFeedbackTitle: $('submitFeedbackTitle'),
  submitFeedbackBody:  $('submitFeedbackBody'),

  // Search
  searchForm:      $('searchForm'),
  searchInput:     $('searchInput'),
  searchBtn:       $('searchBtn'),
  searchResult:    $('searchResult'),
  resultRankBadge: $('resultRankBadge'),
  resultId:        $('resultId'),
  resultScore:     $('resultScore'),
  searchError:     $('searchError'),
  searchErrorMsg:  $('searchErrorMsg'),

  // Neighbors
  neighborsSection:  $('neighborsSection'),
  neighborsTargetId: $('neighborsTargetId'),
  neighborsBody:     $('neighborsBody'),

  // Notifications
  notificationsList:  $('notificationsList'),
  notificationsEmpty: $('notificationsEmpty'),

  // Activity log
  activityList:  $('activityList'),
  activityEmpty: $('activityEmpty'),
  clearLogBtn:   $('clearLogBtn'),

  // Metrics
  metricTotal:      $('metricTotal'),
  metricAccepted:   $('metricAccepted'),
  metricRejected:   $('metricRejected'),
  metricBoardSize:  $('metricBoardSize'),
  metricSseClients: $('metricSseClients'),
  metricIngestTotal:$('metricIngestTotal'),
  metricIngestAcc:  $('metricIngestAcc'),
  metricIngestDup:  $('metricIngestDup'),
  metricIngestRej:  $('metricIngestRej'),
};

// ── GENERAL UTILITIES ────────────────────────────────────────────

function escapeHTML(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Human-readable relative timestamp ("2m ago", "just now").
 * @param {string} isoString
 */
function timeAgo(isoString) {
  const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000);
  if (diff < 5)    return 'just now';
  if (diff < 60)   return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function formatTime(date = new Date()) {
  return date.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

/** Derive avatar initials from a participant ID. */
function getInitials(id) {
  if (!id) return '?';
  const parts = id.replace(/[-_]/g, ' ').split(' ').filter(Boolean);
  return parts.length >= 2
    ? (parts[0][0] + parts[1][0]).toUpperCase()
    : id.slice(0, 2).toUpperCase();
}

/** Rank metadata: medal emoji + CSS class. */
function rankMeta(rank) {
  if (rank === 1) return { emoji: '🥇', cls: 'rank-1' };
  if (rank === 2) return { emoji: '🥈', cls: 'rank-2' };
  if (rank === 3) return { emoji: '🥉', cls: 'rank-3' };
  return { emoji: null, cls: '' };
}

/** Deterministic hue from a participant ID string. */
function idToHue(id) {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) & 0xffff;
  return h % 360;
}

// ── API FETCH ────────────────────────────────────────────────────

let currentSseRetryDelay = SSE_RETRY_MS || 2000;

/**
 * Thin fetch wrapper with AbortController timeout.
 * Returns { ok, data } — data is the parsed JSON or error detail string.
 */
async function apiFetch(path, opts = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);
  opts.signal = controller.signal;

  try {
    const res  = await fetch(`${API_BASE}${path}`, opts);
    clearTimeout(timeoutId);
    
    const data = await res.json().catch(() => ({}));
    if (res.ok) return { ok: true, data };
    const msg  = data.detail
      ? (Array.isArray(data.detail) ? data.detail.map(e => e.msg).join(' | ') : data.detail)
      : `HTTP ${res.status}`;
    return { ok: false, data: msg };
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === 'AbortError') return { ok: false, data: 'Request timed out' };
    return { ok: false, data: 'Cannot reach server' };
  }
}

// ── CONNECTION STATUS ────────────────────────────────────────────

function setLivePush() {
  dom.connectionStatus.className = 'status-badge connected';
  dom.statusText.textContent = 'Live Push';
}

function setPolling() {
  dom.connectionStatus.className = 'status-badge polling';
  dom.statusText.textContent = 'Polling';
}

function setConnecting() {
  dom.connectionStatus.className = 'status-badge';
  dom.statusText.textContent = 'Connecting…';
}

function setOffline() {
  dom.connectionStatus.className = 'status-badge error';
  dom.statusText.textContent = 'Offline';
}

// ── SSE CONNECTION ───────────────────────────────────────────────

function connectSSE() {
  if (sseSource) {
    sseSource.close();
    sseSource = null;
  }

  sseSource = new EventSource(`${API_BASE}/events`);

  sseSource.onopen = () => {
    sseActive = true;
    currentSseRetryDelay = SSE_RETRY_MS || 2000; // reset on success
    setLivePush();
    // Back off polling interval — SSE is live
    restartAutoRefresh(POLL_SLOW_MS);
  };

  sseSource.onmessage = event => {
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }

    if (msg.type === 'score_update') {
      if (msg.contestId !== currentContest) return;

      // Use SSE as a trigger: pull fresh state from the API
      fetchLeaderboard();
      fetchMetrics();

      // Prepend to local activity log immediately (no need to re-fetch)
      prependActivityItem({
        participantId: msg.participantId,
        previousScore: msg.previousScore,
        newScore:      msg.score,
        previousRank:  msg.previousRank,
        newRank:       msg.rank,
        rankMovement:  msg.rankMovement,
        delta:         msg.previousScore != null ? msg.score - msg.previousScore : null,
        board:         msg.board,
        timestamp:     msg.timestamp,
        source:        msg.source
      });
    } else if (msg.type === 'notification') {
      prependNotificationItem(msg.data);
    }
    // heartbeat — no action needed
  };

  sseSource.onerror = () => {
    sseActive = false;
    sseSource.close();
    sseSource = null;
    setConnecting(); // Show connecting state during backoff
    
    // Fallback to fast polling
    restartAutoRefresh(POLL_FAST_MS);
    
    // Exponential backoff up to ~30s
    setTimeout(connectSSE, currentSseRetryDelay);
    currentSseRetryDelay = Math.min(currentSseRetryDelay * 1.5, 30000);
  };
}

// ── HEALTH CHECK ─────────────────────────────────────────────────

async function pollHealth() {
  try {
    const res  = await fetch(`${API_BASE}/health`, { signal: AbortSignal.timeout(30000) });
    const data = await res.json();

    if (data.status === 'ok') {
      if (!sseActive) setPolling();
    } else {
      // API alive but Redis degraded
      if (!sseActive) setPolling();
      logActivity(`Health: Redis degraded — ${data.redis}`, 'error');
    }
  } catch {
    setOffline();
  }
}

function startHealthPolling() {
  pollHealth();
  setInterval(pollHealth, HEALTH_CHECK_MS);
}

// ── ACTIVITY LOG (local + API-seeded) ────────────────────────────

let logCount = 0;

function logActivity(msg, type = 'info') {
  /** Append a plain text system message to the log. */
  const li = document.createElement('li');
  li.className = `activity-item activity-${type}`;
  li.innerHTML = `
    <span class="activity-time">${formatTime()}</span>
    <span class="activity-msg">${escapeHTML(msg)}</span>`;
  _insertLogItem(li);
}

// ── NOTIFICATIONS ────────────────────────────────────────────────

function prependNotificationItem(notif) {
  const { contestName, type, message, createdAt } = notif;
  const timeStr = timeAgo(createdAt);
  
  const li = document.createElement('li');
  li.className = 'notification-item';
  li.innerHTML = `
    <div class="notification-header">
      <span class="notification-badge ${escapeHTML(type)}">${escapeHTML(type)}</span>
      <span class="notification-time">${timeStr}</span>
    </div>
    <div class="notification-msg">${escapeHTML(message)}</div>
  `;
  
  const empty = dom.notificationsEmpty;
  if (empty && !empty.classList.contains('hidden')) empty.classList.add('hidden');
  
  dom.notificationsList.insertBefore(li, dom.notificationsList.firstChild);
  
  const items = dom.notificationsList.querySelectorAll('.notification-item');
  if (items.length > 10) items[items.length - 1].remove();
}

function clearNotifications() {
  dom.notificationsList.innerHTML = '<li class="notifications-empty" id="notificationsEmpty">No recent notifications</li>';
  dom.notificationsEmpty = $('notificationsEmpty');
}

async function fetchNotifications() {
  const { ok, data } = await apiFetch('/notifications');
  if (!ok || !Array.isArray(data) || data.length === 0) {
    clearNotifications();
    return;
  }
  clearNotifications();
  for (const notif of [...data].reverse()) {
    prependNotificationItem(notif);
  }
}

function prependActivityItem(entry) {
  /**
   * Prepend a structured score-change entry to the log.
   * Used for SSE events and API-seeded initial history.
   */
  const { participantId, previousScore, newScore, previousRank, newRank, rankMovement, board, timestamp, source } = entry;
  const hue  = idToHue(participantId);
  const ago  = timeAgo(timestamp);

  let movementHtml = '';
  if (previousRank == null) {
    movementHtml = `<span class="rank-movement move-new">entered at rank ${newRank}</span>`;
  } else if (rankMovement > 0) {
    movementHtml = `<span class="rank-movement move-up">moved up ${rankMovement} places (${previousRank} &rarr; ${newRank})</span>`;
  } else if (rankMovement < 0) {
    movementHtml = `<span class="rank-movement move-down">moved down ${Math.abs(rankMovement)} places (${previousRank} &rarr; ${newRank})</span>`;
  } else {
    movementHtml = `<span class="rank-movement move-none">held rank ${newRank}</span>`;
  }

  const sourceHtml = source 
    ? `<span class="source-badge source-${source}">${source}</span>` 
    : `<span class="source-badge source-admin">admin</span>`;

  const li = document.createElement('li');
  li.className = 'activity-item activity-score';
  li.innerHTML = `
    <span class="activity-avatar" style="--hue:${hue}">${escapeHTML(getInitials(participantId))}</span>
    <span class="activity-body">
      <span class="activity-id mono">${escapeHTML(participantId)}</span>
      ${sourceHtml}
      scored <strong>${escapeHTML(String(newScore))}</strong>
      ${movementHtml}
    </span>
    <span class="activity-time">${ago}</span>`;

  _insertLogItem(li);
}

function _insertLogItem(li) {
  const empty = dom.activityEmpty;
  if (empty && !empty.classList.contains('hidden')) empty.classList.add('hidden');

  dom.activityList.insertBefore(li, dom.activityList.firstChild);
  logCount++;

  // Trim to MAX_LOG_ITEMS
  const items = dom.activityList.querySelectorAll('.activity-item');
  if (items.length > MAX_LOG_ITEMS) items[items.length - 1].remove();
}

function clearLog() {
  dom.activityList.innerHTML =
    '<li class="activity-empty" id="activityEmpty">No activity yet…</li>';
  dom.activityEmpty = $('activityEmpty');
  logCount = 0;
}

/** Pre-populate the log from GET /activity on page load. */
async function fetchActivity() {
  const { ok, data } = await apiFetch(`/contests/${currentContest}/activity?limit=15`);
  if (!ok || !Array.isArray(data) || data.length === 0) {
    clearLog();
    return;
  }
  // data is newest-first; insert in order so newest ends up at top
  for (const entry of data) prependActivityItem(entry);
  // re-sort by reversing what we just built (entries from API are already newest-first)
  // Actually insertBefore always puts at top, so the first entry processed
  // ends up at the bottom and the last at the top. We want newest at top.
  // data[0] is newest; after all insertions it should be at top. But each
  // insertBefore puts at top, so data[0] (inserted first) ends up at bottom.
  // Fix: reverse the array before inserting.
  // We already iterated — clear and redo in reverse order.
  clearLog();
  for (const entry of [...data].reverse()) prependActivityItem(entry);
}

// ── METRICS ──────────────────────────────────────────────────────

async function fetchMetrics() {
  const { ok, data } = await apiFetch(`/contests/${currentContest}/metrics`);
  if (!ok) return;
  dom.metricTotal.textContent      = data.totalSubmissions    ?? '—';
  dom.metricAccepted.textContent   = data.acceptedSubmissions ?? '—';
  dom.metricRejected.textContent   = data.rejectedSubmissions ?? '—';
  dom.metricBoardSize.textContent  = data.leaderboardSize     ?? '—';
  dom.metricSseClients.textContent = data.sseClients          ?? '—';
  if (dom.metricIngestTotal) dom.metricIngestTotal.textContent = data.total_ingested_events ?? '0';
  if (dom.metricIngestAcc) dom.metricIngestAcc.textContent   = data.accepted_ingested_events ?? '0';
  if (dom.metricIngestDup) dom.metricIngestDup.textContent   = data.duplicate_ingested_events ?? '0';
  if (dom.metricIngestRej) dom.metricIngestRej.textContent   = data.rejected_ingested_events ?? '0';
}

function startMetricsPolling() {
  fetchMetrics();
  setInterval(fetchMetrics, METRICS_POLL_MS);
}

// ── COUNTDOWN TIMER ───────────────────────────────────────────────

function updateCountdownDisplay(v) {
  if (dom.refreshCountdown) dom.refreshCountdown.textContent = `${v}s`;
}

function startCountdown(intervalMs = POLL_FAST_MS) {
  clearInterval(countdownTimer);
  countdownValue = intervalMs / 1000;
  updateCountdownDisplay(countdownValue);
  countdownTimer = setInterval(() => {
    countdownValue = Math.max(0, countdownValue - 1);
    updateCountdownDisplay(countdownValue);
  }, 1000);
}

// ── BOARD TABS ───────────────────────────────────────────────────

function initBoardTabs() {
  document.querySelectorAll('.board-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.dataset.board === currentBoard) return;

      // Update active state
      document.querySelectorAll('.board-tab').forEach(b => {
        b.classList.remove('active');
        b.setAttribute('aria-selected', 'false');
      });
      btn.classList.add('active');
      btn.setAttribute('aria-selected', 'true');

      currentBoard = btn.dataset.board;

      // Update heading
      const labels = { all: 'All Time', daily: 'Today', weekly: 'This Week' };
      dom.leaderboardHeadingText.textContent = `Top 10 — ${labels[currentBoard]}`;

      // Hide stale neighbors when switching boards
      dom.neighborsSection.classList.add('hidden');

      showSkeleton();
      fetchLeaderboard();
    });
  });
}

// ── CONTEST SELECTOR ─────────────────────────────────────────────

async function fetchContests() {
  const { ok, data } = await apiFetch('/contests');
  if (!ok || !Array.isArray(data)) return;

  dom.contestSelector.innerHTML = '';
  data.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.contestId;
    opt.textContent = c.name;
    opt.dataset.status = c.status;
    opt.dataset.maxscore = c.maxScore || 1000;
    if (c.contestId === currentContest) {
      opt.selected = true;
      updateContestBadge(c.status);
      currentMaxScore = c.maxScore || 1000;
    }
    dom.contestSelector.appendChild(opt);
  });

  dom.contestSelector.addEventListener('change', async (e) => {
    currentContest = e.target.value;
    const selectedOpt = e.target.options[e.target.selectedIndex];
    updateContestBadge(selectedOpt.dataset.status);
    currentMaxScore = parseInt(selectedOpt.dataset.maxscore, 10) || 1000;

    // Reset UI
    clearLog();
    dom.neighborsSection.classList.add('hidden');
    dom.searchResult.classList.add('hidden');
    dom.searchError.classList.add('hidden');
    showSkeleton();

    // Re-fetch all data
    await fetchLeaderboard();
    await fetchActivity();
    await fetchMetrics();
  });
}

function updateContestBadge(status) {
  if (!status) return;
  dom.contestStatusBadge.className = `contest-status-badge status-${status}`;
  dom.contestStatusBadge.textContent = status;

  if (status !== 'live') {
    dom.participantIdInput.disabled = true;
    dom.scoreInput.disabled = true;
    dom.submitBtn.disabled = true;
    dom.submitBtn.querySelector('.btn-text').textContent = 'Contest Not Live';
  } else {
    dom.participantIdInput.disabled = false;
    dom.scoreInput.disabled = false;
    dom.submitBtn.disabled = false;
    dom.submitBtn.querySelector('.btn-text').textContent = 'Submit Score';
  }
}

// ── LEADERBOARD RENDERING ─────────────────────────────────────────

function showSkeleton() {
  dom.leaderboardSkeleton.classList.remove('hidden');
  dom.leaderboardError.classList.add('hidden');
  dom.leaderboardEmpty.classList.add('hidden');
  dom.leaderboardTableWrapper.classList.add('hidden');
  if (dom.podiumSection) dom.podiumSection.classList.add('hidden');
}

function showTable() {
  dom.leaderboardSkeleton.classList.add('hidden');
  dom.leaderboardError.classList.add('hidden');
  dom.leaderboardEmpty.classList.add('hidden');
  dom.leaderboardTableWrapper.classList.remove('hidden');
  if (dom.podiumSection && currentLeaderboard.length > 0) {
    dom.podiumSection.classList.remove('hidden');
  }
}

function showEmpty() {
  dom.leaderboardSkeleton.classList.add('hidden');
  dom.leaderboardError.classList.add('hidden');
  dom.leaderboardEmpty.classList.remove('hidden');
  dom.leaderboardTableWrapper.classList.add('hidden');
  if (dom.podiumSection) dom.podiumSection.classList.add('hidden');
}

function showError(msg) {
  dom.leaderboardSkeleton.classList.add('hidden');
  dom.leaderboardError.classList.remove('hidden');
  dom.leaderboardEmpty.classList.add('hidden');
  dom.leaderboardTableWrapper.classList.add('hidden');
  if (dom.podiumSection) dom.podiumSection.classList.add('hidden');
  if (dom.leaderboardErrorMsg) dom.leaderboardErrorMsg.textContent = msg;
}

function buildRow(entry, prevMap) {
  const { rank, participantId: id, score, timestamp, rankMovement } = entry;
  const meta = rankMeta(rank);
  const hue  = idToHue(id);
  const pct  = Math.min(100, Math.round((score / currentMaxScore) * 100));

  // Detect score change for flash animation
  const prev    = prevMap[id];
  const changed = prev !== undefined && prev !== score;

  const tr = document.createElement('tr');
  if (changed) tr.classList.add('row-flash');

  let moveHtml = '<span class="text-muted">—</span>';
  if (rankMovement > 0) moveHtml = `<span class="text-emerald">▲ ${rankMovement}</span>`;
  else if (rankMovement < 0) moveHtml = `<span class="text-rose">▼ ${Math.abs(rankMovement)}</span>`;

  let timeHtml = '<span class="text-muted">—</span>';
  if (timestamp) {
    const d = new Date(timestamp);
    timeHtml = d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  tr.innerHTML = `
    <td class="col-rank">
      <span class="rank-badge ${meta.cls}">
        ${meta.emoji ? meta.emoji : `<span class="rank-num">#${rank}</span>`}
      </span>
    </td>
    <td class="col-participant">
      <div class="participant-cell">
        <div class="avatar" style="--hue:${hue}">${escapeHTML(getInitials(id))}</div>
        <span class="participant-name mono">${escapeHTML(id)}</span>
      </div>
    </td>
    <td class="col-score">
      <span class="score-value">${score.toLocaleString()}</span>
    </td>
    <td class="col-movement">
      ${moveHtml}
    </td>
    <td class="col-time">
      ${timeHtml}
    </td>
    <td class="col-bar">
      <div class="progress-bar-bg">
        <div class="progress-bar-fill" style="width:${pct}%"></div>
      </div>
    </td>`;

  return tr;
}

function buildPodiumCard(entry, prevMap) {
  const { rank, participantId: id, score, timestamp, rankMovement } = entry;
  const hue = idToHue(id);
  const meta = rankMeta(rank);
  const pct = Math.min(100, Math.round((score / currentMaxScore) * 100));

  const div = document.createElement('div');
  div.className = `podium-card ${meta.cls}`;
  
  let moveText = '';
  if (rankMovement > 0) moveText = `▲ ${rankMovement}`;
  else if (rankMovement < 0) moveText = `▼ ${Math.abs(rankMovement)}`;

  let timeText = '—';
  if (timestamp) {
    const d = new Date(timestamp);
    timeText = d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  div.innerHTML = `
    <div class="podium-rank">${meta.emoji || '#' + rank}</div>
    <div class="podium-avatar" style="--hue:${hue}">${escapeHTML(getInitials(id))}</div>
    <div class="podium-id">${escapeHTML(id)}</div>
    <div class="podium-score">${score.toLocaleString()}</div>
    <div class="podium-meta">
      <span>${moveText ? `<span class="${rankMovement > 0 ? 'text-emerald' : 'text-rose'}">${moveText}</span>` : '-'}</span>
      <span>•</span>
      <span>${timeText}</span>
    </div>
    <div class="progress-bar-bg" style="width: 100%; margin-top: 16px; height: 4px;">
      <div class="progress-bar-fill" style="width:${pct}%; height: 100%;"></div>
    </div>
  `;
  return div;
}

function patchTable(newData) {
  // Build previous-score map for flash detection
  const prevMap = {};
  currentLeaderboard.forEach(e => { prevMap[e.participantId] = e.score; });

  const tbody = dom.leaderboardBody;
  const cards = dom.podiumCards;
  const fragTable = document.createDocumentFragment();
  const fragCards = document.createDocumentFragment();
  
  const top3 = newData.slice(0, 3);
  const rest = newData.slice(3);

  top3.forEach(entry => fragCards.appendChild(buildPodiumCard(entry, prevMap)));
  rest.forEach(entry => fragTable.appendChild(buildRow(entry, prevMap)));
  
  if (cards) cards.replaceChildren(fragCards);
  tbody.replaceChildren(fragTable);

  currentLeaderboard = newData;
  showTable();
}

function updateStats(data) {
  if (!data.length) return;
  dom.totalParticipants.textContent = data.length;
  dom.topScore.textContent          = data[0].score.toLocaleString();
  dom.leaderName.textContent        = data[0].participantId;
  dom.lastUpdated.textContent       = formatTime();
}

async function fetchLeaderboard() {
  if (isRefreshing) return;
  isRefreshing = true;

  try {
    dom.refreshIcon.classList.add('spin');
    const { ok, data } = await apiFetch(`/contests/${currentContest}/leaderboard?board=${currentBoard}&limit=100`);

    if (!ok) {
      if (isFirstLoad) showError(data);
      setOffline();
      logActivity(`Leaderboard fetch failed: ${data}`, 'error');
      return;
    }

    if (!sseActive) setPolling();
    isFirstLoad = false;

    if (data.length === 0) {
      showEmpty();
      return;
    }

    patchTable(data);
    updateStats(data);
    startCountdown(sseActive ? POLL_SLOW_MS : POLL_FAST_MS);

  } finally {
    isRefreshing = false;
    dom.refreshIcon.classList.remove('spin');
  }
}

// ── AUTO-REFRESH ─────────────────────────────────────────────────

function restartAutoRefresh(intervalMs) {
  clearInterval(refreshTimer);
  refreshTimer = setInterval(fetchLeaderboard, intervalMs);
  startCountdown(intervalMs);
}

function startAutoRefresh() {
  restartAutoRefresh(POLL_FAST_MS);
}

// ── SUBMIT SCORE ──────────────────────────────────────────────────

function showSubmitFeedback(type, title, body) {
  /** type: 'success' | 'error' | 'warning' */
  const icons = { success: '✅', error: '❌', warning: '⚠️' };
  dom.submitFeedback.className = `feedback feedback-${type}`;
  dom.submitFeedbackIcon.textContent  = icons[type] || '•';
  dom.submitFeedbackTitle.textContent = title;
  dom.submitFeedbackBody.textContent  = body;
  dom.submitFeedback.classList.remove('hidden');
  setTimeout(() => dom.submitFeedback.classList.add('hidden'), 5000);
}

async function handleScoreSubmit(e) {
  e.preventDefault();

  const participantId = dom.participantIdInput.value.trim();
  const scoreRaw      = dom.scoreInput.value.trim();
  const score         = parseInt(scoreRaw, 10);

  // Client-side guard
  if (!participantId) {
    showSubmitFeedback('error', 'Validation Error', 'Participant ID is required.');
    return;
  }
  if (isNaN(score) || score < 0) {
    showSubmitFeedback('error', 'Validation Error', 'Score must be a non-negative integer.');
    return;
  }
  if (score > currentMaxScore) {
    showSubmitFeedback('error', 'Validation Error', `Score cannot exceed ${currentMaxScore}.`);
    return;
  }

  dom.submitBtn.disabled = true;
  dom.submitBtn.querySelector('.btn-text').classList.add('hidden');
  dom.submitBtn.querySelector('.btn-spinner').classList.remove('hidden');

  const { ok, data } = await apiFetch(`/contests/${currentContest}/score`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ participantId, score }),
  });

  dom.submitBtn.disabled = false;
  dom.submitBtn.querySelector('.btn-text').classList.remove('hidden');
  dom.submitBtn.querySelector('.btn-spinner').classList.add('hidden');

  if (ok) {
    showSubmitFeedback(
      'success',
      'Score submitted!',
      `${data.participantId} → ${data.score} pts · Rank #${data.rank}`
    );
    dom.scoreForm.reset();
    logActivity(`Submitted: ${participantId} = ${score} pts (Rank #${data.rank})`, 'success');

    // Trigger immediate refresh only if SSE is down
    if (!sseActive) fetchLeaderboard();
  } else {
    const isRateLimit = typeof data === 'string' && data.includes('Rate limit');
    showSubmitFeedback('error', isRateLimit ? 'Too Many Requests' : 'Submission Failed', data);
    logActivity(`Failed: ${participantId} — ${data}`, 'error');
  }
}

// ── SEARCH + NEIGHBORS ────────────────────────────────────────────

async function handleSearch(e) {
  e.preventDefault();

  const id = dom.searchInput.value.trim().toLowerCase();
  if (!id) return;

  dom.searchBtn.disabled = true;
  dom.searchBtn.querySelector('.btn-text').classList.add('hidden');
  dom.searchBtn.querySelector('.btn-spinner').classList.remove('hidden');

  // Hide previous results
  dom.searchResult.classList.add('hidden');
  dom.searchError.classList.add('hidden');
  dom.neighborsSection.classList.add('hidden');

  const { ok, data } = await apiFetch(`/contests/${currentContest}/rank/${encodeURIComponent(id)}?board=${currentBoard}`);

  dom.searchBtn.disabled = false;
  dom.searchBtn.querySelector('.btn-text').classList.remove('hidden');
  dom.searchBtn.querySelector('.btn-spinner').classList.add('hidden');

  if (ok) {
    const meta = rankMeta(data.rank);
    dom.resultRankBadge.className  = `result-rank-badge ${meta.cls}`;
    dom.resultRankBadge.textContent = meta.emoji ? meta.emoji : `#${data.rank}`;
    dom.resultId.textContent        = data.participantId;
    dom.resultScore.textContent     = `${data.score.toLocaleString()} pts`;
    
    const pb = $('resultPercentile');
    if (pb) {
      if (data.percentile !== undefined) {
        pb.textContent = `Top ${data.percentile}%`;
        pb.classList.remove('hidden');
      } else {
        pb.classList.add('hidden');
      }
    }

    dom.searchResult.classList.remove('hidden');

    // Also fetch neighbors
    fetchNeighbors(id);
  } else {
    dom.searchErrorMsg.textContent = data;
    dom.searchError.classList.remove('hidden');
  }
}

async function fetchNeighbors(participantId) {
  const { ok, data } = await apiFetch(
    `/contests/${currentContest}/participants/${encodeURIComponent(participantId)}/neighbors?window=3&board=${currentBoard}`
  );
  if (!ok || !data.neighbors || data.neighbors.length === 0) return;
  renderNeighbors(data);
}

function renderNeighbors(data) {
  dom.neighborsTargetId.textContent = data.participantId;
  dom.neighborsBody.innerHTML = '';

  data.neighbors.forEach(({ rank, participantId, score, relation }) => {
    const tr = document.createElement('tr');
    tr.className =
      relation === 'self'  ? 'self-row'  :
      relation === 'above' ? 'above-row' :
      'below-row';

    const percentHtml = (relation === 'self' && data.percentile !== undefined) 
      ? `<br><span class="percentile-cell">Top ${data.percentile}%</span>` 
      : '';

    tr.innerHTML = `
      <td class="neighbor-rank">#${rank}</td>
      <td class="neighbor-id">${escapeHTML(participantId)}</td>
      <td class="neighbor-score">${score.toLocaleString()}${percentHtml}</td>`;

    dom.neighborsBody.appendChild(tr);
  });

  dom.neighborsSection.classList.remove('hidden');
}

// ── MANUAL REFRESH ────────────────────────────────────────────────

async function handleManualRefresh() {
  dom.refreshIcon.classList.add('spin');
  await fetchLeaderboard();
  startCountdown(sseActive ? POLL_SLOW_MS : POLL_FAST_MS);
}

// ── CONTEST CREATION ──────────────────────────────────────────────

async function handleNewContestSubmit(e) {
  e.preventDefault();
  
  const payload = {
    contestId: dom.ncId.value.trim(),
    name: dom.ncName.value.trim(),
    expectedUsers: parseInt(dom.ncExpectedUsers.value, 10),
    maxScore: parseInt(dom.ncMaxScore.value, 10),
    startTime: new Date(dom.ncStart.value).toISOString(),
    endTime: new Date(dom.ncEnd.value).toISOString(),
    description: dom.ncDesc.value.trim()
  };

  dom.ncError.classList.add('hidden');
  dom.submitContestBtn.disabled = true;
  dom.submitContestBtn.textContent = 'Creating...';

  const { ok, data } = await apiFetch('/contests', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  dom.submitContestBtn.disabled = false;
  dom.submitContestBtn.textContent = 'Create Contest';

  if (ok) {
    dom.newContestModal.close();
    dom.newContestForm.reset();
    
    await fetchContests();
    dom.contestSelector.value = payload.contestId;
    dom.contestSelector.dispatchEvent(new Event('change'));
    
    showSubmitFeedback('success', 'Contest Created', `${payload.name} is now active.`);
  } else {
    let msg = 'Failed to create contest';
    if (typeof data === 'string') {
      msg = data;
    } else if (data && data.detail) {
      msg = Array.isArray(data.detail) ? data.detail.map(e => e.msg || e).join(', ') : data.detail;
    }
    dom.ncError.textContent = msg;
    dom.ncError.classList.remove('hidden');
  }
}

// ── INIT ──────────────────────────────────────────────────────────

async function init() {
  setConnecting();

  // Wire board tabs
  initBoardTabs();

  // Wire form events
  dom.scoreForm.addEventListener('submit', handleScoreSubmit);
  dom.searchForm.addEventListener('submit', handleSearch);
  dom.manualRefreshBtn.addEventListener('click', handleManualRefresh);
  dom.retryBtn.addEventListener('click', handleManualRefresh);
  dom.clearLogBtn.addEventListener('click', clearLog);

  // Modal events
  dom.openNewContestBtn.addEventListener('click', () => dom.newContestModal.showModal());
  dom.closeModalBtn.addEventListener('click', () => dom.newContestModal.close());
  dom.cancelModalBtn.addEventListener('click', () => dom.newContestModal.close());
  
  dom.ncName.addEventListener('input', () => {
    if (dom.ncName.value) {
      dom.ncId.value = dom.ncName.value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
    }
  });

  dom.newContestForm.addEventListener('submit', handleNewContestSubmit);

  // Initial data load
  await fetchContests();
  showSkeleton();
  await fetchLeaderboard();

  // Pre-populate activity log and notifications from server history
  await fetchActivity();
  await fetchNotifications();

  // Connect SSE (primary real-time channel)
  connectSSE();

  // Background polling cycles
  startAutoRefresh();
  startHealthPolling();
  startMetricsPolling();

  // Re-fetch on tab focus
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      fetchLeaderboard();
      pollHealth();
      fetchMetrics();
    }
  });
}

// ── BOOT ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
