// =========================================================
// LANDING PAGE — landing.js
// Scroll reveal, mock animation engine, and live contest fetching
// =========================================================

document.addEventListener('DOMContentLoaded', () => {
  initScrollReveal();
  initMockEngine();
  initContests();
});

// ── SCROLL REVEAL ─────────────────────────────────────────
function initScrollReveal() {
  const reveals = document.querySelectorAll('.reveal');
  
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('visible');
        observer.unobserve(entry.target);
      }
    });
  }, {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  });

  reveals.forEach(el => observer.observe(el));
}

// ── MOCK ENGINE ───────────────────────────────────────────
const MOCK_PARTICIPANTS = [
  { rank: 1, name: "alice", score: 450, move: 1 },
  { rank: 2, name: "bob", score: 420, move: -1 },
  { rank: 3, name: "charlie", score: 395, move: 2 },
  { rank: 4, name: "dave", score: 380, move: 0 },
  { rank: 5, name: "eve", score: 365, move: 0 }
];

function initMockEngine() {
  const container = document.getElementById('mockRows');
  if (!container) return;

  let state = [...MOCK_PARTICIPANTS];

  function render(rows, flashId = null) {
    container.innerHTML = '';
    rows.forEach(p => {
      const div = document.createElement('div');
      div.className = `mock-row ${p.name === flashId ? 'flash' : ''}`;
      
      let moveHtml = `<span class="m-move text-muted">—</span>`;
      if (p.move > 0) moveHtml = `<span class="m-move up">↑ ${p.move}</span>`;
      else if (p.move < 0) moveHtml = `<span class="m-move down">↓ ${Math.abs(p.move)}</span>`;

      div.innerHTML = `
        <span class="m-rank">#${p.rank}</span>
        <span class="m-name">${p.name}</span>
        <span class="m-score">${p.score}</span>
        ${moveHtml}
      `;
      container.appendChild(div);
    });

    if (flashId) {
      setTimeout(() => {
        const flashes = container.querySelectorAll('.flash');
        flashes.forEach(f => f.classList.remove('flash'));
      }, 800);
    }
  }

  render(state);

  setInterval(() => {
    const idx = Math.floor(Math.random() * state.length);
    const target = state[idx];
    target.score += Math.floor(Math.random() * 20) + 5;
    state.sort((a, b) => b.score - a.score);
    
    state.forEach((p, i) => {
      const newRank = i + 1;
      p.move = p.rank - newRank;
      p.rank = newRank;
    });

    render(state, target.name);
  }, 3500);
}


// ── CONTEST ENGINE ────────────────────────────────────────
async function initContests() {
  try {
    const res = await fetch('https://leaderstack.onrender.com/contests');
    const data = await res.json();
    if (res.ok && data.contests) {
      categorizeAndRenderContests(data.contests);
    }
  } catch (err) {
    console.error("Failed to load contests:", err);
  }
}

function categorizeAndRenderContests(contests) {
  const ongoing = [];
  const upcoming = [];
  const past = [];
  const now = new Date();

  contests.forEach(c => {
    const start = new Date(c.startTime);
    const end = new Date(c.endTime);

    if (now < start) {
      upcoming.push(c);
    } else if (now > end) {
      past.push(c);
    } else {
      ongoing.push(c);
    }
  });

  renderGrid('ongoingContestsGrid', ongoing, renderOngoingCard);
  renderGrid('upcomingContestsGrid', upcoming, renderUpcomingCard);
  renderGrid('pastContestsGrid', past, renderPastCard);

  // Lazy load past contest winners
  past.forEach(c => fetchTopPerformer(c.contestId));
}

function renderGrid(containerId, contests, renderFn) {
  const container = document.getElementById(containerId);
  if (!container) return;

  if (contests.length === 0) {
    container.innerHTML = `<div class="empty-state">No contests available in this category.</div>`;
    return;
  }

  container.innerHTML = contests.map(c => renderFn(c)).join('');
}

function renderOngoingCard(c) {
  return `
    <a href="dashboard.html?contest=${c.contestId}" class="contest-card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${c.name}</h3>
          <div class="card-meta">Ends ${new Date(c.endTime).toLocaleDateString()}</div>
        </div>
      </div>
      <div class="card-stats">
        <div class="stat-box">
          <div class="stat-val">${c.expectedUsers || '10k+'}</div>
          <div class="stat-lbl">Participants</div>
        </div>
        <div class="stat-box">
          <div class="stat-val">Live</div>
          <div class="stat-lbl">Status</div>
        </div>
      </div>
      <button class="btn btn-primary">View Live Leaderboard</button>
    </a>
  `;
}

function renderUpcomingCard(c) {
  const start = new Date(c.startTime);
  const diffDays = Math.ceil((start - new Date()) / (1000 * 60 * 60 * 24));
  
  return `
    <div class="contest-card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${c.name}</h3>
          <div class="card-meta">Starts ${start.toLocaleDateString()}</div>
        </div>
      </div>
      <div class="card-stats">
        <div class="stat-box">
          <div class="stat-val">${c.expectedUsers || 'TBD'}</div>
          <div class="stat-lbl">Expected Users</div>
        </div>
        <div class="stat-box">
          <div class="stat-val">${diffDays} Days</div>
          <div class="stat-lbl">Countdown</div>
        </div>
      </div>
      <button class="btn btn-secondary" style="width:100%; margin-top:var(--spacing-sm);">View Details</button>
    </div>
  `;
}

function renderPastCard(c) {
  return `
    <a href="dashboard.html?contest=${c.contestId}" class="contest-card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${c.name}</h3>
          <div class="card-meta">Ended ${new Date(c.endTime).toLocaleDateString()}</div>
        </div>
      </div>
      <div class="winner-box" id="winner-${c.contestId}">
        <div class="winner-avatar">🏆</div>
        <div><div style="font-size:12px; font-weight:600;">Loading Winner...</div></div>
      </div>
      <button class="btn btn-secondary" style="width:100%; margin-top:var(--spacing-sm);">View Results</button>
    </a>
  `;
}

async function fetchTopPerformer(contestId) {
  try {
    const res = await fetch(`https://leaderstack.onrender.com/contests/${contestId}/leaderboard?limit=1`);
    const json = await res.json();
    const box = document.getElementById(`winner-${contestId}`);
    
    if (res.ok && json.data && json.data.length > 0) {
      const p = json.data[0];
      const initials = p.participantId.substring(0, 2).toUpperCase();
      if(box) {
        box.innerHTML = `
          <div class="winner-avatar" style="background:var(--color-primary); color:var(--color-canvas);">${initials}</div>
          <div style="font-size:12px; font-weight:600; color:var(--color-ink);">${p.participantId}</div>
          <div class="winner-score">${p.score} pts</div>
        `;
      }
    } else {
      if(box) {
        box.innerHTML = `
          <div class="winner-avatar">🏆</div>
          <div style="font-size:12px; font-weight:600; color:var(--color-mute);">No submissions</div>
        `;
      }
    }
  } catch (err) {
    console.error("Failed to fetch winner for", contestId, err);
  }
}
