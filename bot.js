require('dotenv').config();
const { io } = require('socket.io-client');
const fs = require('fs');
const path = require('path');
const express = require('express');
const { marked } = require('marked');
const DOMPurify = require('isomorphic-dompurify');

// Stamp format: YYYY-MM-DDTHH-MM. Validate before any path.join to prevent
// path traversal on /b/:stamp and /b/:stamp/download.
const STAMP_RE = /^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}$/;
function isValidStamp(s) { return typeof s === 'string' && STAMP_RE.test(s); }

const CONFIG = {
  j7WsUrl: 'wss://nyc.j7tracker.io',
  nvidiaApiKey: process.env.NVIDIA_API_KEY,
  nvidiaModel: 'moonshotai/kimi-k2.6',
  windowHours: parseFloat(process.env.WINDOW_HOURS) || 6,
  briefingIntervalHours: parseFloat(process.env.BRIEFING_INTERVAL_HOURS) || 6,
  minTweetsPerCluster: parseInt(process.env.MIN_TWEETS_PER_CLUSTER) || 3,
  maxClustersInBriefing: parseInt(process.env.MAX_CLUSTERS) || 12,
  maxHydratePerCluster: parseInt(process.env.MAX_HYDRATE) || 12,
  // Notable authors whose tweets surface as "wildcards" regardless of cluster
  // volume — a niche reply from a founder/CEO can seed a build opportunity
  // even if the j7 label appears in only one tweet.
  notableAuthors: new Set(
    (process.env.NOTABLE_AUTHORS ||
      'aeyakovenko,elonmusk,vitalikbuterin,sama,jack,balajis,naval,paulg,levelsio,nikitabier,garrytan,pmarca,cdixon,packym,dhh,rauchg,patrickc'
    ).split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
  ),
  maxWildcardHydrate: parseInt(process.env.MAX_WILDCARD_HYDRATE) || 60,
  retainHours: 48,
  port: parseInt(process.env.PORT) || 3000,
  // Storage paths — overridable so a hosted deploy can mount a persistent
  // volume (e.g. Railway Volume) and point these at it.
  dataDir: process.env.DATA_DIR || path.join(__dirname, 'data'),
  briefingsDir: process.env.BRIEFINGS_DIR || path.join(__dirname, 'briefings'),
  // Optional HTTP basic auth — set both BASIC_AUTH_USER + BASIC_AUTH_PASS to enable.
  basicAuthUser: process.env.BASIC_AUTH_USER || '',
  basicAuthPass: process.env.BASIC_AUTH_PASS || '',
};
CONFIG.tweetsFile = path.join(CONFIG.dataDir, 'tweets.jsonl');

fs.mkdirSync(CONFIG.dataDir, { recursive: true });
fs.mkdirSync(CONFIG.briefingsDir, { recursive: true });

// shared runtime state for status panel
const state = {
  wsConnected: false,
  wsId: null,
  startedAt: Date.now(),
  lastBriefingAt: null,
  nextBriefingAt: Date.now() + CONFIG.briefingIntervalHours * 3600 * 1000,
  briefingInFlight: false,
  totalSeen: 0,
};

// in-memory ring buffer of most recent suggestions, newest first
const RECENT_MAX = 200;
const recentTweets = [];
// active SSE clients for the live feed
const sseClients = new Set();

// live briefing state — partial markdown as it streams from the LLM
const briefingState = {
  inProgress: false,
  startedAt: null,
  meta: null,
  partialMarkdown: '',
  lastStamp: null,
};
const briefingSseClients = new Set();

// ---------- Storage ----------
function appendTweet(record) {
  fs.appendFileSync(CONFIG.tweetsFile, JSON.stringify(record) + '\n');
}

function readTweetsSince(cutoffMs) {
  if (!fs.existsSync(CONFIG.tweetsFile)) return [];
  const lines = fs.readFileSync(CONFIG.tweetsFile, 'utf8').split('\n');
  const out = [];
  for (const line of lines) {
    if (!line) continue;
    try {
      const r = JSON.parse(line);
      if (r.ts >= cutoffMs) out.push(r);
    } catch {}
  }
  return out;
}

function pruneOldTweets() {
  if (!fs.existsSync(CONFIG.tweetsFile)) return;
  const cutoff = Date.now() - CONFIG.retainHours * 3600 * 1000;
  const lines = fs.readFileSync(CONFIG.tweetsFile, 'utf8').split('\n');
  const keep = [];
  for (const line of lines) {
    if (!line) continue;
    try {
      if (JSON.parse(line).ts >= cutoff) keep.push(line);
    } catch {}
  }
  fs.writeFileSync(CONFIG.tweetsFile, keep.length ? keep.join('\n') + '\n' : '');
}

// ---------- Clustering ----------
function normalizeTopic(s) {
  return (s || '').toLowerCase().trim().replace(/\s+/g, ' ');
}

function clusterTweets(tweets) {
  const map = new Map();
  for (const t of tweets) {
    const key = normalizeTopic(t.prediction);
    if (!key) continue;
    if (!map.has(key)) {
      map.set(key, {
        topic: t.prediction,
        tickers: new Set(),
        records: [],
      });
    }
    const c = map.get(key);
    if (t.ticker) c.tickers.add(t.ticker);
    c.records.push(t);
  }
  return [...map.values()]
    .filter(c => c.records.length >= CONFIG.minTweetsPerCluster)
    .sort((a, b) => b.records.length - a.records.length)
    .slice(0, CONFIG.maxClustersInBriefing);
}

// ---------- Tweet hydration ----------
async function fetchTweet(tweetId) {
  try {
    const res = await fetch(`https://api.fxtwitter.com/i/status/${tweetId}`);
    const data = await res.json();
    if (data.code !== 200 || !data.tweet) return null;
    const t = data.tweet;
    return {
      id: t.id,
      text: (t.text || '').replace(/\s+/g, ' ').trim(),
      author: t.author?.screen_name || 'unknown',
      authorName: t.author?.name || '',
      authorFollowers: t.author?.followers ?? null,
      likes: t.likes || 0,
      retweets: t.retweets || 0,
      replies: t.replies || 0,
      views: t.views || 0,
      hasImage: (t.media?.photos?.length || 0) > 0,
      hasVideo: (t.media?.videos?.length || 0) > 0,
      url: t.url,
    };
  } catch {
    return null;
  }
}

async function hydrateCluster(cluster) {
  // Sample up to maxHydratePerCluster, spread evenly across the time range.
  const recs = cluster.records.slice().sort((a, b) => a.ts - b.ts);
  const sample = recs.length <= CONFIG.maxHydratePerCluster
    ? recs
    : pickSpread(recs, CONFIG.maxHydratePerCluster);

  const samples = [];
  for (const rec of sample) {
    const tweet = await fetchTweet(rec.tweet_id);
    if (tweet) samples.push({ ...tweet, ts: rec.ts });
  }

  // Compute author stats and timeline
  const authorCounts = {};
  for (const s of samples) {
    authorCounts[s.author] = (authorCounts[s.author] || 0) + 1;
  }
  const distinctAuthors = Object.keys(authorCounts).length;
  const repeatAuthors = Object.entries(authorCounts)
    .filter(([, n]) => n > 1)
    .sort((a, b) => b[1] - a[1]);

  const firstTs = recs[0]?.ts;
  const lastTs = recs[recs.length - 1]?.ts;

  // Peak velocity: tweets in the hottest 10-minute bucket. Brand-new tweets
  // mean likes/retweets are useless — volume bursts are the real heat signal.
  const bucketMs = 10 * 60 * 1000;
  const buckets = new Map();
  for (const r of recs) {
    const b = Math.floor(r.ts / bucketMs);
    buckets.set(b, (buckets.get(b) || 0) + 1);
  }
  let peakCount = 0;
  let peakBucket = null;
  for (const [b, n] of buckets) {
    if (n > peakCount) { peakCount = n; peakBucket = b; }
  }
  const peakPerMin = (peakCount / 10).toFixed(1);
  const peakAt = peakBucket !== null ? peakBucket * bucketMs : null;

  // Combined follower reach across distinct authors in the hydrated sample.
  const seenAuthors = new Set();
  let followerReach = 0;
  for (const s of samples) {
    if (seenAuthors.has(s.author)) continue;
    seenAuthors.add(s.author);
    followerReach += s.authorFollowers || 0;
  }

  return {
    ...cluster,
    totalTweets: recs.length,
    samples,
    sampleSize: samples.length,
    authorCounts,
    distinctAuthors,
    repeatAuthors,
    firstTs,
    lastTs,
    peakPerMin,
    peakAt,
    followerReach,
  };
}

function pickSpread(arr, n) {
  if (arr.length <= n) return arr.slice();
  const step = (arr.length - 1) / (n - 1);
  const out = [];
  for (let i = 0; i < n; i++) out.push(arr[Math.round(i * step)]);
  return out;
}

// Scan tweets that did NOT make it into the main clusters and hydrate a
// spread sample to check author against CONFIG.notableAuthors. Any match
// becomes a "wildcard" — surfaced individually in the briefing even though
// its label didn't cluster.
async function findWildcards(allTweets, mainClusters) {
  if (CONFIG.notableAuthors.size === 0) return [];
  const inMain = new Set();
  for (const c of mainClusters) {
    for (const r of c.records) inMain.add(r.tweet_id);
  }
  const leftover = allTweets.filter(t => !inMain.has(t.tweet_id));
  if (leftover.length === 0) return [];
  const sample = leftover.length <= CONFIG.maxWildcardHydrate
    ? leftover
    : pickSpread(leftover.slice().sort((a, b) => a.ts - b.ts), CONFIG.maxWildcardHydrate);
  const wildcards = [];
  for (const rec of sample) {
    const tweet = await fetchTweet(rec.tweet_id);
    if (!tweet) continue;
    const handle = (tweet.author || '').toLowerCase();
    if (CONFIG.notableAuthors.has(handle)) {
      wildcards.push({
        ...tweet,
        ts: rec.ts,
        prediction: rec.prediction,
        ticker: rec.ticker,
      });
    }
  }
  return wildcards;
}

function fmtAgo(ts) {
  const mins = Math.round((Date.now() - ts) / 60000);
  if (mins < 60) return `${mins}m ago`;
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return m ? `${h}h${m}m ago` : `${h}h ago`;
}

function fmtTime(ts) {
  const d = new Date(ts);
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
}

function fmtReach(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return String(n);
}

function formatHeatLine(c) {
  const peakLabel = c.peakAt ? `${fmtTime(c.peakAt)} UTC` : '—';
  return `${c.totalTweets} tweets · ${c.peakPerMin}/min peak @ ${peakLabel} · ${c.distinctAuthors}/${c.sampleSize} sampled authors distinct · ${fmtReach(c.followerReach)} follower reach`;
}

// ---------- LLM briefing (streaming) ----------
async function generateBriefing(hydratedClusters, wildcards, windowHours, onDelta) {
  const sections = hydratedClusters.map(c => {
    const tickers = [...c.tickers].slice(0, 5).join(', ') || 'none';
    const repeats = c.repeatAuthors.length
      ? c.repeatAuthors.map(([a, n]) => `@${a} (${n}x)`).join(', ')
      : 'none';
    const heatLine = formatHeatLine(c);
    const tweets = c.samples.length
      ? c.samples.map(t => {
          const followers = t.authorFollowers !== null ? ` (${fmtReach(t.authorFollowers)} followers)` : '';
          const media = t.hasImage ? ' [img]' : t.hasVideo ? ' [vid]' : '';
          return `    - [${fmtTime(t.ts)} UTC] @${t.author}${followers}${media}: "${t.text.slice(0, 320)}"`;
        }).join('\n')
      : '    (no sample tweets available)';
    return `Topic raw label: "${c.topic}"
  HEAT: ${heatLine}
  First seen: ${fmtAgo(c.firstTs)} | Last seen: ${fmtAgo(c.lastTs)}
  Repeat authors: ${repeats}
  Suggested tickers: ${tickers}
  Sample tweets (chronological, UTC):
${tweets}`;
  }).join('\n\n---\n\n');

  const wildcardsBlock = (!wildcards || wildcards.length === 0)
    ? '(no wildcard tweets from notable authors in this window)'
    : wildcards.map(w => {
        const followers = w.authorFollowers !== null ? ` (${fmtReach(w.authorFollowers)} followers)` : '';
        const media = w.hasImage ? ' [img]' : w.hasVideo ? ' [vid]' : '';
        const tagged = w.prediction ? ` [j7 label: "${w.prediction}"]` : '';
        return `- [${fmtTime(w.ts)} UTC] @${w.author}${followers}${media}${tagged}: "${w.text.slice(0, 400)}"`;
      }).join('\n');

  const prompt = `You are a sharp project-manager-grade analyst writing a BUILD-OPPORTUNITY briefing for engineers and PMs who scan trending tweets to spot what to ship next.

Below are the topic clusters from the last ${windowHours} hours. Each cluster comes from j7tracker's per-tweet AI label.

IMPORTANT — the websocket gives us brand-new tweets the moment they are posted, so likes / retweets / views are NOT meaningful (everything is freshly posted with no time to accumulate engagement). Heat is measured purely by:
- Volume (total tweets in the window)
- Velocity (peak tweets/min in any 10-minute bucket)
- Author breadth (distinct authors in the hydrated sample — many = organic conversation, few = single-account spam)
- Follower reach potential (combined audience of posting accounts)

DATA — CLUSTERS:

${sections}

DATA — WILDCARDS (individual tweets from founder/builder/investor accounts whose label did NOT cluster):

${wildcardsBlock}

YOUR JOB: sort EVERY cluster into exactly one of three tiers, AND evaluate every wildcard individually. Do not silently drop any cluster or wildcard — being explicit about what's noise IS part of the value. The user wants breadth, not a 3-line briefing.

Write a markdown briefing with this exact structure:

1. Open with a single italics sentence naming the biggest build opportunity in this ${windowHours}h window. No heading, just the sentence. If nothing is buildable, say "_No actionable build opportunities in this window._" and continue with the tier sections anyway.

2. \`## Tier 1 — Build now\`
   One \`###\` subsection per buildable topic. The \`###\` is a punchy headline YOU write, not the raw label. Order by build-attractiveness × heat, biggest first. For each topic, EXACTLY these five parts in order:
   - **Heat.** Paste the HEAT line from the data block VERBATIM.
   - **Trend.** 1-2 sentences on what's being said and by whom. Cite @handles. If one author dominates the sample (3+ tweets), call it out — that's either conviction or single-person hype, and the reader needs to know which.
   - **Domain.** A single tag: \`tech\` / \`crypto\` / \`trading\` / \`ai\` / \`culture\` / \`politics\` / \`other\`.
   - **Build angles.** A bulleted list of 2-4 CONCRETE product ideas, one short sentence each. Specific beats generic — "a leaderboard scraping X" beats "a tool for X".
   - **Speed call.** One sentence: \`ship this weekend\` / \`2-week build\` / \`multi-month bet\`, plus one clause on competition ("no incumbent yet" / "X already owns the obvious version" / "saturated, only an angle wins").

3. \`## Tier 2 — Hot but no angle\`
   Clusters with real heat (volume, velocity, or author breadth) but no clear product hook. ONE paragraph each, 3-5 sentences. Lead with the topic name in bold, paste the HEAT line inline, describe what's happening, then say what would have to change for it to become buildable. Do NOT invent forced angles to promote a topic into Tier 1 — honesty about non-buildability IS the value here.

4. \`## Tier 3 — Noise dismissed\`
   Single-account hype, automated breaking-news echo, celebrity drama, sports recaps, anything without a product surface. ONE line each: bold topic name, one-clause dismissal reason. Example:
   - **@Cache100x "ATM" Solana token** — single account, 5 posts in 11 minutes, spam pattern.

5. \`## Watch\`
   2-4 bullets of things to monitor in the next 12-24h that could open ship-worthy windows. One sentence each, specific (named event, account, expected announcement).

Wildcards: each wildcard is a single tweet from a notable account. Their cluster did NOT pass the volume threshold, so there's no HEAT line — judge on tweet content + author authority alone.
- If the wildcard sparks a real product opportunity, slot it as a Tier 1 \`###\` subsection. Use \`Wildcard · @handle (Nx followers)\` as the Heat line in place of the cluster HEAT.
- If it's interesting context but not buildable (e.g. a takeaway from a respected voice), mention in Tier 2 with the handle.
- If it's a throwaway / off-topic reply, list in Tier 3 as \`**Wildcard @handle** — one-clause reason\`.
- Never silently drop a wildcard.

Rules:
- Every cluster in the input MUST appear in exactly one tier. No silent drops.
- Every wildcard in the input MUST appear in exactly one tier. No silent drops.
- Tier 1 and Tier 2 must paste the HEAT line from the cluster data block verbatim — do not paraphrase or round the numbers. Wildcards use the Wildcard · @handle format instead.
- If multiple raw labels clearly describe the same event (e.g. "Trump" + "Secret Service" + "White House gunman"), merge them under one entry and combine the HEAT lines in the form "N raw labels merged · X total tweets · ...".
- Tone: terse, opinionated, no hedging, no emoji, no marketing voice. Senior PM with skin in the game — confident calls, not "could potentially perhaps."`;

  const res = await fetch('https://integrate.api.nvidia.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CONFIG.nvidiaApiKey}`,
      'Accept': 'text/event-stream',
    },
    body: JSON.stringify({
      model: CONFIG.nvidiaModel,
      messages: [
        { role: 'system', content: 'You are a PM-grade analyst who extracts buildable product opportunities from trending tweet data. You skip noise aggressively. Confident, terse, no hedging, no emoji.' },
        { role: 'user', content: prompt },
      ],
      max_tokens: 4000,
      temperature: 0.5,
      stream: true,
    }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`LLM HTTP ${res.status} ${txt.slice(0, 200)}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let full = '';
  let reasoningFull = '';
  let chunkCount = 0;
  const t0 = Date.now();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6);
      if (data === '[DONE]') continue;
      try {
        const parsed = JSON.parse(data);
        const delta = parsed.choices?.[0]?.delta;
        if (!delta) continue;
        if (chunkCount === 0) {
          console.log(`[stream] first chunk after ${Date.now() - t0}ms; delta keys: ${JSON.stringify(Object.keys(delta))}`);
        }
        chunkCount += 1;
        // Reasoning tokens (Kimi K2 thinking models): show as "thinking" so the UI
        // can indicate progress while the model is reasoning before emitting content.
        if (delta.reasoning_content) {
          reasoningFull += delta.reasoning_content;
          if (onDelta) onDelta({ type: 'reasoning', text: delta.reasoning_content });
        }
        if (delta.content) {
          if (full === '' && reasoningFull) {
            console.log(`[stream] content begins after ${Date.now() - t0}ms (${reasoningFull.length} reasoning chars)`);
          }
          full += delta.content;
          if (onDelta) onDelta({ type: 'content', text: delta.content });
        }
      } catch {}
    }
  }
  console.log(`[stream] done: ${chunkCount} chunks, ${full.length} content chars, ${reasoningFull.length} reasoning chars, ${Date.now() - t0}ms total`);
  return full || '(LLM returned no content)';
}

// ---------- Briefing cycle ----------
async function runBriefing() {
  if (state.briefingInFlight) {
    console.log('[briefing] already running, skipping');
    return null;
  }
  state.briefingInFlight = true;
  briefingState.inProgress = true;
  briefingState.startedAt = Date.now();
  briefingState.partialMarkdown = '';
  briefingState.meta = null;
  broadcastBriefEvent('start', { startedAt: briefingState.startedAt });

  try {
    const startedAt = new Date();
    const cutoff = Date.now() - CONFIG.windowHours * 3600 * 1000;
    const tweets = readTweetsSince(cutoff);
    console.log(`[briefing] window=${CONFIG.windowHours}h tweets=${tweets.length}`);

    const clusters = clusterTweets(tweets);
    console.log(`[briefing] clusters above threshold (>=${CONFIG.minTweetsPerCluster}): ${clusters.length}`);

    if (clusters.length === 0) {
      console.log('[briefing] no hot topics, skipping write');
      broadcastBriefEvent('noop', { reason: 'no topics in window' });
      return null;
    }

    console.log(`[briefing] hydrating ${clusters.length} clusters...`);
    broadcastBriefEvent('phase', { phase: 'hydrating', clusters: clusters.length });
    const hydrated = [];
    for (const c of clusters) {
      hydrated.push(await hydrateCluster(c));
    }

    broadcastBriefEvent('phase', { phase: 'wildcards' });
    const wildcards = await findWildcards(tweets, clusters);
    console.log(`[briefing] wildcards from notable authors: ${wildcards.length}`);

    const meta = {
      generatedAt: startedAt.toISOString(),
      windowHours: CONFIG.windowHours,
      tweetsSeen: tweets.length,
      topicsCovered: clusters.length,
      wildcards: wildcards.length,
    };
    briefingState.meta = meta;
    broadcastBriefEvent('meta', meta);

    console.log('[briefing] calling LLM (streaming)...');
    broadcastBriefEvent('phase', { phase: 'writing' });
    const body = await generateBriefing(hydrated, wildcards, CONFIG.windowHours, (chunk) => {
      if (chunk.type === 'content') {
        briefingState.partialMarkdown += chunk.text;
        broadcastBriefEvent('delta', { text: chunk.text });
      } else if (chunk.type === 'reasoning') {
        broadcastBriefEvent('thinking', { text: chunk.text });
      }
    });

    const stamp = startedAt.toISOString().replace(/[:.]/g, '-').slice(0, 16);
    const file = path.join(CONFIG.briefingsDir, `${stamp}.md`);
    const header =
      `---\n` +
      Object.entries(meta).map(([k, v]) => `${k}: ${v}`).join('\n') +
      `\n---\n\n` +
      `# Briefing — ${startedAt.toISOString()}\n\n`;
    fs.writeFileSync(file, header + body + '\n');
    console.log(`[briefing] wrote ${file}`);

    state.lastBriefingAt = Date.now();
    state.nextBriefingAt = Date.now() + CONFIG.briefingIntervalHours * 3600 * 1000;
    briefingState.lastStamp = stamp;
    broadcastBriefEvent('done', { stamp, file });

    pruneOldTweets();
    return { file, stamp, meta };
  } catch (e) {
    broadcastBriefEvent('error', { message: e.message });
    throw e;
  } finally {
    state.briefingInFlight = false;
    briefingState.inProgress = false;
    // keep partialMarkdown around for 60s so late SSE subscribers see the final state
    setTimeout(() => {
      if (!briefingState.inProgress) {
        briefingState.partialMarkdown = '';
        briefingState.meta = null;
      }
    }, 60_000);
  }
}

function broadcastBriefEvent(eventName, data) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of briefingSseClients) {
    try { res.write(payload); } catch {}
  }
}

// ---------- WS collector ----------
function startCollector() {
  const socket = io(CONFIG.j7WsUrl, {
    transports: ['websocket'],
    reconnection: true,
    reconnectionDelay: 1000,
    reconnectionAttempts: Infinity,
  });

  socket.on('connect', () => {
    state.wsConnected = true;
    state.wsId = socket.id;
    console.log(`[ws] connected id=${socket.id}`);
  });
  socket.on('disconnect', (r) => {
    state.wsConnected = false;
    state.wsId = null;
    console.log(`[ws] disconnected: ${r}`);
  });
  socket.on('connect_error', (e) => console.log(`[ws] connect_error: ${e.message}`));

  let loggedPayloads = 0;
  socket.on('ai_suggestion', (data) => {
    if (!data?.tweet_id) return;
    // One-time peek at the raw payload shape. If j7 already sends author info
    // here, we can skip the wildcard hydration cost in a later pass.
    if (loggedPayloads < 3) {
      console.log('[ws] ai_suggestion payload sample:', JSON.stringify(data));
      loggedPayloads++;
    }
    const rec = {
      tweet_id: data.tweet_id,
      prediction: data.prediction || '',
      ticker: data.ticker || '',
      ts: Date.now(),
    };
    appendTweet(rec);
    recentTweets.unshift(rec);
    if (recentTweets.length > RECENT_MAX) recentTweets.length = RECENT_MAX;
    state.totalSeen += 1;
    broadcastSse(rec);
  });

  return socket;
}

// ---------- UI helpers ----------
function listBriefings() {
  const files = fs.readdirSync(CONFIG.briefingsDir).filter(f => f.endsWith('.md'));
  return files
    .map(f => {
      const full = path.join(CONFIG.briefingsDir, f);
      const stat = fs.statSync(full);
      const stamp = f.replace(/\.md$/, '');
      return { stamp, file: full, mtimeMs: stat.mtimeMs };
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs);
}

function readBriefing(stamp) {
  const full = path.join(CONFIG.briefingsDir, `${stamp}.md`);
  if (!fs.existsSync(full)) return null;
  const raw = fs.readFileSync(full, 'utf8');
  // Parse frontmatter
  const fm = {};
  let body = raw;
  const m = raw.match(/^---\n([\s\S]+?)\n---\n+([\s\S]*)$/);
  if (m) {
    for (const line of m[1].split('\n')) {
      const idx = line.indexOf(':');
      if (idx > 0) fm[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
    }
    body = m[2];
  }
  return { stamp, frontmatter: fm, body };
}

function fmtStampForDisplay(stamp) {
  // stamp = "2026-05-23T18-42"  → "2026-05-23 18:42 UTC"
  const m = stamp.match(/^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})/);
  if (!m) return stamp;
  return `${m[1]} ${m[2]}:${m[3]} UTC`;
}

function fmtRelative(ts) {
  if (!ts) return '—';
  const mins = Math.round((Date.now() - ts) / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const h = Math.floor(mins / 60);
  return `${h}h ${mins % 60}m ago`;
}

function fmtFuture(ts) {
  if (!ts) return '—';
  const mins = Math.round((ts - Date.now()) / 60000);
  if (mins <= 0) return 'now';
  if (mins < 60) return `in ${mins}m`;
  const h = Math.floor(mins / 60);
  return `in ${h}h ${mins % 60}m`;
}

function renderStatusBlock() {
  const cutoff = Date.now() - CONFIG.windowHours * 3600 * 1000;
  const inWindow = readTweetsSince(cutoff).length;
  const items = [
    ['feed', state.wsConnected ? '<span class="dot ok"></span>live' : '<span class="dot bad"></span>offline'],
    ['tweets · ' + CONFIG.windowHours + 'h', String(inWindow)],
    ['last briefing', state.lastBriefingAt ? fmtRelative(state.lastBriefingAt) : '—'],
    ['next briefing', state.briefingInFlight ? 'running…' : fmtFuture(state.nextBriefingAt)],
  ];
  return items.map(([k, v]) => `<div class="status-row"><span class="status-k">${k}</span><span class="status-v">${v}</span></div>`).join('');
}

function renderSidebarList(currentStamp) {
  const list = listBriefings();
  if (list.length === 0) {
    return `<div class="sidebar-empty">No briefings yet.<br>First one fires when enough tweets cluster.</div>`;
  }
  return list.map(b => {
    const isCurrent = b.stamp === currentStamp ? ' current' : '';
    return `<a href="/b/${b.stamp}" class="briefing-link${isCurrent}">
      <div class="briefing-link-stamp">${fmtStampForDisplay(b.stamp)}</div>
    </a>`;
  }).join('');
}

function renderShell(opts) {
  const {
    title,
    eyebrow = '',
    topbarTitle,
    topbarButton = '',
    metaHTML = '',
    currentStamp = null,
    activeNav = 'briefings',
    contentHTML,
  } = opts;
  // Use the function form of replace to avoid $-pattern interpretation
  // ($' / $& / $1 etc. in replacement strings would corrupt JS/markdown content).
  const sub = (val) => () => String(val);
  const template = fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf8');
  return template
    .replace(/\{\{TITLE\}\}/g, sub(title))
    .replace(/\{\{EYEBROW\}\}/g, sub(eyebrow))
    .replace(/\{\{TOPBAR_TITLE\}\}/g, sub(topbarTitle ?? title))
    .replace(/\{\{TOPBAR_BUTTON\}\}/g, sub(topbarButton))
    .replace(/\{\{NAV_BRIEFINGS_CLASS\}\}/g, sub(activeNav === 'briefings' ? 'active' : ''))
    .replace(/\{\{NAV_LIVE_CLASS\}\}/g, sub(activeNav === 'live' ? 'active' : ''))
    .replace(/\{\{NAV_FEED_CLASS\}\}/g, sub(activeNav === 'feed' ? 'active' : ''))
    .replace(/\{\{STATUS\}\}/g, sub(renderStatusBlock()))
    .replace(/\{\{BRIEFINGS_LIST\}\}/g, sub(renderSidebarList(currentStamp)))
    .replace(/\{\{META\}\}/g, sub(metaHTML))
    .replace(/\{\{CONTENT\}\}/g, sub(contentHTML));
}

// Briefings now fire automatically on a UTC clock-aligned schedule (see
// scheduleNextBriefing). The manual-trigger button is gone from the UI to
// avoid suggesting human action is required. POST /api/brief still exists
// for testing via curl.
const BRIEFING_BUTTON = '';

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function renderBriefingPage(stamp) {
  if (!isValidStamp(stamp)) return null;
  const b = readBriefing(stamp);
  if (!b) return null;
  const md = b.body;
  // Sanitize LLM-generated HTML — tweet content can include arbitrary text that
  // the LLM may quote verbatim, and marked passes raw HTML through by default.
  const html = DOMPurify.sanitize(marked.parse(md));
  const fm = b.frontmatter;
  const metaHTML = `
    <div class="meta-line"><span class="meta-k">generated</span> ${escapeHtml(fm.generatedAt || stamp)}</div>
    <div class="meta-line"><span class="meta-k">window</span> ${escapeHtml(fm.windowHours || '?')}h</div>
    <div class="meta-line"><span class="meta-k">tweets</span> ${escapeHtml(fm.tweetsSeen || '?')}</div>
    <div class="meta-line"><span class="meta-k">topics</span> ${escapeHtml(fm.topicsCovered || '?')}</div>
    <div class="meta-line"><a class="meta-download" href="/b/${encodeURIComponent(stamp)}/download">download .md ↓</a></div>
  `;
  return renderShell({
    title: `Briefing · ${fmtStampForDisplay(stamp)}`,
    eyebrow: 'latest briefing',
    topbarButton: BRIEFING_BUTTON,
    currentStamp: stamp,
    activeNav: 'briefings',
    metaHTML,
    contentHTML: `<article class="briefing"><div class="briefing-stamp">${fmtStampForDisplay(stamp)}</div>${html}</article>`,
  });
}

function renderHomePage() {
  const list = listBriefings();
  if (list.length === 0) {
    const cutoff = Date.now() - CONFIG.windowHours * 3600 * 1000;
    const inWindow = readTweetsSince(cutoff).length;
    return renderShell({
      title: 'Briefing',
      eyebrow: 'no briefings yet',
      topbarTitle: 'Briefing',
      topbarButton: BRIEFING_BUTTON,
      activeNav: 'briefings',
      contentHTML: `
        <article class="briefing">
          <div class="empty">
            <div class="empty-title">No briefings yet</div>
            <div class="empty-body">
              The bot is listening to <code>${CONFIG.j7WsUrl}</code> and collecting tweets.<br>
              Right now there are <strong>${inWindow}</strong> tweets in the last ${CONFIG.windowHours}h window.<br>
              The first briefing fires once enough topics cluster.
            </div>
          </div>
        </article>
      `,
    });
  }
  return renderBriefingPage(list[0].stamp);
}

function renderLivePage() {
  const metaHTML = `
    <div class="meta-line"><span class="meta-k">status</span> <span id="live-meta-status">connecting…</span></div>
    <div class="meta-line"><span class="meta-k">chars</span> <span id="live-meta-chars">0</span></div>
  `;
  const contentHTML = `
    <article class="briefing live-briefing">
      <div id="live-idle" class="live-idle" style="display:none">
        <div class="live-idle-title">No briefing being written</div>
        <div class="live-idle-sub" id="live-idle-sub">—</div>
      </div>
      <div id="live-content" class="live-content"></div>
    </article>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"></script>
    <script>
      (function () {
        const idleEl = document.getElementById('live-idle');
        const idleSubEl = document.getElementById('live-idle-sub');
        const contentEl = document.getElementById('live-content');
        const metaStatusEl = document.getElementById('live-meta-status');
        const metaCharsEl = document.getElementById('live-meta-chars');

        let markdown = '';
        let renderPending = false;

        function render() {
          // Sanitize the rendered markdown — LLM output can include arbitrary
          // tweet text that may contain raw HTML (XSS prevention).
          contentEl.innerHTML = DOMPurify.sanitize(marked.parse(markdown));
          metaCharsEl.textContent = String(markdown.length);
        }
        function scheduleRender() {
          if (renderPending) return;
          renderPending = true;
          requestAnimationFrame(() => {
            renderPending = false;
            render();
          });
        }
        function showIdle(nextAt) {
          idleEl.style.display = '';
          contentEl.style.display = 'none';
          metaStatusEl.textContent = 'idle';
          if (nextAt) {
            const mins = Math.max(0, Math.round((nextAt - Date.now()) / 60000));
            const h = Math.floor(mins / 60);
            const m = mins % 60;
            idleSubEl.textContent = 'Next scheduled in ' + (h ? h + 'h ' + m + 'm' : m + 'm');
          } else {
            idleSubEl.textContent = 'No timer info';
          }
        }
        function showRunning() {
          idleEl.style.display = 'none';
          contentEl.style.display = '';
        }

        const es = new EventSource('/api/brief/stream');
        es.onopen = () => { /* state event will arrive */ };
        es.onerror = () => { metaStatusEl.textContent = 'reconnecting…'; };

        es.addEventListener('state', ev => {
          const s = JSON.parse(ev.data);
          if (s.inProgress) {
            markdown = s.markdown || '';
            showRunning();
            metaStatusEl.textContent = 'writing…';
            scheduleRender();
          } else {
            showIdle(s.nextBriefingAt);
          }
        });
        es.addEventListener('start', () => {
          markdown = '';
          showRunning();
          metaStatusEl.textContent = 'starting…';
          scheduleRender();
        });
        es.addEventListener('phase', ev => {
          const p = JSON.parse(ev.data);
          if (p.phase === 'hydrating') metaStatusEl.textContent = 'hydrating ' + p.clusters + ' clusters…';
          else if (p.phase === 'writing') metaStatusEl.textContent = 'writing…';
        });
        es.addEventListener('meta', ev => {
          const m = JSON.parse(ev.data);
          metaStatusEl.textContent = 'writing · ' + m.topicsCovered + ' topics · ' + m.tweetsSeen + ' tweets';
        });
        let thinkingChars = 0;
        es.addEventListener('thinking', ev => {
          const d = JSON.parse(ev.data);
          thinkingChars += (d.text || '').length;
          if (markdown.length === 0) {
            metaStatusEl.textContent = 'thinking… ' + thinkingChars + ' chars';
          }
        });
        es.addEventListener('delta', ev => {
          const d = JSON.parse(ev.data);
          markdown += d.text;
          scheduleRender();
        });
        es.addEventListener('done', ev => {
          const d = JSON.parse(ev.data);
          metaStatusEl.textContent = 'done — redirecting…';
          setTimeout(() => { location.href = '/b/' + d.stamp; }, 1500);
        });
        es.addEventListener('noop', ev => {
          const d = JSON.parse(ev.data);
          metaStatusEl.textContent = 'no topics';
          showIdle(null);
          idleSubEl.textContent = d.reason || 'Not enough clustered tweets yet.';
        });
        es.addEventListener('error', ev => {
          try {
            const d = JSON.parse(ev.data);
            metaStatusEl.textContent = 'error';
            showIdle(null);
            idleSubEl.textContent = 'Briefing failed: ' + (d.message || 'unknown');
          } catch {}
        });
      })();
    </script>
  `;
  return renderShell({
    title: 'Live brief',
    eyebrow: 'live writing',
    topbarTitle: 'Live brief',
    topbarButton: '',
    activeNav: 'live',
    metaHTML,
    contentHTML,
  });
}

function renderFeedPage() {
  const metaHTML = `
    <div class="meta-line"><span class="meta-k">since start</span> <span id="feed-total">${state.totalSeen}</span> tweets</div>
    <div class="meta-line"><span class="meta-k">stream</span> <span id="feed-status">connecting…</span></div>
  `;
  const contentHTML = `
    <div class="feed">
      <div class="feed-header">
        <div>incoming tweets</div>
        <div><span id="feed-count">0</span> shown</div>
      </div>
      <div class="feed-list" id="feed-list">
        <div class="feed-empty" id="feed-empty">Waiting for the first tweet…</div>
      </div>
    </div>
    <script>
      (function () {
        const list = document.getElementById('feed-list');
        const emptyEl = document.getElementById('feed-empty');
        const countEl = document.getElementById('feed-count');
        const totalEl = document.getElementById('feed-total');
        const statusEl = document.getElementById('feed-status');
        let shown = 0;
        let total = parseInt(totalEl.textContent, 10) || 0;

        function esc(s) {
          return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
        }
        function row(r, isNew) {
          const d = new Date(r.ts);
          const time = String(d.getUTCHours()).padStart(2,'0') + ':' +
                       String(d.getUTCMinutes()).padStart(2,'0') + ':' +
                       String(d.getUTCSeconds()).padStart(2,'0');
          const url = 'https://twitter.com/i/web/status/' + encodeURIComponent(r.tweet_id);
          const el = document.createElement('div');
          el.className = 'feed-row' + (isNew ? ' new' : '');
          el.innerHTML =
            '<div class="feed-time">' + time + '</div>' +
            '<div class="feed-pred" title="' + esc(r.prediction) + '">' + esc(r.prediction || '(no prediction)') + '</div>' +
            '<div class="feed-ticker">' + (r.ticker ? '$' + esc(r.ticker) : '') + '</div>' +
            '<a class="feed-link" href="' + url + '" target="_blank" rel="noopener">view →</a>';
          return el;
        }

        fetch('/api/feed/recent').then(r => r.json()).then(rows => {
          if (rows.length === 0) return;
          if (emptyEl) emptyEl.remove();
          for (const r of rows) {
            list.appendChild(row(r, false));
            shown++;
          }
          countEl.textContent = shown;
        });

        const es = new EventSource('/api/feed/stream');
        es.onopen = () => { statusEl.textContent = 'streaming'; };
        es.onerror = () => { statusEl.textContent = 'reconnecting…'; };
        es.onmessage = ev => {
          let r;
          try { r = JSON.parse(ev.data); } catch { return; }
          if (emptyEl && emptyEl.parentNode) emptyEl.remove();
          list.insertBefore(row(r, true), list.firstChild);
          shown++;
          total++;
          while (list.children.length > 300) list.removeChild(list.lastChild);
          countEl.textContent = shown;
          totalEl.textContent = total;
        };
      })();
    </script>
  `;
  return renderShell({
    title: 'Live feed',
    eyebrow: 'real-time',
    topbarTitle: 'Live feed',
    topbarButton: '',
    activeNav: 'feed',
    metaHTML,
    contentHTML,
  });
}

// ---------- SSE broadcast ----------
function broadcastSse(record) {
  const payload = `data: ${JSON.stringify(record)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch {}
  }
}

// ---------- HTTP basic auth ----------
function basicAuth(req, res, next) {
  if (!CONFIG.basicAuthUser || !CONFIG.basicAuthPass) return next();
  const header = req.headers.authorization || '';
  if (header.startsWith('Basic ')) {
    try {
      const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
      const idx = decoded.indexOf(':');
      if (idx > 0) {
        const u = decoded.slice(0, idx);
        const p = decoded.slice(idx + 1);
        const expectU = Buffer.from(CONFIG.basicAuthUser);
        const expectP = Buffer.from(CONFIG.basicAuthPass);
        const gotU = Buffer.from(u);
        const gotP = Buffer.from(p);
        if (
          gotU.length === expectU.length &&
          gotP.length === expectP.length &&
          require('crypto').timingSafeEqual(gotU, expectU) &&
          require('crypto').timingSafeEqual(gotP, expectP)
        ) {
          return next();
        }
      }
    } catch {}
  }
  res.set('WWW-Authenticate', 'Basic realm="briefing"');
  res.status(401).send('Authentication required');
}

// ---------- HTTP server ----------
function startServer() {
  const app = express();
  app.use(express.json());
  app.use(basicAuth);

  app.get('/', (req, res) => {
    res.type('html').send(renderHomePage());
  });

  app.get('/feed', (req, res) => {
    res.type('html').send(renderFeedPage());
  });

  app.get('/live', (req, res) => {
    res.type('html').send(renderLivePage());
  });

  app.get('/b/:stamp', (req, res) => {
    if (!isValidStamp(req.params.stamp)) return res.status(404).type('html').send('not found');
    const html = renderBriefingPage(req.params.stamp);
    if (!html) return res.status(404).type('html').send('not found');
    res.type('html').send(html);
  });

  app.get('/b/:stamp/download', (req, res) => {
    if (!isValidStamp(req.params.stamp)) return res.status(404).send('not found');
    const full = path.join(CONFIG.briefingsDir, `${req.params.stamp}.md`);
    if (!fs.existsSync(full)) return res.status(404).send('not found');
    res.set({
      'Content-Type': 'text/markdown; charset=utf-8',
      'Content-Disposition': `attachment; filename="briefing-${req.params.stamp}.md"`,
    });
    res.sendFile(full);
  });

  app.get('/api/status', (req, res) => {
    const cutoff = Date.now() - CONFIG.windowHours * 3600 * 1000;
    res.json({
      wsConnected: state.wsConnected,
      tweetsInWindow: readTweetsSince(cutoff).length,
      windowHours: CONFIG.windowHours,
      lastBriefingAt: state.lastBriefingAt,
      nextBriefingAt: state.nextBriefingAt,
      briefingInFlight: state.briefingInFlight,
    });
  });

  app.get('/api/feed/recent', (req, res) => {
    res.json(recentTweets.slice(0, 100));
  });

  app.get('/api/feed/stream', (req, res) => {
    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders();
    res.write(': connected\n\n');
    sseClients.add(res);
    const heartbeat = setInterval(() => {
      try { res.write(': ping\n\n'); } catch {}
    }, 25000);
    req.on('close', () => {
      clearInterval(heartbeat);
      sseClients.delete(res);
    });
  });

  // Fire-and-forget: returns immediately, watch progress via /api/brief/stream
  app.post('/api/brief', (req, res) => {
    if (state.briefingInFlight) {
      return res.status(409).json({ error: 'briefing already running' });
    }
    runBriefing().catch(e => console.error('[briefing] error:', e.message));
    res.status(202).json({ status: 'started' });
  });

  app.get('/api/brief/stream', (req, res) => {
    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders();
    res.write(': connected\n\n');

    const initial = briefingState.inProgress
      ? {
          inProgress: true,
          startedAt: briefingState.startedAt,
          meta: briefingState.meta,
          markdown: briefingState.partialMarkdown,
        }
      : { inProgress: false, nextBriefingAt: state.nextBriefingAt };
    res.write(`event: state\ndata: ${JSON.stringify(initial)}\n\n`);

    briefingSseClients.add(res);
    const heartbeat = setInterval(() => {
      try { res.write(': ping\n\n'); } catch {}
    }, 25000);
    req.on('close', () => {
      clearInterval(heartbeat);
      briefingSseClients.delete(res);
    });
  });

  app.listen(CONFIG.port, () => console.log(`[ui] http://localhost:${CONFIG.port}`));
}

// ---------- Stats CLI ----------
function printStats() {
  const cutoff = Date.now() - CONFIG.windowHours * 3600 * 1000;
  const tweets = readTweetsSince(cutoff);
  const clusters = clusterTweets(tweets);
  console.log(`Window: last ${CONFIG.windowHours}h`);
  console.log(`Tweets in window: ${tweets.length}`);
  console.log(`Clusters above threshold (>=${CONFIG.minTweetsPerCluster}): ${clusters.length}`);
  console.log('\nTop topics:');
  for (const c of clusters.slice(0, 20)) {
    const tickers = [...c.tickers].slice(0, 3).join(', ');
    console.log(`  ${String(c.records.length).padStart(4)}  ${c.topic}${tickers ? `  [${tickers}]` : ''}`);
  }
}

// ---------- Main ----------
async function main() {
  const cmd = process.argv[2];

  if (cmd === 'brief') {
    await runBriefing();
    return;
  }
  if (cmd === 'stats') {
    printStats();
    return;
  }

  console.log('=== Twitter Briefing Bot ===');
  console.log(`WS: ${CONFIG.j7WsUrl}`);
  console.log(`Briefing every ${CONFIG.briefingIntervalHours}h | window ${CONFIG.windowHours}h | min ${CONFIG.minTweetsPerCluster} tweets/cluster`);
  console.log(`Data: ${CONFIG.tweetsFile}`);
  console.log(`Briefings: ${CONFIG.briefingsDir}`);
  console.log('');

  startCollector();
  startServer();
  scheduleNextBriefing();
}

// Schedule the next briefing aligned to UTC clock boundaries, so briefings
// always fire at the same wall-clock times regardless of when the service
// started or was redeployed (e.g. with a 6h interval: 00:00, 06:00, 12:00,
// 18:00 UTC every day).
function scheduleNextBriefing() {
  const intervalMs = CONFIG.briefingIntervalHours * 3600 * 1000;
  const now = Date.now();
  const nextBoundary = Math.ceil((now + 1) / intervalMs) * intervalMs;
  const delayMs = nextBoundary - now;
  state.nextBriefingAt = nextBoundary;
  console.log(`[briefing] next run at ${new Date(nextBoundary).toISOString()} (in ${Math.round(delayMs / 60000)} min)`);
  setTimeout(async () => {
    try {
      await runBriefing();
    } catch (e) {
      console.error('[briefing] error:', e.message);
    }
    scheduleNextBriefing();
  }, delayMs);
}

if (require.main === module) {
  main().catch(e => { console.error(e); process.exit(1); });
}

module.exports = {
  CONFIG,
  appendTweet,
  readTweetsSince,
  clusterTweets,
  fetchTweet,
  hydrateCluster,
  generateBriefing,
  runBriefing,
  startCollector,
};
