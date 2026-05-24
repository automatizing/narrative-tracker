require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { io: j7Connect } = require('socket.io-client');

const app = express();
const server = http.createServer(app);
const uiSocket = new Server(server);

const CONFIG = {
  j7WsUrl: 'wss://j7tracker.io',
  j7DeployUrl: process.env.J7_DEPLOY_URL,
  j7ApiKey: process.env.J7_API_KEY,
  nvidiaApiKey: process.env.NVIDIA_API_KEY,
  nvidiaModel: 'moonshotai/kimi-k2-instruct',
  deployType: process.env.DEPLOY_TYPE || 'pump',
  buyAmount: parseFloat(process.env.BUY_AMOUNT) || 1,
  scoreThreshold: 7,
  autoDeploy: false,
};

// --- Tweet Fetcher ---
async function fetchTweet(tweetId) {
  try {
    const res = await fetch(`https://api.fxtwitter.com/i/status/${tweetId}`);
    const data = await res.json();
    if (data.code !== 200 || !data.tweet) return null;
    const t = data.tweet;
    return {
      id: t.id,
      text: t.text,
      author: t.author?.screen_name || 'unknown',
      authorName: t.author?.name || 'unknown',
      authorAvatar: t.author?.avatar_url || '',
      likes: t.likes || 0,
      retweets: t.retweets || 0,
      replies: t.replies || 0,
      views: t.views || 0,
      photos: t.media?.photos?.map(p => p.url) || [],
      videoThumbnails: t.media?.videos?.map(v => v.thumbnail_url) || [],
      url: t.url,
    };
  } catch (e) {
    return null;
  }
}

// --- LLM Scorer ---
async function scoreTweet(tweet, suggestion) {
  const prompt = `You are an expert memecoin trader and deployer working with REAL MONEY. You spot narratives before they blow up. Your decisions have direct financial consequences so be precise and thoughtful.

TWEET:
- Author: @${tweet.author} (${tweet.authorName})
- Text: "${tweet.text}"

J7 SUGGESTION:
- Token name: "${suggestion.prediction}"
- Ticker: $${suggestion.ticker}

AVAILABLE IMAGES (from J7):
${suggestion.images.map((img, i) => `${i + 1}. ${img.name} ($${img.symbol}) [${img.source}] - ${img.url}`).join('\n')}

TWEET IMAGES:
${tweet.photos.map((url, i) => `Photo ${i + 1}: ${url}`).join('\n') || 'None'}
${tweet.videoThumbnails.map((url, i) => `Video thumbnail ${i + 1}: ${url}`).join('\n') || ''}

IMPORTANT: These are real-time tweets so they will have low engagement. DO NOT factor in current likes, RTs, followers, or account size. A nobody account can break a narrative just as well as a big account. Instead evaluate ONLY based on:
- WHAT is the narrative (breaking news, cultural moments, trending topics, funny/absurd memes, political events)
- IS the name/ticker catchy and memeable
- WOULD crypto degens actually ape into this
- IS this a fresh narrative or already played out
- DOES the tweet content itself have viral potential regardless of who posted it

TICKER RULES (critical - DO NOT use random acronyms):
- NEVER use first-letter acronyms as tickers. $DZF, $HSPTF, $BLMF, $RPU, $EKB mean NOTHING. Nobody will buy a token if the ticker doesn't tell them what it is.
- The ticker should be ONE recognizable word from the narrative — the funniest, most shocking, or most memorable word.
- Examples of GOOD ticker choices:
  - "Drug Zombie Farmers" -> $FARMERS or $ZOMBIE (not $DZF)
  - "Rocket Pocket Underpants" -> $UNDERPANTS (not $RPU)
  - "Justice For Samantha" -> $SAMANTHA (not $JFS)
  - "Elon Musk buys TikTok" -> $TIKTOK (not $EMBT)
  - "Giant Radioactive Spider" -> $SPIDER (not $GRS)
- Pick the word that makes someone go "I NEED to buy that" just from reading the ticker.
- Tickers should be max 10 characters. No special characters.
- CAPITALIZATION: Match the exact casing from the tweet. If the tweet says "Farmers", the ticker is $Farmers NOT $FARMERS. If the tweet says "ZOMBIE", the ticker is $ZOMBIE. People using tweet trackers will look for the exact word from the tweet — matching the casing makes the token instantly recognizable as being from that tweet.

NAME RULES:
- The token name should be the catchiest, most memeable part of the tweet. Not always the full phrase.
- Keep it short and punchy when possible. "How is this possible" is fine. "Working Families Tax Cuts Reform Act" is not.

IMAGE RULES (critical - verify before choosing):
- The image MUST visually match the token narrative. If the token is about a dog, the image must show a dog. If it's about Trump, it must show Trump or be Trump-related.
- If the tweet has a photo/video attached, prefer using that (tweet_photo_1, tweet_photo_2) as it's the most relevant to the narrative.
- From the J7 image list, only pick images that DIRECTLY relate to the token name/concept. Ignore images from unrelated tokens that just happen to share a keyword.
- Images from "solanatracker" are existing on-chain tokens - only use them if the image itself is good and relevant, not because the token name matches.
- If NO image is a good match, use "ascii" - a clean ASCII art image is better than a misleading or unrelated image.

JUSTICE/TRAGEDY NARRATIVES:
- Tweets about murders, injustices, police brutality, wrongful deaths, or "Justice For X" are HIGH VALUE narratives in memecoin culture. People rally behind these emotionally.
- If the tweet is about an injustice to a person (e.g. "Samantha was wrongfully killed"), the name should be "Justice For Samantha" and the ticker should be the person's name: $SAMANTHA.
- CRITICAL: Only deploy if this is BREAKING NEWS or something happening RIGHT NOW. If the person or event in the tweet is from weeks/months/years ago (old case resurfacing, anniversary, throwback), SKIP IT. Stale justice narratives don't pump.
- Clues it's fresh: urgent language ("just happened", "breaking", "today"), news outlet reporting it, multiple accounts tweeting about the same person.
- Clues it's old: "remember when", "X years ago", "RIP" on an anniversary, Wikipedia-style facts about a past event.

Score 8-10: Strong narrative from a big account or a genuinely viral moment. Deploy immediately.
Score 5-7: Decent idea but not urgent. Skip.
Score 1-4: Weak narrative, random tweet, niche content. Skip.

Reply with JSON only:
{
  "score": <1-10>,
  "deploy": <true/false>,
  "reason": "<brief reason>",
  "token_name": "<name to use, max 32 chars>",
  "token_ticker": "<ticker to use, max 10 chars, match casing from tweet, no special chars>",
  "image_choice": <image number from list, or "ascii" or "tweet_photo_1" etc>,
  "image_reason": "<why this image fits the narrative>"
}`;

  try {
    const res = await fetch('https://integrate.api.nvidia.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${CONFIG.nvidiaApiKey}`,
      },
      body: JSON.stringify({
        model: CONFIG.nvidiaModel,
        messages: [
          { role: 'system', content: 'You are a memecoin analyst. Reply with valid JSON only, no markdown.' },
          { role: 'user', content: prompt },
        ],
        max_tokens: 300,
        temperature: 0.3,
      }),
    });
    const data = await res.json();
    const content = data.choices?.[0]?.message?.content;
    if (!content) return null;
    const jsonStr = content.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    return JSON.parse(jsonStr);
  } catch (e) {
    return null;
  }
}

// --- Deploy ---
async function deployToken(params) {
  try {
    const body = {
      api_key: CONFIG.j7ApiKey,
      type: CONFIG.deployType,
      name: params.token_name,
      ticker: params.token_ticker,
      buy_amount: CONFIG.buyAmount,
    };
    if (params.image_choice === 'ascii') {
      body.image_type = 'ascii';
    } else if (params.imageUrl) {
      body.image_data = params.imageUrl;
    }
    const res = await fetch(CONFIG.j7DeployUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return await res.json();
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function resolveImageUrl(choice, suggestion, tweet) {
  if (choice === 'ascii') return null;
  if (typeof choice === 'string' && choice.startsWith('tweet_photo_')) {
    const idx = parseInt(choice.replace('tweet_photo_', '')) - 1;
    return tweet.photos[idx] || null;
  }
  if (typeof choice === 'number' && suggestion.images[choice - 1]) {
    return suggestion.images[choice - 1].url;
  }
  return null;
}

// --- Serve UI ---
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/dashboard.html');
});

// --- Stats ---
const stats = { total: 0, approved: 0, rejected: 0, deployed: 0 };

// --- J7 WebSocket ---
const j7 = j7Connect(CONFIG.j7WsUrl, {
  transports: ['websocket'],
  reconnection: true,
  reconnectionDelay: 1000,
  reconnectionAttempts: 50,
});

const pending = new Map();

j7.on('connect', () => {
  console.log(`[j7] Connected: ${j7.id}`);
  uiSocket.emit('j7_status', 'connected');
});

j7.on('disconnect', (reason) => {
  console.log(`[j7] Disconnected: ${reason}`);
  uiSocket.emit('j7_status', 'disconnected');
});

j7.on('ai_suggestion', (data) => {
  pending.set(data.tweet_id, {
    prediction: data.prediction,
    ticker: data.ticker,
    tweet_id: data.tweet_id,
    images: [],
    processed: false,
  });

  uiSocket.emit('new_suggestion', {
    tweet_id: data.tweet_id,
    prediction: data.prediction,
    ticker: data.ticker,
    status: 'fetching',
  });

  setTimeout(() => processSuggestion(data.tweet_id), 5000);
});

j7.on('ai_suggestion_update', (data) => {
  const entry = pending.get(data.tweet_id);
  if (!entry) return;
  data.results.forEach(r => {
    entry.images.push({
      name: r.name,
      symbol: r.symbol,
      url: r.image,
      source: r.source || data.source,
    });
  });
  uiSocket.emit('images_update', {
    tweet_id: data.tweet_id,
    images: entry.images,
  });
});

async function processSuggestion(tweetId) {
  const suggestion = pending.get(tweetId);
  if (!suggestion || suggestion.processed) return;
  suggestion.processed = true;
  stats.total++;

  // 1. Fetch tweet
  uiSocket.emit('status_update', { tweet_id: tweetId, status: 'fetching_tweet' });
  const tweet = await fetchTweet(tweetId);
  if (!tweet) {
    uiSocket.emit('status_update', { tweet_id: tweetId, status: 'fetch_failed' });
    pending.delete(tweetId);
    return;
  }

  uiSocket.emit('tweet_data', { tweet_id: tweetId, tweet });

  // 2. Score with LLM
  uiSocket.emit('status_update', { tweet_id: tweetId, status: 'scoring' });
  const score = await scoreTweet(tweet, suggestion);
  if (!score) {
    uiSocket.emit('status_update', { tweet_id: tweetId, status: 'score_failed' });
    pending.delete(tweetId);
    return;
  }

  const imageUrl = resolveImageUrl(score.image_choice, suggestion, tweet);
  const approved = score.deploy && score.score >= CONFIG.scoreThreshold;

  if (approved) stats.approved++;
  else stats.rejected++;

  uiSocket.emit('score_result', {
    tweet_id: tweetId,
    score,
    approved,
    imageUrl,
  });

  uiSocket.emit('stats', stats);

  // 3. Deploy
  if (approved && CONFIG.autoDeploy) {
    uiSocket.emit('status_update', { tweet_id: tweetId, status: 'deploying' });
    const result = await deployToken({
      token_name: score.token_name,
      token_ticker: score.token_ticker,
      image_choice: score.image_choice,
      imageUrl,
    });
    stats.deployed++;
    uiSocket.emit('deploy_result', { tweet_id: tweetId, result });
    uiSocket.emit('stats', stats);
  }

  pending.delete(tweetId);
}

// --- UI socket events ---
uiSocket.on('connection', (socket) => {
  socket.emit('config', {
    autoDeploy: CONFIG.autoDeploy,
    scoreThreshold: CONFIG.scoreThreshold,
    deployType: CONFIG.deployType,
    buyAmount: CONFIG.buyAmount,
  });
  socket.emit('stats', stats);
  socket.emit('j7_status', j7.connected ? 'connected' : 'disconnected');
});

// --- Start ---
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`Dashboard: http://localhost:${PORT}`);
});
