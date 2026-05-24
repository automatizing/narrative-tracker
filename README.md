# Narrative Tracker

A Twitter briefing bot that watches j7tracker's tweet stream, clusters trending topics every few hours, and uses an LLM to write a build-opportunity briefing aimed at product managers and engineers.

## What it does

The bot keeps a persistent WebSocket connection to `nyc.j7tracker.io`. Every incoming tweet event (a `tweet_id` plus a topic prediction from j7's per-tweet AI labeling) is stored on disk. On a UTC clock-aligned schedule, the bot:

1. Groups tweets from the previous window by normalized topic prediction.
2. Hydrates the strongest clusters with actual tweet text via fxtwitter.
3. Calls NVIDIA NIM (Kimi K2.6) with `stream: true`.
4. Streams the partial markdown to any connected `/live` page in real time.
5. Saves the final briefing as a markdown file in `briefings/`.

The briefing prompt is tuned for PM use. Every section names a domain (tech, crypto, trading, etc.), lists two to four concrete build angles, and gives a speed call (weekend ship, two week build, or multi month bet). Topics with no actionable angle get skipped.

## Pages

| Path | What |
| --- | --- |
| `/` | Latest saved briefing, with a sidebar listing all past briefings |
| `/live` | In progress briefing streamed token by token, or an idle screen if no briefing is running |
| `/feed` | Real time stream of incoming tweet events |
| `/b/:stamp` | A specific saved briefing |
| `/b/:stamp/download` | Raw markdown download with attachment headers |

## Local setup

Prerequisites: Node 20 or later.

```
git clone <this repo>
cd narrative-tracker
npm install
```

Create a `.env` file at the project root with at least:

```
NVIDIA_API_KEY=your_nim_key
```

Then:

```
node bot.js
```

Open http://localhost:3000.

Helper commands:

```
node bot.js stats   # print current window stats
node bot.js brief   # force one briefing run and exit
```

## Environment variables

| Key | Default | Notes |
| --- | --- | --- |
| `NVIDIA_API_KEY` | required | NIM key, used for the briefing LLM call |
| `WINDOW_HOURS` | 6 | Tweet collection window |
| `BRIEFING_INTERVAL_HOURS` | 6 | How often briefings fire (aligned to UTC clock boundaries) |
| `MIN_TWEETS_PER_CLUSTER` | 3 | Minimum tweet count for a topic to make the briefing |
| `MAX_CLUSTERS` | 12 | Cap on topics passed to the LLM |
| `MAX_HYDRATE` | 12 | Sample size hydrated per cluster |
| `PORT` | 3000 | HTTP server port (Railway injects this) |
| `DATA_DIR` | `./data` | Where `tweets.jsonl` lives |
| `BRIEFINGS_DIR` | `./briefings` | Where briefing markdown files are saved |
| `BASIC_AUTH_USER` | (unset) | Set together with `BASIC_AUTH_PASS` to enable HTTP basic auth |
| `BASIC_AUTH_PASS` | (unset) | If both unset, server is open |

## Deploying to Railway

1. Push this repo to GitHub.
2. railway.app, New Project, Deploy from GitHub repo.
3. In the service, Variables tab, set the env vars above. Do not set `PORT`, Railway injects it.
4. Settings, Volumes, attach a volume at mount path `/data`.
5. Set `DATA_DIR=/data/tweets` and `BRIEFINGS_DIR=/data/briefings` so collected tweets and saved briefings survive across deploys.
6. Settings, Networking, Generate Domain.
7. Open the domain. If basic auth is on, log in. Done.

## Architecture notes

* Tweets are stored as JSON lines in `data/tweets.jsonl`. The file is pruned to the last 48h on each briefing run.
* Briefings are persistent markdown files in `briefings/`. Nothing deletes them automatically.
* The live writing page uses Server Sent Events for token streaming. The live feed page uses SSE for tweet events.
* LLM output is sanitized with DOMPurify on both the server and the client before being inserted into the DOM.
* HTTP basic auth uses constant time string comparison.

## Customization

The two pieces most worth tweaking live in `bot.js`:

* The clustering logic in `clusterTweets()` and `hydrateCluster()`. Change the threshold, sampling, or grouping if your tweet stream looks different.
* The briefing prompt inside `generateBriefing()`. Adjust the tone, structure, and the domain tags to match your team.

## License

Pick whatever license fits your use. Add a `LICENSE` file before sharing widely.
