# kalshi-ml-project-reprised 
1/16 update 

pipeline flow idea: 

Goal: turn raw market data into high-signal Slack alerts that are actually worth reading.

Core ideas (brief):

Add change detection (compare current vs last snapshot)

Set alert thresholds (ignore small/noisy moves)

Reduce noise (rate-limit, dedupe, one alert per real move)

Make Slack alerts concise + contextual, not spammy

Pipeline (simple):

Snapshot prices every N minutes

Compare to previous snapshot

If threshold hit:

Log alert to SQL

Pull recent context (news/search)

Post Slack alert with:

What changed

Why it might matter

Link to market

Slack stays read-only but intelligent.

Example alert:

⚠️ Price spike detected on TikTok (+18% in 30 min)
Recent context:
• Trump rally scheduled today in Ohio
• TikTok ban hearing trending on Google (this is where we get an alert and if a tiktok hearing is indeed happening, we put yes on tiktok being mentioned before most of the market does) 
