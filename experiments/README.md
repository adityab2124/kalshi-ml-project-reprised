scaling goes like: https://docs.google.com/spreadsheets/d/1rpYTXEB-waNgf1h_NyegB-TRW2ysbmxM6XNXwSfZwx0/edit?pli=1&gid=1186098886#gid=1186098886
 kalshi-ml-project-reprised 

1/18 

well right now, were focusing on speech markets becuase its just an area of focus. if i have proof of concept we can very much expand in the future. my goal is something like this:



Example alert:

⚠️ Price spike detected on TikTok (+18% in 30 min) Recent context: • Trump rally scheduled today in Ohio • TikTok ban hearing trending on Google (this is where we get an alert and if a tiktok hearing is indeed happening, we put yes on tiktok being mentioned before most of the market does) (this is a reactive method)


in the future we can ask ai gpt token to quick search the internet and find alerts faster than anybody (this is a proactive method)

i think a reactive method is much easier to implement right now



but even with the reactive method, we have to constantly refresh odds on kalshi, like every minute. lets say a mention market lasts a week. and for that week, we have to refresh every minute , just to wait for that one moment where we see a price spike... whats ur thought ... brief answe

1/16 update 

right now: it seems like pulling mention bet, then recording it through cache, sseeing price alert, which would trigger ai to pull news sources to pissibly what could cause this price spike. we validate possibly through just ourselves by double checking then order a bet. but after that process actually works, the ml can distingusih between bs and potential money makers, which wpuld make it better for us cuz we can do less manual checking . and then of course outcome modeling, but i think outcome modeling is whole other process given different types of markets, features, volatility etc 

**so main goals: have slack messages be better and detailed cache price differences alert when price spikes or goes down noticeably search for reason, using ai tokens store these price spikes in sql and outcome as well so we can use this to train ml models**


**In depth version**


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


random thoughts 

1/15/26

something cool too to add would be like lets say theres a bet on what trumpwill say this week and then somehow we can cross reference the date into a google search or use ai tokens and then we'll know hey the olympics are coming up this week might be a good idea to bet on olympic terms being sad or somehting. just an idea

discretization values, finding pattern, use CSSR for potential better historical patterns, tbd
