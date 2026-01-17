**Project Summary**

This project builds an automated pipeline for discovering, tracking, and monitoring Kalshi “mention” markets (e.g. What will Trump say this week?), which are series-driven and not discoverable via generic pagination.

The system:

Identifies mention markets using hard-coded series prefixes + semantic tagging

Fetches current, active contracts across politics, sports, and media

Groups contracts into events and extracts price-level signals

Sends structured, readable updates to Slack

Lays the groundwork for price-change alerts, context enrichment, and ML

What’s working now

Reliable discovery of mention markets via series_ticker (not pagination)

Semantic classification of markets (mention, speech-driven, time-bucketed, industry)

Filtering to currently open events only

Extraction of top contracts and prices per event

Slack bot that posts clean, human-readable market summaries

Why this matters

Mention markets are highly event-driven and reactive to real-world news, making them ideal candidates for:

Price-spike detection

News-driven signal generation

Discrete-state / regime-based modeling (e.g. Markov-style transitions)

Near-term roadmap

Cache price snapshots and detect significant deltas

Trigger alerts only when changes cross meaningful thresholds

Enrich alerts with automated context (news / search signals)

Store price movements and outcomes in SQL for analysis and ML training

Improve Slack alerts to reduce noise and surface only actionable events

The long-term goal is an intelligent, read-only trading assistant that surfaces high-signal Kalshi opportunities before they fully reprice.
