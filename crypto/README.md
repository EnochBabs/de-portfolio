# Crypto Market Price Pipeline

This project is a streaming pipeline.
It runs automatically every hour, pulls live cryptocurrency prices 
from a public API, and builds up a time-series dataset that grows 
richer every single run.

The data engineering challenge here is not cleaning a messy file. 
It is building infrastructure that runs without you.

---

## What this pipeline does

Every hour, automatically, without anyone pressing a button:

1. Hits the CoinGecko public API and fetches live prices for the 
   top 10 cryptocurrencies by market cap
2. Appends 10 new rows to a Bronze Delta table — one per coin
3. Reprocesses the full history through Silver, computing price 
   movement between each batch
4. Refreshes three Gold tables that analysts and dashboards query

After one month of running, the Bronze table will have approximately 
14,400 rows — a full time-series record of every price movement 
across 10 coins, captured every hour.

That is the point of streaming. You are not processing data once. 
You are building a dataset that gets more valuable over time.



The key word in Bronze is append. Every run adds to the table, 
never replaces it. This is what separates a streaming pipeline 
from a batch pipeline architecturally — you are accumulating 
history, not refreshing a snapshot.

---

## The dataset

Source: CoinGecko Markets API
URL: https://api.coingecko.com/api/v3/coins/markets
Cost: free, no API key required
Coins tracked: top 10 by market cap
Refresh: every hour via Databricks Workflows
Rows added per run: 10
Rows per day: 240 (assuming it was uninterrupted or paused)
Rows per month: ~7,200

The 10 coins tracked at time of build:
Bitcoin, Ethereum, Tether, BNB, XRP, USDC, 
Solana, TRON, Figure Heloc, Dogecoin

---

## What the Bronze layer does

Bronze is append-only. Each run fetches 10 rows from the API 
and writes them to the Delta table without touching existing data.

The batch ID is a Unix timestamp — the number of seconds since 
January 1 1970. This gives every batch a unique, automatically 
generated identifier that also tells you exactly when the data 
was collected. No manual counters, no sequences, no collisions.

Two metadata columns are added to every row:
- _ingested_at: the exact timestamp when the row was written
- _batch_id: Unix timestamp identifying which run produced this row

Bronze never gets cleaned or modified. It is the raw historical 
record of everything the API ever returned.

---

## What the Silver layer does

Silver reads the full Bronze history every run and reprocesses it.

The most interesting transformations here are not data quality fixes 
like in project 1 — the API returns clean, well-typed data. The 
interesting work is computing movement between batches.

For each coin, Silver uses a window function to look back at the 
previous batch and compute:

- previous_price: what was this coin worth last hour?
- batch_price_change: how much did it move in absolute terms?
- batch_price_change_pct: how much did it move in percentage terms?
- price_direction: UP, DOWN, or FLAT?
- is_volatile: did it move more than 0.1% in a single hour?

This is the difference between raw API data and engineered data. 
The API tells you the current price. Silver tells you the story 
of how that price got here.

The first batch per coin always has a null previous_price — 
there is nothing to compare against. This is expected behaviour 
and handled explicitly rather than hidden.

---

## What the Gold layer does

Gold builds three tables that answer real business questions.

### latest_prices

The most recent batch only. This is what a dashboard shows as 
the current state of the market. One row per coin, always fresh.

### price_movement_summary

Aggregated across all batches. For each coin:
- minimum, maximum, and average price observed
- total price range across the collection window
- count of UP, DOWN, and FLAT batches
- overall direction: BULLISH, BEARISH, or NEUTRAL
- volatility rank — which coin moved the most?

### market_dominance

Each coin's share of total market cap among the tracked coins. 
Bitcoin's dominance within this dataset tells you how much of 
the top 10 crypto market it controls.

---

## What we found

These findings are from the initial 10-batch collection window. 
They will evolve as the pipeline accumulates more history.

Bitcoin dropped from $68,452 to $68,231 during the first 
collection window — a $221 decline over 10 minutes. Every 
other coin moved less than $5 in the same period. Bitcoin 
is by far the most volatile coin in the top 10.

The market was BEARISH during collection. 4 of the 10 coins 
showed more DOWN batches than UP. No coin was BULLISH. 
6 coins were NEUTRAL — either stablecoins by design (USDT, USDC) 
or coins with prices too low to show movement at 2 decimal precision.

Bitcoin controls 63.6% of the total market cap among the 
tracked coins. Ethereum is second at 11.7%. Everything else 
is in single digits. The top 2 coins control 75% of the market.

The stablecoins — USDT and USDC — held exactly $1.00 across 
every single batch. That is them working correctly, not a 
data quality issue.

---

## How the pipeline runs automatically

This pipeline does not need anyone to press a button.

A Databricks Workflow runs the three notebooks in sequence 
every hour:

If Bronze fails, Silver and Gold do not run. If Silver fails, 
Gold does not run. Each task depends on the one before it 
completing successfully.

The job runs on Serverless compute — it spins up instantly, 
runs for approximately 30 seconds, and shuts down. No cluster 
sitting idle between runs. No wasted cost.

Estimated monthly cost: under $3.

---

## What I learned building this

Streaming and batch are architecturally different from the start.
In batch you design around a file. In streaming you design around 
time. Every decision — how you generate batch IDs, how you compute 
movement, how you handle the first batch — is a time-based decision.

Append mode changes how you think about data.
In project every run adds to it. 
That means your Bronze table is also your audit log, your history, 
and your recovery point all at once. You never lose a data point.

Window functions are how you connect time-series rows.
The lag() function — looking back one row per coin ordered by batch — 
is what makes movement calculation possible. Without it you have 
a collection of prices. With it you have a story of price change.

Orchestration is what makes a pipeline real.
Writing notebooks that run manually is not a pipeline. 
A pipeline runs on a schedule, handles failures, sends alerts, 
and delivers data without human intervention. Databricks Workflows 
is what turned these three notebooks into an actual pipeline.

Cost awareness matters from day one.
Running a pipeline every hour on an interactive cluster would 
cost significantly more than Serverless. Understanding the 
difference between DBU types, cluster startup costs, and 
Serverless billing is not optional knowledge for a DE.

---

## Azure stack

| Service | Name | Purpose |
|---|---|---|
| ADLS Gen2 | theportfoliostorage | Delta Lake storage |
| Azure Databricks | de-portfolio-dbx | Compute + notebooks |
| Azure Key Vault | deportfolio-kv | Secret management |
| Unity Catalog | crypto catalog | Data governance |
| Databricks Workflows | crypto-price-pipeline | Orchestration |

---

