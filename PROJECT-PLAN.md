# Edge CDN Video Cache & Streaming Analytics Warehouse

## Project Overview

An experimental edge Content Delivery Network (CDN) caching proxy for HLS video streaming, paired with a Star Schema data warehouse that ingests delivery logs to power descriptive analytics on cache efficiency, viewer behavior, and backhaul bandwidth savings.

The project simulates the architecture used by transit-based edge CDN providers (e.g., Netskrt) where storage-constrained edge nodes on trains, buses, or aircraft must intelligently cache popular video content while minimizing expensive upstream backhaul over cellular/satellite links.

### Course Coverage

| Requirement | How This Project Hits It |
|---|---|
| **COMP-7171** – Database Systems for the Edge, Content & Multimedia DB Systems | Edge caching proxy with embedded SQLite metadata store, video-aware eviction policies |
| **COMP-8071** – Data Warehouse with Descriptive Analytics | Star schema warehouse over CDN delivery logs; OLAP rollup/drilldown queries; ETL pipeline |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LOCAL DOCKER COMPOSE                         │
│                                                                     │
│  ┌──────────────────┐        ┌──────────────────────────────────┐  │
│  │  Origin Server   │◄───────│  Edge Cache Node                 │  │
│  │  (Nginx)         │ backhaul│  ┌────────────────────────────┐ │  │
│  │                  │ (throttled)│  FastAPI Caching Proxy     │ │  │
│  │  Serves full HLS │        │  │  - Route: GET /hls/{path}  │ │  │
│  │  library from    │        │  │  - Cache HIT → serve local │ │  │
│  │  mounted volume  │        │  │  - Cache MISS → fetch      │ │  │
│  │                  │        │  │    origin, store, log       │ │  │
│  └──────────────────┘        │  │  - Eviction engine (LRU    │ │  │
│                              │  │    vs. Video-Aware)        │ │  │
│                              │  └────────────────────────────┘ │  │
│                              │  ┌────────────────────────────┐ │  │
│                              │  │  SQLite (WAL mode)         │ │  │
│                              │  │  - chunks table            │ │  │
│                              │  │  - delivery_log table      │ │  │
│                              │  └────────────────────────────┘ │  │
│                              │  ┌────────────────────────────┐ │  │
│                              │  │  Local Cache Volume        │ │  │
│                              │  │  (capped at 200MB)         │ │  │
│                              │  └────────────────────────────┘ │  │
│                              └──────────────────────────────────┘  │
│                                          │                         │
│                                          │ delivery logs (CSV/SQL) │
│                                          ▼                         │
│                              ┌──────────────────────────────────┐  │
│                              │  Analytics Warehouse (DuckDB)    │  │
│                              │  - Star Schema                   │  │
│                              │  - OLAP queries                  │  │
│                              │  - Python plotting (matplotlib)  │  │
│                              └──────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Traffic Simulator (Python script)                           │   │
│  │  - Simulates 20-50 concurrent viewers                        │   │
│  │  - Zipf popularity distribution (80/20 rule)                 │   │
│  │  - Realistic drop-off behavior (not all viewers finish)      │   │
│  │  - Multiple bitrate tiers                                    │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Edge Database Schema (SQLite)

```sql
-- Tracks every chunk the edge node knows about
CREATE TABLE chunks (
    chunk_id    TEXT PRIMARY KEY,   -- e.g. "video1/720p/seg_012.ts"
    video_id    TEXT NOT NULL,
    segment_num INTEGER NOT NULL,
    bitrate     INTEGER NOT NULL,   -- e.g. 720, 1080, 480
    file_size   INTEGER NOT NULL,   -- bytes
    is_cached   INTEGER DEFAULT 0,  -- 1 if on local disk
    hit_count   INTEGER DEFAULT 0,
    last_accessed DATETIME,
    cached_at   DATETIME
);

-- Every delivery event (feeds the warehouse ETL)
CREATE TABLE delivery_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   DATETIME DEFAULT CURRENT_TIMESTAMP,
    client_id   TEXT NOT NULL,
    chunk_id    TEXT NOT NULL,
    video_id    TEXT NOT NULL,
    segment_num INTEGER NOT NULL,
    bitrate     INTEGER NOT NULL,
    is_cache_hit INTEGER NOT NULL,  -- 1 = HIT, 0 = MISS
    latency_ms  REAL NOT NULL,
    bytes_served INTEGER NOT NULL,
    eviction_policy TEXT NOT NULL   -- "lru" or "video_aware"
);
```

---

## Warehouse Star Schema (DuckDB)

### Fact Table

```sql
CREATE TABLE fact_chunk_delivery (
    delivery_id       INTEGER PRIMARY KEY,
    time_key          INTEGER REFERENCES dim_time(time_key),
    content_key       INTEGER REFERENCES dim_content(content_key),
    client_key        INTEGER REFERENCES dim_client(client_key),
    eviction_policy   TEXT,          -- "lru" or "video_aware"
    is_cache_hit      INTEGER,       -- 1/0
    latency_ms        REAL,
    bytes_served      INTEGER,
    backhaul_bytes    INTEGER        -- 0 if HIT, bytes_served if MISS
);
```

### Dimension Tables

```sql
CREATE TABLE dim_time (
    time_key      INTEGER PRIMARY KEY,
    timestamp     DATETIME,
    hour          INTEGER,
    minute        INTEGER,
    is_peak       INTEGER     -- 1 if during peak viewing window
);

CREATE TABLE dim_content (
    content_key   INTEGER PRIMARY KEY,
    video_id      TEXT,
    video_title   TEXT,
    genre         TEXT,       -- e.g. "action", "documentary"
    duration_sec  INTEGER,
    segment_num   INTEGER,
    bitrate       INTEGER,
    bitrate_tier  TEXT        -- "low" / "mid" / "high"
);

CREATE TABLE dim_client (
    client_key    INTEGER PRIMARY KEY,
    client_id     TEXT,
    session_id    TEXT
);
```

---

## OLAP Queries

### Query 1 – Cache Hit Rate & Bandwidth Savings by Content and Hour (Rollup)

> "For each video title and hour of day, what is the cache hit percentage and total backhaul bandwidth saved? Roll up to see totals across all content."

```sql
SELECT
    dc.video_title,
    dt.hour,
    COUNT(*)                                          AS total_requests,
    SUM(f.is_cache_hit)                               AS cache_hits,
    ROUND(100.0 * SUM(f.is_cache_hit) / COUNT(*), 1)  AS hit_rate_pct,
    SUM(f.bytes_served) - SUM(f.backhaul_bytes)        AS backhaul_bytes_saved
FROM fact_chunk_delivery f
JOIN dim_content dc ON f.content_key = dc.content_key
JOIN dim_time dt    ON f.time_key = dt.time_key
GROUP BY ROLLUP (dc.video_title, dt.hour)
ORDER BY dc.video_title, dt.hour;
```

### Query 2 – Viewer Drop-Off Analysis by Segment (Drilldown)

> "For each video, at what segment number do viewers tend to stop watching? Compare engagement depth between high and low bitrate tiers."

```sql
SELECT
    dc.video_id,
    dc.bitrate_tier,
    dc.segment_num,
    COUNT(DISTINCT f.client_key) AS unique_viewers
FROM fact_chunk_delivery f
JOIN dim_content dc ON f.content_key = dc.content_key
GROUP BY dc.video_id, dc.bitrate_tier, dc.segment_num
ORDER BY dc.video_id, dc.bitrate_tier, dc.segment_num;
```

### Query 3 – LRU vs. Video-Aware Policy Comparison (The Experiment)

> "Compare average latency and cache hit rate between LRU and Video-Aware eviction policies across cache utilization levels."

```sql
SELECT
    f.eviction_policy,
    ROUND(100.0 * SUM(f.is_cache_hit) / COUNT(*), 1)  AS hit_rate_pct,
    ROUND(AVG(f.latency_ms), 2)                        AS avg_latency_ms,
    SUM(f.backhaul_bytes) / (1024 * 1024)              AS total_backhaul_mb
FROM fact_chunk_delivery f
GROUP BY f.eviction_policy;
```

---

## Eviction Policies (The Experiment)

### Policy A: Naive LRU (Baseline)

Standard Least Recently Used. When the cache disk limit is reached, evict the chunk whose `last_accessed` timestamp is oldest. No awareness of video structure.

### Policy B: Video-Aware (Experimental)

Three improvements over LRU:

1. **Sequential Prefetch:** When client requests segment `N`, proactively fetch segment `N+1` from origin in the background (since HLS playback is sequential and predictable).
2. **Popularity-Weighted Retention:** Chunks from videos with high `hit_count` are penalized less during eviction. Early segments (seg 0–3) of popular videos are "pinned" longer since every new viewer needs them.
3. **Abandoned Tail Pruning:** If a video's later segments (e.g., seg 40+) have zero hits in the last window while its early segments are active, evict the tail first — those viewers already dropped off.

---

## Technology Stack

| Component | Technology | Justification |
|---|---|---|
| Edge Caching Proxy | Python + FastAPI | Fast to build, async HTTP, perfect for prototype |
| Edge Metadata Store | SQLite (WAL mode) | Embedded, zero-config, models real edge constraints |
| Origin Server | Nginx (static file server) | Dead simple, serves HLS chunks from a volume mount |
| Video Segmentation | ffmpeg | Industry standard for HLS chunking |
| Traffic Simulation | Python (asyncio + httpx) | Async client sim with configurable viewer profiles |
| Data Warehouse | DuckDB | Embedded analytical DB, columnar, fast OLAP, zero setup |
| ETL | Python script | Reads SQLite delivery_log → transforms → loads DuckDB star schema |
| Plotting / Reporting | Matplotlib / Seaborn | Publication-quality charts for the final report |
| Infrastructure | Docker Compose | Reproducible, self-contained, runs on any laptop |
| Backhaul Simulation | Docker network + `tc` or `toxiproxy` | Adds realistic latency between edge ↔ origin |

---

## Data Sources

All data is **synthetically generated** by the traffic simulator with realistic distributions:

- **Video Library:** 3–5 open-source/Creative Commons videos segmented into HLS chunks via `ffmpeg`
  - Big Buck Bunny, Tears of Steel, Sintel (Blender Foundation — freely redistributable)
- **Client Traffic:** Zipf-distributed popularity (a few videos get ~80% of requests)
- **Viewer Behavior:** Exponential drop-off model (most viewers don't finish; average watch ~40–60% of a video)
- **Bitrate Tiers:** 3 tiers per video (480p, 720p, 1080p) to exercise bitrate-aware eviction

---

## 5-Week Implementation Schedule (~25 hours total)

### Week 1 (Nov 3–9): Setup & Media Preparation — 5 hrs

- [ ] Initialize project repo structure under `data-center-design-COMP-7171/project/`
- [ ] Download 3 Creative Commons test videos (Big Buck Bunny, Tears of Steel, Sintel)
- [ ] Segment each into HLS chunks at 3 bitrate tiers using `ffmpeg`
- [ ] Write `docker-compose.yml` with Origin (Nginx) and Edge (Python) containers
- [ ] Create SQLite schema (`chunks`, `delivery_log` tables)
- [ ] Verify Origin serves chunks and Edge container can reach it

### Week 2 (Nov 10–16): Edge Cache Proxy (LRU Baseline) — 5 hrs

- [ ] Build FastAPI proxy: `GET /hls/{video_id}/{bitrate}/{segment}` 
- [ ] Implement cache storage with disk quota enforcement (200MB cap)
- [ ] Implement LRU eviction (evict oldest `last_accessed` when quota full)
- [ ] Log every request to `delivery_log` table
- [ ] Basic smoke test: curl a few chunks, verify HIT/MISS behavior

### Week 3 (Nov 17–23): Video-Aware Policy + Traffic Simulator — 5 hrs

- [ ] Implement Video-Aware eviction policy (prefetch, popularity weighting, tail pruning)
- [ ] Add CLI flag / env var to switch between `lru` and `video_aware` policies
- [ ] Build traffic simulator script:
  - Zipf popularity distribution across videos
  - Exponential viewer drop-off model
  - 20–50 concurrent async clients
  - Configurable run duration and request rate

### Week 4 (Nov 24–30): Warehouse ETL + OLAP Queries + Benchmarks — 5 hrs

- [ ] Run traffic simulator against LRU policy → export delivery_log
- [ ] Run traffic simulator against Video-Aware policy → export delivery_log
- [ ] Build ETL script: SQLite `delivery_log` → DuckDB star schema (fact + 3 dims)
- [ ] Write and execute the 3 OLAP queries (rollup, drilldown, policy comparison)
- [ ] Generate benchmark plots:
  - Cache Hit Rate (%) — LRU vs. Video-Aware (bar chart)
  - Backhaul Bandwidth Saved (MB) — by hour and policy (line chart)
  - Viewer Drop-Off Curve — segment number vs. unique viewers (line chart)

### Week 5 (Dec 1–7): Report, Presentation & Polish — 5 hrs

- [ ] Write final project report:
  - Problem statement & motivation (CDN edge caching in transit environments)
  - Architecture diagram
  - Database schemas (edge SQLite + warehouse star schema)
  - Experimental methodology
  - Results with annotated charts
  - Conclusions and future work
- [ ] Prepare presentation slides (10–15 min demo)
- [ ] Record or script a live demo walkthrough
- [ ] Clean up repo, write README with setup instructions

---

## Expected Deliverables

1. **Working Docker Compose environment** — `docker compose up` runs the full stack
2. **Edge caching proxy** with switchable LRU / Video-Aware eviction
3. **Traffic simulator** generating realistic HLS streaming workloads
4. **DuckDB data warehouse** with populated star schema
5. **3 OLAP queries** with results
6. **2–3 benchmark charts** comparing eviction policies
7. **Project report** (PDF)
8. **Presentation slides**

---

## File Structure

```
project/
├── PROJECT-PLAN.md            ← this file
├── docker-compose.yml
├── origin/
│   ├── nginx.conf
│   └── media/                 ← HLS chunks go here after ffmpeg
│       ├── bunny/
│       │   ├── 480p/
│       │   ├── 720p/
│       │   └── 1080p/
│       └── sintel/
│           └── ...
├── edge/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                ← FastAPI caching proxy
│   ├── cache_manager.py       ← LRU + Video-Aware eviction logic
│   ├── db.py                  ← SQLite schema & queries
│   └── config.py              ← Cache size limit, policy selection
├── simulator/
│   ├── simulate.py            ← Traffic generator (asyncio + httpx)
│   └── profiles.py            ← Zipf distribution, drop-off model
├── warehouse/
│   ├── etl.py                 ← SQLite → DuckDB star schema loader
│   ├── schema.sql             ← DuckDB warehouse DDL
│   ├── queries.sql            ← The 3 OLAP queries
│   └── plots.py               ← Matplotlib chart generation
├── docs/
│   ├── report.md
│   └── slides/
└── README.md
```
