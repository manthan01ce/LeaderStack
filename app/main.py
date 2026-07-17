"""
Leaderboard API — FastAPI + Redis Sorted Sets  v3.0
====================================================

Features added in v3 (Contest-Aware Architecture):
  - Support for multiple contests (upcoming, live, ended)
  - Backward compatibility for global board via "global" default
  - Contest metadata endpoints
"""

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import redis
from fastapi import FastAPI, HTTPException, Query, Request, Path, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, field_validator, model_validator

# ── Global Exception Handler ──────────────────────────────────────
async def redis_exception_handler(request: Request, exc: redis.RedisError):
    return JSONResponse(
        status_code=503,
        content={"detail": "Service temporarily unavailable. Database connection failed."}
    )

# ── Configuration ─────────────────────────────────────────────────
REDIS_URL       = os.getenv("REDIS_URL")
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", 6379))
MAX_SCORE       = 1000
ACTIVITY_MAX    = 100

RATE_LIMIT_MAX    = 10
RATE_LIMIT_WINDOW = 60

DAILY_TTL  = 48 * 3600
WEEKLY_TTL = 10 * 86400

# ── Contest Metadata ──────────────────────────────────────────────
DEMO_CONTESTS = {
    "global": {
        "contestId": "global",
        "name": "Global Leaderboard",
        "startTime": "2000-01-01T00:00:00Z",
        "endTime": "2099-12-31T23:59:59Z",
        "maxScore": 1000,
        "expectedUsers": 1000,
        "description": ""
    },
    "contest-live": {
        "contestId": "contest-live",
        "name": "Summer Hackathon",
        "startTime": "2026-07-01T00:00:00Z",
        "endTime": "2026-07-31T23:59:59Z",
        "maxScore": 1000,
        "expectedUsers": 1000,
        "description": ""
    },
    "contest-upcoming": {
        "contestId": "contest-upcoming",
        "name": "Winter CodeFest",
        "startTime": "2026-12-01T00:00:00Z",
        "endTime": "2026-12-31T23:59:59Z",
        "maxScore": 1000,
        "expectedUsers": 1000,
        "description": ""
    },
    "contest-ended": {
        "contestId": "contest-ended",
        "name": "Spring Challenge",
        "startTime": "2026-03-01T00:00:00Z",
        "endTime": "2026-03-31T23:59:59Z",
        "maxScore": 1000,
        "expectedUsers": 1000,
        "description": ""
    }
}

if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
else:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
_sse_queues: list[asyncio.Queue] = []
_event_loop: asyncio.AbstractEventLoop | None = None
_set_score_script = None

LUA_SET_SCORE = """
local all_key = KEYS[1]
local daily_key = KEYS[2]
local weekly_key = KEYS[3]
local metrics_key = KEYS[4]
local activity_key = KEYS[5]
local meta_key = KEYS[6]

local contest_id = ARGV[1]
local participant_id = ARGV[2]
local composite_score = tonumber(ARGV[3])
local int_score = tonumber(ARGV[4])
local max_score = tonumber(ARGV[5])
local activity_max = tonumber(ARGV[6])
local daily_ttl = tonumber(ARGV[7])
local weekly_ttl = tonumber(ARGV[8])
local timestamp = ARGV[9]

if int_score > max_score then
    redis.call("HINCRBY", metrics_key, "rejected_submissions", 1)
    redis.call("HINCRBY", metrics_key, "total_submissions", 1)
    return cjson.encode({error = "Score " .. int_score .. " exceeds contest max score " .. max_score})
end

local existing_composite = redis.call("ZSCORE", all_key, participant_id)
local current_best = nil
if existing_composite then
    current_best = math.floor(tonumber(existing_composite))
end

if current_best and int_score <= current_best then
    redis.call("HINCRBY", metrics_key, "rejected_submissions", 1)
    redis.call("HINCRBY", metrics_key, "total_submissions", 1)
    return cjson.encode({error = "Score " .. int_score .. " does not improve current best."})
end

local rank_before = redis.call("ZREVRANK", all_key, participant_id)
local prev_rank = nil
if rank_before then prev_rank = rank_before + 1 end

redis.call("ZADD", all_key, composite_score, participant_id)
redis.call("ZADD", daily_key, composite_score, participant_id)
redis.call("ZADD", weekly_key, composite_score, participant_id)
redis.call("EXPIRE", daily_key, daily_ttl)
redis.call("EXPIRE", weekly_key, weekly_ttl)

local new_rank_raw = redis.call("ZREVRANK", all_key, participant_id)
local new_rank = new_rank_raw + 1
local rank_movement = nil
if prev_rank and new_rank then rank_movement = prev_rank - new_rank end

redis.call("HSET", meta_key, participant_id, cjson.encode({timestamp = timestamp, rankMovement = rank_movement or 0}))

redis.call("HINCRBY", metrics_key, "accepted_submissions", 1)
redis.call("HINCRBY", metrics_key, "total_submissions", 1)

local rank_movement = nil
if prev_rank and new_rank then rank_movement = prev_rank - new_rank end
local delta = nil
if current_best then delta = int_score - current_best end

local activity_dict = {
    contestId = contest_id,
    participantId = participant_id,
    previousScore = current_best,
    newScore = int_score,
    previousRank = prev_rank,
    newRank = new_rank,
    rankMovement = rank_movement,
    delta = delta,
    board = "all",
    timestamp = timestamp,
    source = "admin"
}
local activity_json = cjson.encode(activity_dict)

redis.call("LPUSH", activity_key, activity_json)
redis.call("LTRIM", activity_key, 0, activity_max - 1)

return cjson.encode({
    success = true,
    activity = activity_dict,
    rank = new_rank
})
"""

LUA_INGEST_EVENT = """
local all_key = KEYS[1]
local daily_key = KEYS[2]
local weekly_key = KEYS[3]
local metrics_key = KEYS[4]
local activity_key = KEYS[5]
local dedupe_key = KEYS[6]
local meta_key = KEYS[7]

local contest_id = ARGV[1]
local participant_id = ARGV[2]
local score_value = tonumber(ARGV[3])
local score_mode = ARGV[4]
local max_score = tonumber(ARGV[5])
local activity_max = tonumber(ARGV[6])
local daily_ttl = tonumber(ARGV[7])
local weekly_ttl = tonumber(ARGV[8])
local timestamp = ARGV[9]
local event_id = ARGV[10]
local source = ARGV[11]
local dedupe_ttl = tonumber(ARGV[12])
local timestamp_ms = tonumber(ARGV[13])

local is_duplicate = redis.call("SET", dedupe_key, "COMPLETED", "NX", "EX", dedupe_ttl)
if not is_duplicate then
    redis.call("HINCRBY", metrics_key, "duplicate_ingested_events", 1)
    return cjson.encode({status = "duplicate"})
end

redis.call("HINCRBY", metrics_key, "total_ingested_events", 1)

local existing_composite = redis.call("ZSCORE", all_key, participant_id)
local current_best = nil
if existing_composite then
    current_best = math.floor(tonumber(existing_composite))
end

local int_score = score_value
if score_mode == "delta" then
    if score_value < 0 then
        redis.call("HINCRBY", metrics_key, "rejected_ingested_events", 1)
        return cjson.encode({status = "rejected", reason = "negative_delta_not_allowed"})
    end
    if current_best then
        int_score = current_best + score_value
    end
end

if int_score > max_score then
    redis.call("HINCRBY", metrics_key, "rejected_ingested_events", 1)
    return cjson.encode({status = "rejected", reason = "exceeds_max_score", computed_score=int_score})
end

if score_mode == "absolute" and current_best and int_score <= current_best then
    redis.call("HINCRBY", metrics_key, "rejected_ingested_events", 1)
    return cjson.encode({status = "rejected", reason = "does_not_improve_best", computed_score=int_score})
end

local composite_score = int_score + (1.0 / timestamp_ms)

local rank_before = redis.call("ZREVRANK", all_key, participant_id)
local prev_rank = nil
if rank_before then prev_rank = rank_before + 1 end

redis.call("ZADD", all_key, composite_score, participant_id)
redis.call("ZADD", daily_key, composite_score, participant_id)
redis.call("ZADD", weekly_key, composite_score, participant_id)
redis.call("EXPIRE", daily_key, daily_ttl)
redis.call("EXPIRE", weekly_key, weekly_ttl)

local new_rank_raw = redis.call("ZREVRANK", all_key, participant_id)
local new_rank = new_rank_raw + 1

redis.call("HINCRBY", metrics_key, "accepted_ingested_events", 1)
-- Also increment standard metrics
redis.call("HINCRBY", metrics_key, "accepted_submissions", 1)
redis.call("HINCRBY", metrics_key, "total_submissions", 1)

local rank_movement = nil
if prev_rank and new_rank then rank_movement = prev_rank - new_rank end

redis.call("HSET", meta_key, participant_id, cjson.encode({timestamp = timestamp, rankMovement = rank_movement or 0}))

local delta = nil
if current_best then delta = int_score - current_best end

local activity_dict = {
    eventId = event_id,
    source = source,
    contestId = contest_id,
    participantId = participant_id,
    previousScore = current_best,
    newScore = int_score,
    previousRank = prev_rank,
    newRank = new_rank,
    rankMovement = rank_movement,
    delta = delta,
    board = "all",
    timestamp = timestamp
}
local activity_json = cjson.encode(activity_dict)

redis.call("LPUSH", activity_key, activity_json)
redis.call("LTRIM", activity_key, 0, activity_max - 1)

return cjson.encode({
    status = "accepted",
    activity = activity_dict,
    rank = new_rank
})
"""

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def internal_get_contest_meta(contest_id: str) -> dict | None:
    try:
        raw = r.hgetall(f"contest:{contest_id}:meta")
        if not raw:
            return None
        if "maxScore" in raw: raw["maxScore"] = int(raw["maxScore"])
        if "expectedUsers" in raw: raw["expectedUsers"] = int(raw["expectedUsers"])
        return raw
    except redis.RedisError:
        return None

def get_contest_status(contest_id: str) -> str:
    c = internal_get_contest_meta(contest_id)
    if not c:
        return "not_found"
    now = now_utc()
    try:
        start = datetime.fromisoformat(c["startTime"].replace('Z', '+00:00'))
        end = datetime.fromisoformat(c["endTime"].replace('Z', '+00:00'))
    except ValueError:
        return "not_found"
    if now < start:
        return "upcoming"
    if now > end:
        return "ended"
    return "live"

def encode_score(int_score: int) -> float:
    return float(int_score) + (1.0 / int(time.time() * 1000))

def decode_score(stored: float) -> int:
    return int(stored)

def board_key(contest_id: str, board: str) -> str:
    now = now_utc()
    if board == "daily":
        return f"leaderboard:{contest_id}:daily:{now.strftime('%Y-%m-%d')}"
    if board == "weekly":
        year, week, _ = now.isocalendar()
        return f"leaderboard:{contest_id}:weekly:{year}-W{week:02d}"
    return f"leaderboard:{contest_id}:all"

def get_activity_key(contest_id: str) -> str:
    return f"activity:{contest_id}:log"

def get_metrics_key(contest_id: str) -> str:
    return f"metrics:{contest_id}"

def check_rate_limit(key: str, limit: int, window: int) -> bool:
    try:
        count = r.incr(key)
        if count == 1:
            r.expire(key, window)
        return count <= limit
    except redis.RedisError:
        return True # Fail open gracefully

def RateLimit(limit: int, window: int, scope: str = "global"):
    async def _limit(request: Request):
        ip = request.client.host if request.client else "127.0.0.1"
        key = f"rate:{scope}:{ip}"
        if not check_rate_limit(key, limit, window):
            raise HTTPException(status_code=429, detail="Rate limit exceeded.")
    return _limit

def incr_metric(contest_id: str, field: str, amount: int = 1) -> None:
    try:
        r.hincrby(get_metrics_key(contest_id), field, amount)
    except redis.RedisError:
        pass

def log_activity(contest_id: str, participant_id: str, prev_score: int | None, new_score: int, prev_rank: int | None, new_rank: int | None, board: str) -> dict:
    rank_movement = (prev_rank - new_rank) if (prev_rank is not None and new_rank is not None) else None
    entry = {
        "contestId": contest_id,
        "participantId": participant_id,
        "previousScore": prev_score,
        "newScore": new_score,
        "previousRank": prev_rank,
        "newRank": new_rank,
        "rankMovement": rank_movement,
        "delta": (new_score - prev_score) if prev_score is not None else None,
        "board": board,
        "timestamp": now_utc().isoformat(),
    }
    k = get_activity_key(contest_id)
    try:
        r.lpush(k, json.dumps(entry))
        r.ltrim(k, 0, ACTIVITY_MAX - 1)
    except redis.RedisError:
        pass
    return entry

def broadcast_event(data: dict) -> None:
    if _event_loop is None:
        return
    payload = json.dumps(data)
    for q in list(_sse_queues):
        _event_loop.call_soon_threadsafe(q.put_nowait, payload)

DEMO_PARTICIPANTS = [
    ("alice",   300),
    ("bob",     275),
    ("charlie", 250),
    ("diana",   225),
    ("eve",     200),
]

def seed_if_empty() -> None:
    try:
        if r.scard("contests:list") == 0:
            for cid, meta in DEMO_CONTESTS.items():
                r.sadd("contests:list", cid)
                r.hset(f"contest:{cid}:meta", mapping=meta)

        for cid in ["global", "contest-live", "contest-ended"]:
            if r.zcard(board_key(cid, "all")) == 0:
                for pid, score in DEMO_PARTICIPANTS:
                    composite = encode_score(score)
                    r.zadd(board_key(cid, "all"),     {pid: composite})
                    r.zadd(board_key(cid, "daily"),  {pid: composite})
                    r.zadd(board_key(cid, "weekly"), {pid: composite})
                    time.sleep(0.002)
                r.expire(board_key(cid, "daily"),  DAILY_TTL)
                r.expire(board_key(cid, "weekly"), WEEKLY_TTL)
    except redis.RedisError:
        pass

def check_and_generate_notifications():
    try:
        cids = r.smembers("contests:list")
        now = now_utc()
        for cid in cids:
            meta = internal_get_contest_meta(cid)
            if not meta: continue
            try:
                start = datetime.fromisoformat(meta["startTime"].replace('Z', '+00:00'))
                end = datetime.fromisoformat(meta["endTime"].replace('Z', '+00:00'))
            except ValueError:
                continue
                
            thresholds = [
                ("start_24h", start, 24 * 3600, "upcoming", "starts"),
                ("start_1h", start, 3600, "upcoming", "starts"),
                ("end_1h", end, 3600, "ending", "ends"),
                ("end_15m", end, 900, "ending", "ends"),
                ("ended", end, 0, "ended", "ended")
            ]
            
            for t_key, target_time, max_seconds_left, notif_type, verb in thresholds:
                diff = (target_time - now).total_seconds()
                valid = (diff <= 0 and diff > -86400) if t_key == "ended" else (0 <= diff <= max_seconds_left)
                
                if valid:
                    dedupe_key = f"notified:{cid}:{t_key}"
                    if not r.exists(dedupe_key):
                        r.set(dedupe_key, "1", ex=7*86400)
                        msg = f"Contest '{meta['name']}' {verb}"
                        if t_key != "ended":
                            h = int(diff // 3600)
                            m = int((diff % 3600) // 60)
                            time_str = f"{h}h {m}m" if h > 0 else f"{m}m"
                            msg += f" in {time_str}"
                            
                        notif = {
                            "contestId": cid,
                            "contestName": meta["name"],
                            "type": notif_type,
                            "message": msg,
                            "createdAt": now_utc().isoformat()
                        }
                        r.lpush("notifications:admin", json.dumps(notif))
                        r.ltrim("notifications:admin", 0, 9)
                        broadcast_event({"type": "notification", "data": notif})
    except redis.RedisError:
        pass

async def notification_worker():
    while True:
        try:
            check_and_generate_notifications()
        except Exception:
            pass
        await asyncio.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _event_loop, _set_score_script, _ingest_event_script
    _event_loop = asyncio.get_running_loop()
    try:
        _set_score_script = r.register_script(LUA_SET_SCORE)
        _ingest_event_script = r.register_script(LUA_INGEST_EVENT)
    except redis.RedisError:
        pass
    seed_if_empty()
    worker = asyncio.create_task(notification_worker())
    yield
    worker.cancel()

app = FastAPI(title="LeaderStack API v3", lifespan=lifespan)
app.add_exception_handler(redis.RedisError, redis_exception_handler)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

class ScoreRequest(BaseModel):
    participantId: str
    score: int
    @field_validator("participantId")
    @classmethod
    def validate_participant_id(cls, v: str) -> str:
        v = v.strip().lower()
        if not v or len(v) > 64 or not re.match(r"^[A-Za-z0-9_-]+$", v):
            raise ValueError("Invalid participantId")
        return v
    @field_validator("score")
    @classmethod
    def validate_score(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Score must be a positive integer")
        return v

class ScoreEventRequest(BaseModel):
    eventId: str
    participantId: str
    source: str
    scoreMode: str
    scoreValue: int
    timestamp: str
    metadata: dict | None = None

    @field_validator("participantId", "eventId")
    @classmethod
    def validate_ids(cls, v: str) -> str:
        v = v.strip().lower()
        if not v or len(v) > 64 or not re.match(r"^[A-Za-z0-9_-]+$", v):
            raise ValueError("Invalid ID format")
        return v

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        valid = {"admin", "judge", "system", "sensor", "organizer"}
        if v not in valid:
            raise ValueError(f"Source must be one of {valid}")
        return v

    @field_validator("scoreMode")
    @classmethod
    def validate_score_mode(cls, v: str) -> str:
        if v not in {"absolute", "delta"}:
            raise ValueError("scoreMode must be 'absolute' or 'delta'")
        return v

class ContestCreateRequest(BaseModel):
    contestId: str
    name: str
    expectedUsers: int
    maxScore: int
    startTime: str
    endTime: str
    description: str

    @field_validator("contestId")
    @classmethod
    def validate_id(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) > 64 or not re.match(r"^[A-Za-z0-9_-]+$", v):
            raise ValueError("Invalid contestId format")
        return v
    
    @field_validator("expectedUsers")
    @classmethod
    def validate_users(cls, v: int) -> int:
        if v < 1 or v > 1000:
            raise ValueError("Expected users must be between 1 and 1000")
        return v
    
    @field_validator("maxScore")
    @classmethod
    def validate_max_score(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Max score must be greater than 0")
        return v
        
    @model_validator(mode='after')
    def check_dates(self):
        try:
            start = datetime.fromisoformat(self.startTime.replace('Z', '+00:00'))
            end = datetime.fromisoformat(self.endTime.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            
            # Allow a 5-minute buffer for form submission time
            if start < now - timedelta(minutes=5):
                raise ValueError("Start time must be in the present or future, not the past")
            if start >= end:
                raise ValueError("Start time must be strictly before end time")
        except ValueError as e:
            # Re-raise string so it shows up in pydantic error msg cleanly
            raise ValueError(str(e))
        return self

@app.get("/")
def home():
    return {"message": "LeaderStack API v3"}

@app.get("/notifications", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_notifications():
    try:
        raw = r.lrange("notifications:admin", 0, 9)
        return [json.loads(x) for x in raw]
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/health", dependencies=[Depends(RateLimit(60, 60, "read"))])
def health():
    timestamp = now_utc().isoformat()
    try:
        r.ping()
        return JSONResponse(content={"status": "ok", "redis": "healthy", "sseClients": len(_sse_queues), "timestamp": timestamp})
    except redis.RedisError as exc:
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": f"unhealthy: {exc}", "timestamp": timestamp})

@app.get("/contests", dependencies=[Depends(RateLimit(60, 60, "read"))])
def list_contests():
    try:
        cids = r.smembers("contests:list")
        res = []
        for cid in cids:
            c = internal_get_contest_meta(cid)
            if c:
                c["status"] = get_contest_status(cid)
                res.append(c)
        return sorted(res, key=lambda x: x.get("startTime", ""))
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/contests/{contest_id}", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_contest(contest_id: str):
    c = internal_get_contest_meta(contest_id)
    if not c:
        raise HTTPException(status_code=404, detail="Contest not found")
    c["status"] = get_contest_status(contest_id)
    return c

@app.post("/contests", dependencies=[Depends(RateLimit(3, 60, "admin"))])
def create_contest(payload: ContestCreateRequest):
    try:
        if r.sismember("contests:list", payload.contestId):
            raise HTTPException(status_code=400, detail="Contest ID already exists")
        
        meta = {
            "contestId": payload.contestId,
            "name": payload.name,
            "expectedUsers": payload.expectedUsers,
            "maxScore": payload.maxScore,
            "startTime": payload.startTime,
            "endTime": payload.endTime,
            "description": payload.description
        }
        r.sadd("contests:list", payload.contestId)
        r.hset(f"contest:{payload.contestId}:meta", mapping=meta)
        
        meta["status"] = get_contest_status(payload.contestId)
        return meta
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

def internal_get_metrics(contest_id: str):
    try:
        m = r.hgetall(get_metrics_key(contest_id))
        size = r.zcard(board_key(contest_id, "all"))
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis error: {exc}")
    return {
        "contestId": contest_id,
        "totalSubmissions": int(m.get("total_submissions", 0)),
        "acceptedSubmissions": int(m.get("accepted_submissions", 0)),
        "rejectedSubmissions": int(m.get("rejected_submissions", 0)),
        "total_ingested_events": int(m.get("total_ingested_events", 0)),
        "accepted_ingested_events": int(m.get("accepted_ingested_events", 0)),
        "duplicate_ingested_events": int(m.get("duplicate_ingested_events", 0)),
        "rejected_ingested_events": int(m.get("rejected_ingested_events", 0)),
        "leaderboardSize": size,
        "sseClients": len(_sse_queues),
        "timestamp": now_utc().isoformat(),
    }

@app.get("/health/metrics", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_metrics_global():
    return internal_get_metrics("global")

@app.get("/contests/{contest_id}/metrics", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_metrics_contest(contest_id: str):
    if not r.sismember("contests:list", contest_id):
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_get_metrics(contest_id)

def internal_get_leaderboard(contest_id: str, board: str, limit: int):
    key = board_key(contest_id, board)
    meta_key = f"participant_meta:{contest_id}"
    try:
        data = r.zrevrange(key, 0, limit - 1, withscores=True)
        if not data:
            return []
        
        # Fetch metadata for these participants using pipeline
        pipe = r.pipeline()
        for participant_id, _ in data:
            pipe.hget(meta_key, participant_id)
        meta_results = pipe.execute()
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
        
    result = []
    for i, ((u, s), meta_raw) in enumerate(zip(data, meta_results), start=1):
        meta = json.loads(meta_raw) if meta_raw else {}
        result.append({
            "rank": i,
            "participantId": u,
            "score": decode_score(s),
            "timestamp": meta.get("timestamp"),
            "rankMovement": meta.get("rankMovement")
        })
    return result

@app.get("/leaderboard", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_leaderboard_global(board: str = Query("all", pattern="^(all|daily|weekly)$"), limit: int = Query(10, ge=1, le=100)):
    return internal_get_leaderboard("global", board, limit)

@app.get("/contests/{contest_id}/leaderboard", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_leaderboard_contest(contest_id: str, board: str = Query("all", pattern="^(all|daily|weekly)$"), limit: int = Query(10, ge=1, le=100)):
    if not r.sismember("contests:list", contest_id):
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_get_leaderboard(contest_id, board, limit)

def internal_get_rank(contest_id: str, participant_id: str, board: str):
    key = board_key(contest_id, board)
    try:
        stored = r.zscore(key, participant_id)
        rank = r.zrevrank(key, participant_id)
        size = r.zcard(key)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if stored is None or rank is None:
        raise HTTPException(status_code=404, detail="Not found")
    percentile = round(((rank + 1) / size) * 100, 1) if size > 0 else 0.0
    return {"participantId": participant_id, "score": decode_score(stored), "rank": rank + 1, "percentile": percentile}

@app.get("/rank/{participant_id}", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_rank_global(participant_id: str, board: str = Query("all", pattern="^(all|daily|weekly)$")):
    return internal_get_rank("global", participant_id, board)

@app.get("/contests/{contest_id}/rank/{participant_id}", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_rank_contest(contest_id: str, participant_id: str, board: str = Query("all", pattern="^(all|daily|weekly)$")):
    if not r.sismember("contests:list", contest_id):
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_get_rank(contest_id, participant_id, board)

def internal_get_neighbors(contest_id: str, participant_id: str, window: int, board: str):
    key = board_key(contest_id, board)
    try:
        rank_0 = r.zrevrank(key, participant_id)
        stored = r.zscore(key, participant_id)
        size = r.zcard(key)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if rank_0 is None or stored is None:
        raise HTTPException(status_code=404, detail="Not found")
    start = max(0, rank_0 - window)
    end = rank_0 + window
    try:
        raw = r.zrevrange(key, start, end, withscores=True)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    neighbors = [
        {"rank": start + idx + 1, "participantId": uid, "score": decode_score(s),
         "relation": "self" if uid == participant_id else "above" if (start + idx) < rank_0 else "below"}
        for idx, (uid, s) in enumerate(raw)
    ]
    percentile = round(((rank_0 + 1) / size) * 100, 1) if size > 0 else 0.0
    return {"participantId": participant_id, "rank": rank_0 + 1, "score": decode_score(stored), "percentile": percentile, "board": board, "neighbors": neighbors}

@app.get("/participants/{participant_id}/neighbors", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_neighbors_global(participant_id: str, window: int = Query(3, ge=1, le=10), board: str = Query("all", pattern="^(all|daily|weekly)$")):
    return internal_get_neighbors("global", participant_id, window, board)

@app.get("/contests/{contest_id}/participants/{participant_id}/neighbors", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_neighbors_contest(contest_id: str, participant_id: str, window: int = Query(3, ge=1, le=10), board: str = Query("all", pattern="^(all|daily|weekly)$")):
    if not r.sismember("contests:list", contest_id):
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_get_neighbors(contest_id, participant_id, window, board)

def internal_set_score(contest_id: str, payload: ScoreRequest, max_score: int):
    status = get_contest_status(contest_id)
    if status != "live":
        incr_metric(contest_id, "total_submissions")
        incr_metric(contest_id, "rejected_submissions")
        raise HTTPException(status_code=400, detail=f"Contest is {status}. Submissions are not accepted.")
    
    if not check_rate_limit(f"rate:score_part:{contest_id}:{payload.participantId}", 5, 10):
        incr_metric(contest_id, "total_submissions")
        incr_metric(contest_id, "rejected_submissions")
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")
        
    composite = encode_score(payload.score)
    timestamp = now_utc().isoformat()
    
    try:
        res_json = _set_score_script(
            keys=[
                board_key(contest_id, "all"),
                board_key(contest_id, "daily"),
                board_key(contest_id, "weekly"),
                get_metrics_key(contest_id),
                get_activity_key(contest_id),
                f"participant_meta:{contest_id}"
            ],
            args=[
                contest_id,
                payload.participantId,
                composite,
                payload.score,
                max_score,
                ACTIVITY_MAX,
                DAILY_TTL,
                WEEKLY_TTL,
                timestamp
            ]
        )
        res = json.loads(res_json)
    except redis.exceptions.NoScriptError:
        # Script was flushed from Redis, reload it
        res_json = _set_score_script(
            keys=[
                board_key(contest_id, "all"),
                board_key(contest_id, "daily"),
                board_key(contest_id, "weekly"),
                get_metrics_key(contest_id),
                get_activity_key(contest_id),
                f"participant_meta:{contest_id}"
            ],
            args=[
                contest_id,
                payload.participantId,
                composite,
                payload.score,
                max_score,
                ACTIVITY_MAX,
                DAILY_TTL,
                WEEKLY_TTL,
                timestamp
            ],
            client=r
        )
        res = json.loads(res_json)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
        
    if "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
        
    activity = res["activity"]
    broadcast_event({
        "type": "score_update",
        "contestId": contest_id,
        "participantId": payload.participantId,
        "score": payload.score,
        "previousScore": activity.get("previousScore"),
        "previousRank": activity.get("previousRank"),
        "newRank": activity.get("newRank"),
        "rankMovement": activity.get("rankMovement"),
        "rank": res["rank"],
        "board": "all",
        "timestamp": activity["timestamp"],
    })

    return {"participantId": payload.participantId, "score": payload.score, "rank": res["rank"]}

@app.post("/score", dependencies=[Depends(RateLimit(5, 10, "score"))])
def set_score_global(payload: ScoreRequest):
    return internal_set_score("global", payload, max_score=1000000)

@app.post("/contests/{contest_id}/score", dependencies=[Depends(RateLimit(5, 10, "score"))])
def set_score_contest(contest_id: str, payload: ScoreRequest):
    meta = internal_get_contest_meta(contest_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_set_score(contest_id, payload, max_score=meta.get("maxScore", 1000))

def process_score_event(contest_id: str, event: ScoreEventRequest, max_score: int):
    # Optional Future Architecture:
    # Instead of executing synchronously via Lua here, this could XADD to a Redis Stream:
    # r.xadd("stream:score_events", {"payload": event.json()})
    # And a separate worker would XREADGROUP and apply the logic below.
    
    timestamp_ms = int(time.time() * 1000)
    
    try:
        res_json = _ingest_event_script(
            keys=[
                board_key(contest_id, "all"),
                board_key(contest_id, "daily"),
                board_key(contest_id, "weekly"),
                get_metrics_key(contest_id),
                get_activity_key(contest_id),
                f"event:dedupe:{event.eventId}",
                f"participant_meta:{contest_id}"
            ],
            args=[
                contest_id,
                event.participantId,
                event.scoreValue,
                event.scoreMode,
                max_score,
                ACTIVITY_MAX,
                DAILY_TTL,
                WEEKLY_TTL,
                event.timestamp,
                event.eventId,
                event.source,
                86400, # 24h dedupe ttl
                timestamp_ms
            ]
        )
        res = json.loads(res_json)
    except redis.exceptions.NoScriptError:
        res_json = _ingest_event_script(
            keys=[
                board_key(contest_id, "all"),
                board_key(contest_id, "daily"),
                board_key(contest_id, "weekly"),
                get_metrics_key(contest_id),
                get_activity_key(contest_id),
                f"event:dedupe:{event.eventId}",
                f"participant_meta:{contest_id}"
            ],
            args=[
                contest_id,
                event.participantId,
                event.scoreValue,
                event.scoreMode,
                max_score,
                ACTIVITY_MAX,
                DAILY_TTL,
                WEEKLY_TTL,
                event.timestamp,
                event.eventId,
                event.source,
                86400,
                timestamp_ms
            ],
            client=r
        )
        res = json.loads(res_json)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
        
    if res["status"] != "accepted":
        # duplicate or rejected
        return {
            "status": res["status"],
            "eventId": event.eventId,
            "contestId": contest_id,
            "applied": False,
            "reason": res.get("reason")
        }
        
    activity = res["activity"]
    broadcast_event({
        "type": "score_update",
        "contestId": contest_id,
        "participantId": event.participantId,
        "score": activity["newScore"],
        "previousScore": activity.get("previousScore"),
        "previousRank": activity.get("previousRank"),
        "newRank": activity.get("newRank"),
        "rankMovement": activity.get("rankMovement"),
        "rank": res["rank"],
        "board": "all",
        "timestamp": activity["timestamp"],
        "source": activity["source"]
    })

    return {
        "status": "accepted",
        "eventId": event.eventId,
        "contestId": contest_id,
        "participantId": event.participantId,
        "source": event.source,
        "applied": True
    }

@app.post("/contests/{contest_id}/ingest-score-event", dependencies=[Depends(RateLimit(60, 60, "ingest"))])
def ingest_score_event(contest_id: str, event: ScoreEventRequest):
    meta = internal_get_contest_meta(contest_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Contest not found")
        
    status = get_contest_status(contest_id)
    if status != "live":
        raise HTTPException(status_code=400, detail=f"Contest is {status}. Ingestion not accepted.")
        
    return process_score_event(contest_id, event, max_score=meta.get("maxScore", 1000))

def internal_get_activity(contest_id: str, limit: int):
    try:
        raw = r.lrange(get_activity_key(contest_id), 0, limit - 1)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return [json.loads(entry) for entry in raw]

@app.get("/activity", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_activity_global(limit: int = Query(10, ge=1, le=100)):
    return internal_get_activity("global", limit)

@app.get("/contests/{contest_id}/activity", dependencies=[Depends(RateLimit(60, 60, "read"))])
def get_activity_contest(contest_id: str, limit: int = Query(10, ge=1, le=100)):
    if not r.sismember("contests:list", contest_id):
        raise HTTPException(status_code=404, detail="Contest not found")
    return internal_get_activity(contest_id, limit)

@app.get("/events", dependencies=[Depends(RateLimit(10, 60, "sse"))])
async def sse_endpoint_global(request: Request):
    return await sse_stream(request)

@app.get("/contests/{contest_id}/events", dependencies=[Depends(RateLimit(10, 60, "sse"))])
async def sse_endpoint_contest(request: Request, contest_id: str):
    return await sse_stream(request)

async def sse_stream(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    _sse_queues.append(q)
    async def generator():
        try:
            yield f"data: {json.dumps({'type': 'connected', 'timestamp': now_utc().isoformat()})}\n\n"
            while True:
                if await request.is_disconnected(): break
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=20.0)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': now_utc().isoformat()})}\n\n"
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)
    return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})