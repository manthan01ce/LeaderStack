import os
import time
from datetime import datetime, timezone
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DAILY_TTL = 48 * 3600
WEEKLY_TTL = 10 * 86400

def now_utc():
    return datetime.now(timezone.utc)

def encode_score(int_score: int) -> float:
    return float(int_score) + (1.0 / int(time.time() * 1000))

def board_key(contest_id, board):
    now = now_utc()
    if board == "daily":
        return f"leaderboard:{contest_id}:daily:{now.strftime('%Y-%m-%d')}"
    if board == "weekly":
        y, w, _ = now.isocalendar()
        return f"leaderboard:{contest_id}:weekly:{y}-W{w:02d}"
    return f"leaderboard:{contest_id}:all"

DEMO_PARTICIPANTS = [
    ("alice", 300), ("bob", 275), ("charlie", 250),
    ("diana", 225), ("eve", 200), ("frank", 190),
    ("grace", 175), ("henry", 160), ("ivy", 145), ("jack", 130)
]

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.ping()
    except redis.RedisError as e:
        print(f"[ERROR] Redis unreachable: {e}")
        return

    contests = ["global", "contest-live", "contest-ended"]
    
    # Flush existing leaderboard keys to ensure clean seed
    for c in contests:
        keys = r.keys(f"leaderboard:{c}:*")
        if keys:
            r.delete(*keys)
        # Clear activity and metrics
        r.delete(f"activity:{c}:log", f"metrics:{c}")
        
    print(f"[INFO] Cleared existing entries.")
    print(f"[INFO] Seeding {len(DEMO_PARTICIPANTS)} demo participants for {len(contests)} contests...\n")

    for c in contests:
        print(f"  [{c}]")
        for pid, score in DEMO_PARTICIPANTS:
            # Vary scores a bit for different contests so they look distinct
            adj_score = score if c == "global" else (score + 50 if c == "contest-live" else score - 20)
            composite = encode_score(adj_score)
            r.zadd(board_key(c, "all"), {pid: composite})
            r.zadd(board_key(c, "daily"), {pid: composite})
            r.zadd(board_key(c, "weekly"), {pid: composite})
            time.sleep(0.002)
        r.expire(board_key(c, "daily"), DAILY_TTL)
        r.expire(board_key(c, "weekly"), WEEKLY_TTL)

    print(f"\n[OK] Participants seeded for all contests.")

if __name__ == "__main__":
    main()
