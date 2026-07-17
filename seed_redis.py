import redis
from datetime import datetime, timedelta, timezone

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def create_contest(cid, name, start, end):
    meta = {
        "contestId": cid,
        "name": name,
        "expectedUsers": 500,
        "maxScore": 1000,
        "startTime": start.isoformat(),
        "endTime": end.isoformat(),
        "description": "Demo contest"
    }
    r.sadd("contests:list", cid)
    r.hset(f"contest:{cid}:meta", mapping=meta)
    print(f"Created {cid} in Redis")

now = datetime.now(timezone.utc)
create_contest("past_demo_2", "Winter Algorithms 2025", now - timedelta(days=30), now - timedelta(days=28))
create_contest("ongoing_demo_2", "Global Code Sprint", now - timedelta(hours=2), now + timedelta(days=2))
create_contest("future_demo_2", "Summer Hacks", now + timedelta(days=10), now + timedelta(days=12))

# Let's also add a winner to the past_demo_2 so the UI fetches a winner correctly
# Rank key: leaderboard:{contest_id}:all
r.zadd("leaderboard:past_demo_2:all", {"winner_alice": 950})
r.zadd("leaderboard:past_demo_2:all", {"runner_up_bob": 900})
