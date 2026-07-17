import requests
from datetime import datetime, timedelta, timezone

API_URL = "http://localhost:8000"

def create_contest(cid, name, start, end):
    payload = {
        "contestId": cid,
        "name": name,
        "expectedUsers": 500,
        "maxScore": 1000,
        "startTime": start.isoformat(),
        "endTime": end.isoformat(),
        "description": "Demo contest"
    }
    r = requests.post(f"{API_URL}/contests", json=payload)
    print(f"Create {cid}: {r.status_code}")

now = datetime.now(timezone.utc)

# 1. Past contest
create_contest("past_demo", "Winter Algorithms 2025", now - timedelta(days=30), now - timedelta(days=28))

# 2. Ongoing contest
create_contest("ongoing_demo", "Global Code Sprint", now - timedelta(hours=2), now + timedelta(days=2))

# 3. Upcoming contest
create_contest("future_demo", "Summer Hacks", now + timedelta(days=10), now + timedelta(days=12))

print("Contests created.")
