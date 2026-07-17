import requests
import time
import random

API_URL = "http://localhost:8000/score"

participants = [
    {"participantId": "alice_demo", "score": 50},
    {"participantId": "bob_demo", "score": 45},
    {"participantId": "charlie_demo", "score": 60},
    {"participantId": "dave_demo", "score": 75},
    {"participantId": "eve_demo", "score": 90}
]

print("Injecting demo data with time lapses...")

for p in participants:
    try:
        resp = requests.post(API_URL, json=p)
        print(f"Submitted {p['participantId']}: {resp.status_code}")
    except Exception as e:
        print(f"Failed {p['participantId']}: {e}")
    
    # Wait between 1.5 and 3 seconds so each has a visibly different timestamp (including seconds)
    time.sleep(random.uniform(1.5, 3.0))

print("Done!")
