import os
import json
import requests

from dotenv import load_dotenv

load_dotenv()

URL = "https://api.openaq.org/v3/parameters/2/latest"

headers = {
    "X-API-Key": os.getenv("OPENAQ_API_KEY")
}

params = {
    "limit": 10
}

response = requests.get(
    URL,
    headers=headers,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

print(json.dumps(
    data,
    indent=4,
    ensure_ascii=False
))