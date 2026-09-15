from dotenv import load_dotenv
from pathlib import Path
import os
from dataclasses import dataclass

@dataclass
class Setting:
    ROOT = Path(__file__).resolve().parents[1]

    load_dotenv(dotenv_path=ROOT/".env")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
    NASA_FIRMS_MAP_KEY = os.getenv("NASA_FIRMS_MAP_KEY")
    OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

setting = Setting()

if __name__=="__main__":
    print(setting.ROOT)
    print(setting.KAFKA_BOOTSTRAP_SERVERS)
    print(setting.NASA_FIRMS_MAP_KEY)
    print(setting.OPENAQ_API_KEY)