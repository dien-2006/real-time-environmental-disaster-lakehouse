import json
from datetime import datetime, timezone
from common.http_client import create_http_session
from config.settings import setting

class OpenAQClient:
    def __init__(self):

        self._url = "https://api.openaq.org/v3"
        TOPIC = "air_quality"
        self.session = create_http_session()
        self.openap_api_key = setting.OPENAQ_API_KEY
        
        if not self.openap_api_key:
            raise ValueError(
                "OPENAQ_API_KEY is missing"
            )
        
    def fetch_openaq(self):

        headers = {
            "X-API-Key": self.openap_api_key
        }

        response = self.session.get(
            f"{self._url}/locations",
            headers=headers,
            params={
                "iso": "VN",
                "limit": 100,
            },
            timeout=30,
        )

        response.raise_for_status()

        data= response.json()  
        for item in data["results"]:
            print(
                item.get("id"),
                item.get("name"),
                item.get("locality"),
                item.get("timezone"),
                item.get("coordinates")
            ) 
        


if __name__ == "__main__":
    client = OpenAQClient()
    print(client.fetch_openaq())