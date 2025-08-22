# === libraries/api_client.py ===
import requests
import logging

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self):
        self.headers = {
            "Content-Type": "application/json"
        }

    def post(self, url: str, payload: dict):
        """Send a POST request with full URL"""
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"✅ POST {url} successful")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ POST {url} failed: {e}")
            return {"error": str(e)}
