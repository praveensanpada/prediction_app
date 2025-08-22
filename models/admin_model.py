# === models/admin_model.py ===
from pymongo import MongoClient
from config.settings import MONGO_URL, MONGO_DB
from utils.logger import get_logger
from decimal import Decimal

from bson.json_util import dumps
import json

logger = get_logger(__name__)

def convert_decimals(obj):
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj
    
class AdminModel:
    def __init__(self):
        try:
            self.mongo_client = MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client[MONGO_DB]
            logger.info("Connected to MongoDB database (mongo_db).")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def upsert_match_description_by_type(self, description_type: str, description_data: dict, collection: str = "match_descriptions"):
        try:
            sanitized_data = convert_decimals(description_data)
            result = self.mongo_db[collection].update_one(
                {"description_type": description_type},
                {"$set": sanitized_data},
                upsert=True
            )
            if result.matched_count:
                logger.info(f"Match description updated for description_type: {description_type}")
                return {"status": "updated", "description_type": description_type}
            else:
                logger.info(f"Match description inserted for description_type: {description_type}")
                return {"status": "inserted", "description_type": description_type}
        except Exception as e:
            logger.error(f"Error in upsert_match_description_by_type: {e}")
            return {"status": "error", "message": str(e)}