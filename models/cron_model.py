# === models/cron_model.py ===
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
    
class CronModel:
    def __init__(self):
        try:
            self.mongo_client = MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client[MONGO_DB]
            logger.info("Connected to MongoDB database (mongo_db).")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def upsert_match_detail_by_id(self, match_id: str, match_data: dict, collection: str = "match_details"):
        try:
            sanitized_data = convert_decimals(match_data)
            result = self.mongo_db[collection].update_one(
                {"match_id": match_id},
                {"$set": sanitized_data},
                upsert=True
            )
            if result.matched_count:
                logger.info(f"Match updated for match_id: {match_id}")
                return {"status": "updated", "match_id": match_id}
            else:
                logger.info(f"Match inserted for match_id: {match_id}")
                return {"status": "inserted", "match_id": match_id}
        except Exception as e:
            logger.error(f"Error in upsert_match_by_id: {e}")
            return {"status": "error", "message": str(e)}
        
    def upsert_match_stats_by_id(self, match_id: str, match_data: dict, collection: str = "match_stats"):
        try:
            sanitized_data = convert_decimals(match_data)
            result = self.mongo_db[collection].update_one(
                {"match_id": match_id},
                {"$set": sanitized_data},
                upsert=True
            )
            if result.matched_count:
                logger.info(f"Match updated for match_id: {match_id}")
                return {"status": "updated", "match_id": match_id}
            else:
                logger.info(f"Match inserted for match_id: {match_id}")
                return {"status": "inserted", "match_id": match_id}
        except Exception as e:
            logger.error(f"Error in upsert_match_by_id: {e}")
            return {"status": "error", "message": str(e)}

    def get_match_description(self, description_type: str):
        try:
            description_data = self.mongo_db.match_descriptions.find_one({"description_type": description_type},{"_id": 0})
            return json.loads(dumps(description_data)) if description_data else {}
        except Exception as e:
            logger.error(f"Error in get_match_description: {e}")
            return {}
        
    def get_match_details_by_id(self, match_id: str):
        try:
            match_data = self.mongo_db.match_details.find_one({"match_id": match_id},{"_id": 0})
            return json.loads(dumps(match_data)) if match_data else {}
        except Exception as e:
            logger.error(f"Error in get_match_details_by_id: {e}")
            return {}

    def get_match_stats_by_id(self, match_id: str):
        try:
            match_data = self.mongo_db.match_stats.find_one({"match_id": match_id},{"_id": 0})
            return json.loads(dumps(match_data)) if match_data else {}
        except Exception as e:
            logger.error(f"Error in get_match_stats_by_id: {e}")
            return {}