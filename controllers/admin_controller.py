# === controllers/admin_controller.py ===
from config.settings import SOURCE_URL, SOURCE_URL_1, SOURCE_URL_2, SOURCE_URL_3, SOURCE_URL_4
from utils.logger import get_logger
import requests
from datetime import datetime
from models.admin_model import AdminModel

admin_model = AdminModel()

logger = get_logger(__name__)

def add_update_match_description(description_type: str = None, description_data: dict = {}):
    try:
        if not description_type or str(description_type).strip() == "":
            return {"status": "Description type is required!", "description_type": description_type}
        
        master_description_detail = admin_model.upsert_match_description_by_type(description_type, description_data)
        logger.info(f'Master description data updated for description_type = {description_type}.')

        return master_description_detail
    except Exception as e:
        logger.error(f"Error in master_trigger: {e}")
        return []