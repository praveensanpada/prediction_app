# === routes/admin_routes.py ===
from fastapi import APIRouter, Body
from controllers.admin_controller import add_update_match_description

router = APIRouter()

@router.post("/add_update_match_description")
def add_update_match_description_post(description_type: str = Body(..., embed=True), description_data: dict = Body(..., embed=True)):
    return add_update_match_description(description_type, description_data)