# === routes/cron_routes.py ===
from fastapi import APIRouter, Body
from controllers.cron_controller import get_upcoming_matches_list, get_upcoming_matches_cron, get_upcoming_matches_embeding

router = APIRouter()

@router.get("/get_upcoming_matches_list")
def get_upcoming_matches_list_get():
    return get_upcoming_matches_list()

@router.get("/get_upcoming_matches_cron")
def get_upcoming_matches_cron_get():
    return get_upcoming_matches_cron()

@router.get("/get_upcoming_matches_embeding")
def get_upcoming_matches_embeding_get():
    return get_upcoming_matches_embeding()