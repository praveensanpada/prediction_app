# === routes/user_routes.py ===
from fastapi import APIRouter, Body
from controllers.user_controller import handle_user_question

router = APIRouter()

@router.post("/handle_user_question")
def handle_user_question_post(match_id: int, question: str = Body(..., embed=True)):
    return handle_user_question(match_id, question)