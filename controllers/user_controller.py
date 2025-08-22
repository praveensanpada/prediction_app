# === controllers/user_controller.py ===
import logging
from typing import Any, Dict

from qdrant_client.http.models import Filter as QFilter, FieldCondition, MatchValue
from libraries.qdrant_searcher import (
    QdrantMultiCollectionSearcher,
    QdrantForbiddenError,
)
from config.settings import QDRANT_URL, QDRANT_API_KEY, EMBEDDING_MODEL

logger = logging.getLogger(__name__)

def handle_user_question(match_id: str, question: str = None) -> Any:
    try:
        # Validate inputs
        if match_id is None:
            return {"status": "match_id is required!", "match_id": match_id}
        if not question or str(question).strip() == "":
            return {"status": "Question is required!", "question": question}

        # Build strict payload filter: payload.match_id == <match_id>
        match_id_filter = QFilter(
            must=[
                FieldCondition(
                    key="match_id",
                    match=MatchValue(value=str(match_id))  # ensure string match if stored as string
                )
            ]
        )

        # Init searcher (ensure your URL is HTTPS and API key is correct)
        searcher = QdrantMultiCollectionSearcher(
            collections=["match_details", "match_stats"],  # make sure these collections exist
            embedder_model=EMBEDDING_MODEL,
            qdrant_url=QDRANT_URL,
            qdrant_api_key=QDRANT_API_KEY,
            run_self_test=True,
        )

        # Apply the same filter to both collections
        filters: Dict[str, QFilter] = {
            "match_details": match_id_filter,
            "match_stats": match_id_filter,
        }

        results = searcher.search_question(question, top_k=5, filters=filters)
        logger.info(f"Search executed successfully for match_id={match_id}")

        return {
            "match_id": str(match_id),
            "results": results,
        }

    except QdrantForbiddenError as e:
        logger.error(f"Qdrant forbidden: {e}")
        return {
            "status": "forbidden",
            "message": "Qdrant rejected the request (403). Check API key, HTTPS URL, and permissions/IP allowlist.",
            "hints": {
                "QDRANT_URL": "Use the cluster API endpoint with https://",
                "QDRANT_API_KEY": "Verify it is correct and has read/search permissions",
                "Collections": "Ensure 'match_details' and 'match_stats' exist",
                "Network": "If allowlist is enabled, add this server's public IP",
            },
        }

    except Exception as e:
        logger.error(f"Error in handle_user_question: {e}")
        return []
