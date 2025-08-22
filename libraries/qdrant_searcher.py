from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter as QFilter
from qdrant_client.http.exceptions import UnexpectedResponse
from langchain_huggingface import HuggingFaceEmbeddings  # non-deprecated

logger = logging.getLogger(__name__)


class QdrantForbiddenError(Exception):
    """Raised when Qdrant returns 403 Forbidden (bad URL/key/permissions/IP)."""
    pass


class QdrantMultiCollectionSearcher:
    def __init__(
        self,
        collections: List[str],
        embedder_model: str,
        qdrant_url: str,
        qdrant_api_key: Optional[str] = None,
        timeout: Optional[float] = 15.0,
        run_self_test: bool = True,
    ):
        """
        :param qdrant_url: MUST be your cluster API endpoint (use https://).
        :param qdrant_api_key: Required for Qdrant Cloud/private deployments.
        """
        self.collections = collections
        self.qdrant = QdrantClient(url=qdrant_url, api_key=qdrant_api_key, timeout=timeout)
        self.embedder = HuggingFaceEmbeddings(model_name=embedder_model)

        if run_self_test:
            self._self_test()

    def _self_test(self) -> None:
        """Validate connectivity/permissions at startup so we fail fast & clear."""
        try:
            resp = self.qdrant.get_collections()
            names = [c.name for c in resp.collections]
            logger.info(f"Qdrant reachable. Collections: {names}")
        except UnexpectedResponse as e:
            # Qdrant returns UnexpectedResponse with details
            if getattr(e, "status_code", None) == 403 or "403" in str(e):
                msg = "Qdrant auth/permissions failed (403 Forbidden). Check HTTPS URL, API key, and IP allowlist."
                logger.error(msg)
                raise QdrantForbiddenError(msg) from e
            logger.error(f"Qdrant self-test failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Qdrant self-test error: {e}")
            raise

    def _embed_query(self, query: str) -> List[float]:
        return self.embedder.embed_query(query)

    def _search_collection(
        self,
        collection_name: str,
        vector: List[float],
        top_k: int = 5,
        qfilter: Optional[QFilter] = None,
    ) -> List[Dict[str, Any]]:
        """
        Version-agnostic call:
        - Tries newer `filter=` param.
        - Falls back to older `query_filter=` param.
        Raises QdrantForbiddenError on 403 so the caller can return a clear message.
        """
        try:
            # Newer client signature
            results = self.qdrant.search(
                collection_name=collection_name,
                query_vector=vector,
                limit=top_k,
                filter=qfilter,            # newer param name
                with_payload=True,
                with_vectors=False,
            )
        except TypeError as te:
            # Older client fallback
            if "unexpected keyword argument 'filter'" in str(te):
                try:
                    results = self.qdrant.search(
                        collection_name=collection_name,
                        query_vector=vector,
                        limit=top_k,
                        query_filter=qfilter,  # older param name
                        with_payload=True,
                        with_vectors=False,
                    )
                except UnexpectedResponse as e2:
                    if getattr(e2, "status_code", None) == 403 or "403" in str(e2):
                        logger.warning(
                            f"⚠️ Forbidden for collection '{collection_name}' (fallback). Raw: {getattr(e2, 'content', b'')}"
                        )
                        raise QdrantForbiddenError("Forbidden (403) while searching Qdrant.") from e2
                    logger.warning(f"⚠️ Search failed for '{collection_name}' (fallback): {e2}")
                    return []
                except Exception as e2:
                    logger.warning(f"⚠️ Search failed for '{collection_name}' (fallback): {e2}")
                    return []
            else:
                logger.warning(f"⚠️ Search failed for '{collection_name}': {te}")
                return []
        except UnexpectedResponse as e:
            if getattr(e, "status_code", None) == 403 or "403" in str(e):
                logger.warning(
                    f"⚠️ Forbidden for collection '{collection_name}'. Raw: {getattr(e, 'content', b'')}"
                )
                raise QdrantForbiddenError("Forbidden (403) while searching Qdrant.") from e
            logger.warning(f"⚠️ Search failed for '{collection_name}': {e}")
            return []
        except Exception as e:
            logger.warning(f"⚠️ Search failed for '{collection_name}': {e}")
            return []

        return [{"id": r.id, "payload": r.payload, "score": r.score} for r in results]

    def search_question(
        self,
        question: str,
        top_k: int = 5,
        filters: Optional[Dict[str, QFilter]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        vector = self._embed_query(question)
        out: Dict[str, List[Dict[str, Any]]] = {}

        for collection in self.collections:
            qf = filters.get(collection) if filters else None
            out[collection] = self._search_collection(
                collection_name=collection,
                vector=vector,
                top_k=top_k,
                qfilter=qf,
            )
        return out
