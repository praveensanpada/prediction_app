from typing import List, Optional, Dict, Any
from uuid import uuid5, NAMESPACE_DNS
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    PointStruct,
    VectorParams,
    Distance,
    PayloadSchemaType,
    Filter as QFilter,
    FieldCondition,
    MatchValue,
)
from qdrant_client.http.exceptions import UnexpectedResponse
# from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from config.settings import (
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    QDRANT_URL,
    QDRANT_API_KEY,
    COLLECTION_INDEX_FIELDS,
    ID_INDEX_TYPE,  # <-- add this in your config (see below)
)
from utils.logger import get_logger

# Fallback types for older qdrant-client versions
try:
    from qdrant_client.http.models import KeywordIndexParams, IntegerIndexParams, TextIndexParams
except Exception:
    KeywordIndexParams = IntegerIndexParams = TextIndexParams = None

logger = get_logger(__name__)


class QdrantMatchPusher:
    def __init__(
        self,
        collection_name: str,
        embedder: Optional[HuggingFaceEmbeddings] = None,
        qdrant: Optional[QdrantClient] = None,
    ):
        self.collection_name = collection_name
        self.embedder = embedder or self.get_embedder()
        self.qdrant = qdrant or self.get_qdrant_client()
        self.vector_dim = EMBEDDING_DIM  # default; will verify/create
        self._ensure_collection()

    @staticmethod
    def get_embedder(model_name: str = EMBEDDING_MODEL) -> HuggingFaceEmbeddings:
        """Initialize HuggingFace embeddings."""
        return HuggingFaceEmbeddings(model_name=model_name)

    @staticmethod
    def get_qdrant_client() -> QdrantClient:
        """Initialize Qdrant client."""
        return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

    @staticmethod
    def generate_unique_id_from_match_id(match_id: str) -> str:
        """Generate deterministic UUID based on match_id."""
        return str(uuid5(NAMESPACE_DNS, str(match_id)))

    def document_exists(self, vector_id: str) -> bool:
        """Check if a document/vector already exists in Qdrant."""
        try:
            result = self.qdrant.retrieve(collection_name=self.collection_name, ids=[vector_id])
            return bool(result)
        except UnexpectedResponse as e:
            if getattr(e, "status_code", None) == 404:
                return False
            logger.warning(f"⚠️ Existence check failed for ID {vector_id}: {e}")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Existence check failed for ID {vector_id}: {e}")
            return False

    # ---------- index helpers ----------
    @staticmethod
    def _infer_schema_for_field(field: str) -> str:
        """
        Decide which payload index schema to use.
        Returns one of: "KEYWORD", "TEXT", "INTEGER".
        Uses config.ID_INDEX_TYPE for IDs.
        """
        f = field.lower()

        # IDs & short categorical codes
        if f == "match_id" or f.endswith("_id") or f in {"venue_id", "player_id"}:
            return ID_INDEX_TYPE  # "INTEGER" or "KEYWORD" (see config)
        if f in {"home_team", "away_team", "match_format", "position"}:
            # short codes; keep as KEYWORD
            return "KEYWORD"

        # Human-readable names / free text
        if f.endswith("_name") or f in {"ground_name", "full_name", "league_name"}:
            return "TEXT"

        # Default safe choice
        return "TEXT"

    def _create_payload_index(self, field: str, schema: str):
        """
        Create payload index handling both modern (PayloadSchemaType) and older
        (*IndexParams) client signatures.
        schema ∈ {"KEYWORD","TEXT","INTEGER"}
        """
        try:
            if KeywordIndexParams and TextIndexParams:
                if schema == "KEYWORD":
                    idx = KeywordIndexParams()
                elif schema == "TEXT":
                    idx = TextIndexParams()
                elif schema == "INTEGER":
                    idx = IntegerIndexParams()
                else:
                    raise ValueError(f"Unknown schema '{schema}'")

                self.qdrant.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema=idx,
                )
            else:
                if schema == "KEYWORD":
                    enum_schema = PayloadSchemaType.KEYWORD
                elif schema == "TEXT":
                    enum_schema = PayloadSchemaType.TEXT
                elif schema == "INTEGER":
                    enum_schema = PayloadSchemaType.INTEGER
                else:
                    raise ValueError(f"Unknown schema '{schema}'")

                self.qdrant.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema=enum_schema,
                )
            logger.info(f"📚 Created payload index: {self.collection_name}.{field} ({schema})")
        except UnexpectedResponse as e:
            msg = str(e).lower()
            if "already exists" in msg:
                logger.info(f"✅ Index already exists: {self.collection_name}.{field}")
            else:
                logger.warning(f"⚠️ Skipped index creation for '{field}' → {e}")
        except Exception as e:
            logger.warning(f"⚠️ Skipped index creation for '{field}' → {e}")

    # ---------- collection + index bootstrap ----------
    def _ensure_collection(self):
        """Create collection if it doesn't exist and setup payload schema."""
        existing_fields = set()
        try:
            collection_info = self.qdrant.get_collection(self.collection_name)
            logger.info(f"Collection '{self.collection_name}' already exists.")
            # Try to detect vector dim if server exposes it
            try:
                vcfg = getattr(collection_info, "vectors_count", None)  # not always present
                # Leave self.vector_dim as configured if not available
            except Exception:
                pass
            try:
                if getattr(collection_info, "payload_schema", None):
                    existing_fields = set(collection_info.payload_schema.keys())
            except Exception:
                existing_fields = set()
        except UnexpectedResponse as e:
            if getattr(e, "status_code", None) == 404:
                # Create missing collection with configured vector dim
                self.qdrant.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
                )
                logger.info(f"🆕 Created Qdrant collection: {self.collection_name} (dim={EMBEDDING_DIM})")
            else:
                logger.warning(f"⚠️ Could not fetch existing collection info: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch existing collection info: {e}")

        # Build index set: configured fields + 'text'
        fields_cfg = COLLECTION_INDEX_FIELDS.get(self.collection_name, [])
        fields = set(fields_cfg) | {"text"}  # always index 'text'

        for field in fields:
            if field in existing_fields:
                continue
            schema = self._infer_schema_for_field(field)
            self._create_payload_index(field, schema)

    # ---------- fast search helpers ----------
    def _make_match_id_filter(self, match_id: Any) -> QFilter:
        """Build a filter for payload.match_id with correct type based on ID_INDEX_TYPE."""
        if ID_INDEX_TYPE == "INTEGER":
            mv = MatchValue(value=int(match_id))
        else:
            mv = MatchValue(value=str(match_id))
        return QFilter(must=[FieldCondition(key="match_id", match=mv)])

    def fetch_by_match_id(self, match_id: Any, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fast fetch by payload filter (no embeddings needed).
        Tries scroll() first (no vector required); if your server needs search(), uses a zero vector.
        """
        qfilter = self._make_match_id_filter(match_id)

        # 1) Try filter-only scroll (recommended for equality lookups)
        try:
            hits, _ = self.qdrant.scroll(
                collection_name=self.collection_name,
                scroll_filter=qfilter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return [{"id": h.id, "payload": h.payload} for h in hits]
        except Exception as e:
            logger.warning(f"Scroll failed, falling back to search: {e}")

        # 2) Fallback: search with a zero vector (same dim) + filter
        try:
            zero_vec = [0.0] * EMBEDDING_DIM
            results = self.qdrant.search(
                collection_name=self.collection_name,
                query_vector=zero_vec,
                limit=limit,
                filter=qfilter,          # newer clients
                with_payload=True,
                with_vectors=False,
            )
            return [{"id": r.id, "payload": r.payload, "score": r.score} for r in results]
        except TypeError as te:
            # Older clients: query_filter=
            if "unexpected keyword argument 'filter'" in str(te):
                results = self.qdrant.search(
                    collection_name=self.collection_name,
                    query_vector=zero_vec,
                    limit=limit,
                    query_filter=qfilter,  # older param name
                    with_payload=True,
                    with_vectors=False,
                )
                return [{"id": r.id, "payload": r.payload, "score": r.score} for r in results]
            raise
        except Exception as e:
            logger.error(f"❌ fetch_by_match_id failed: {e}")
            return []

    # ---------- upsert ----------
    def push_matches(self, match_docs: List[Document]):
        """Push each match document individually to Qdrant."""
        if not match_docs:
            logger.warning(f"No documents to embed for collection: {self.collection_name}")
            return

        for doc in match_docs:
            match_id = doc.metadata.get("match_id")
            if match_id is None:
                logger.warning(f"⚠️ Skipped document without match_id: {doc.page_content[:50]}")
                continue

            vector_id = self.generate_unique_id_from_match_id(match_id)

            if not self.document_exists(vector_id):
                logger.info(f"🆕 Adding new match → ID: {vector_id}")
            else:
                logger.info(f"♻️ Updating existing match → ID: {vector_id}")

            try:
                vector = self.embedder.embed_query(doc.page_content)
            except Exception as e:
                logger.error(f"❌ Embedding failed for match ID {match_id} → {e}")
                continue

            if not vector or not isinstance(vector, list):
                logger.warning(f"⚠️ Skipped invalid vector for match ID {match_id}")
                continue

            point = PointStruct(
                id=vector_id,
                vector=vector,
                payload={**doc.metadata, "text": doc.page_content},
            )

            try:
                self.qdrant.upsert(collection_name=self.collection_name, points=[point])
                logger.info(f"✅ Upserted match ID {match_id} to '{self.collection_name}'")
            except UnexpectedResponse as e:
                if getattr(e, "status_code", None) == 404:
                    logger.warning(
                        f"⚠️ Collection '{self.collection_name}' not found during upsert. Recreating collection."
                    )
                    self._ensure_collection()
                    self.qdrant.upsert(collection_name=self.collection_name, points=[point])
                    logger.info(f"✅ Upserted match ID {match_id} after recreating collection.")
                else:
                    logger.error(f"❌ Qdrant upsert failed for match ID {match_id}: {e}")
            except Exception as e:
                logger.error(f"❌ Qdrant upsert failed for match ID {match_id}: {e}")
