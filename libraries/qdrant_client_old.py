from typing import List, Optional
from uuid import uuid5, NAMESPACE_DNS
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance, PayloadSchemaType
from qdrant_client.http.exceptions import UnexpectedResponse
# from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from config.settings import EMBEDDING_MODEL, QDRANT_URL, QDRANT_API_KEY, COLLECTION_INDEX_FIELDS
from utils.logger import get_logger

logger = get_logger(__name__)

class QdrantMatchPusher:
    def __init__(self, collection_name: str, embedder: Optional[HuggingFaceEmbeddings] = None, qdrant: Optional[QdrantClient] = None):
        self.collection_name = collection_name
        self.embedder = embedder or self.get_embedder()
        self.qdrant = qdrant or self.get_qdrant_client()
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
            if e.status_code == 404:
                return False
            logger.warning(f"⚠️ Existence check failed for ID {vector_id}: {e}")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Existence check failed for ID {vector_id}: {e}")
            return False

    def _ensure_collection(self):
        """Create collection if it doesn't exist and setup payload schema."""
        try:
            collection_info = self.qdrant.get_collection(self.collection_name)
            logger.info(f"Collection '{self.collection_name}' already exists.")
            existing_fields = set(collection_info.payload_schema.keys()) if collection_info.payload_schema else set()
        except UnexpectedResponse as e:
            if e.status_code == 404:
                # Collection does not exist → create it
                vector_size = len(self.embedder.embed_query("test"))
                self.qdrant.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
                )
                logger.info(f"🆕 Created Qdrant collection: {self.collection_name}")
                existing_fields = set()
            else:
                logger.warning(f"⚠️ Could not fetch existing collection info: {e}")
                existing_fields = set()
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch existing collection info: {e}")
            existing_fields = set()

        # Setup payload indices
        fields = set(COLLECTION_INDEX_FIELDS.get(self.collection_name, []) + ["text"])
        for field in fields - existing_fields:
            try:
                self.qdrant.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema=PayloadSchemaType.TEXT
                )
                logger.info(f"📚 Created payload index: {field}")
            except Exception as e:
                logger.warning(f"⚠️ Skipped index creation for '{field}' → {e}")

    def push_matches(self, match_docs: List[Document]):
        """Push each match document individually to Qdrant."""
        if not match_docs:
            logger.warning(f"No documents to embed for collection: {self.collection_name}")
            return

        for doc in match_docs:
            match_id = doc.metadata.get("match_id")
            if not match_id:
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

            point = PointStruct(id=vector_id, vector=vector, payload={**doc.metadata, "text": doc.page_content})

            try:
                self.qdrant.upsert(collection_name=self.collection_name, points=[point])
                logger.info(f"✅ Upserted match ID {match_id} to '{self.collection_name}'")
            except UnexpectedResponse as e:
                if e.status_code == 404:
                    logger.warning(f"⚠️ Collection '{self.collection_name}' not found during upsert. Recreating collection.")
                    self._ensure_collection()
                    self.qdrant.upsert(collection_name=self.collection_name, points=[point])
                    logger.info(f"✅ Upserted match ID {match_id} after recreating collection.")
                else:
                    logger.error(f"❌ Qdrant upsert failed for match ID {match_id}: {e}")
            except Exception as e:
                logger.error(f"❌ Qdrant upsert failed for match ID {match_id}: {e}")
