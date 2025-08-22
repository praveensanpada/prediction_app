# create_qdrant_collections_and_indices.py

import os
from typing import Dict, List, Set

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PayloadSchemaType
from qdrant_client.http.exceptions import UnexpectedResponse

# ---------------- CONFIG ----------------
# You can override these via environment variables if you prefer
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")  # or set a string here

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384  # <- your specified dim
MATCH_DETAILS_COLLECTION = "matches_details"
MATCH_STATS_COLLECTION = "stats_details"

COLLECTION_INDEX_FIELDS: Dict[str, List[str]] = {
    MATCH_DETAILS_COLLECTION: [
        "match_id", "away_team_id", "away_team", "away_team_name", "home_team",
        "home_team_id", "home_team_name", "league_id", "league_name", "ground_name",
        "match_format", "venue_id", "player_id", "full_name", "position"
    ],
    MATCH_STATS_COLLECTION: [
        "match_id", "away_team_id", "away_team", "away_team_name", "home_team",
        "home_team_id", "home_team_name", "league_id", "league_name", "ground_name",
        "match_format", "venue_id"
    ],
}
# ----------------------------------------


# Optional fallbacks for older qdrant-client versions
try:
    from qdrant_client.http.models import KeywordIndexParams, IntegerIndexParams, TextIndexParams
except Exception:
    KeywordIndexParams = IntegerIndexParams = TextIndexParams = None


def infer_schema(field: str) -> str:
    """
    Heuristic to choose index schema per field.
    Returns one of: "KEYWORD", "TEXT"
    (Use "INTEGER" only if you actually store numeric types; most IDs are strings.)
    """
    f = field.lower()

    # IDs and short categorical codes
    if f == "match_id" or f.endswith("_id"):
        return "KEYWORD"
    if f in {"home_team", "away_team", "match_format", "venue_id", "position"}:
        return "KEYWORD"

    # Human-readable names / free text
    if f.endswith("_name") or f in {"ground_name", "full_name", "league_name"}:
        return "TEXT"

    # Fallback
    return "TEXT"


def ensure_collection(client: QdrantClient, name: str, vector_size: int, distance=Distance.COSINE) -> None:
    """Create collection if it doesn't exist, otherwise do nothing."""
    try:
        client.get_collection(name)
        print(f"✅ Collection exists: {name}")
    except UnexpectedResponse as e:
        if getattr(e, "status_code", None) == 404:
            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
            )
            print(f"🆕 Created collection: {name} (dim={vector_size}, distance={distance.value})")
        else:
            raise
    except Exception as e:
        raise RuntimeError(f"Failed to ensure collection '{name}': {e}") from e


def create_index(client: QdrantClient, collection: str, field: str, schema: str) -> None:
    """
    Create payload index with compatibility for older/newer qdrant-client versions.
    schema in {"KEYWORD", "TEXT", "INTEGER"}
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

            client.create_payload_index(
                collection_name=collection,
                field_name=field,
                field_schema=idx,
            )
        else:
            # Enum API path
            if schema == "KEYWORD":
                enum_schema = PayloadSchemaType.KEYWORD
            elif schema == "TEXT":
                enum_schema = PayloadSchemaType.TEXT
            elif schema == "INTEGER":
                enum_schema = PayloadSchemaType.INTEGER
            else:
                raise ValueError(f"Unknown schema '{schema}'")

            client.create_payload_index(
                collection_name=collection,
                field_name=field,
                field_schema=enum_schema,
            )
        print(f"   📚 Created index: {collection}.{field} ({schema})")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg:
            print(f"   ⚠️ Index already exists: {collection}.{field}")
        else:
            raise


def get_existing_indexed_fields(client: QdrantClient, collection: str) -> Set[str]:
    """Best-effort read of existing payload schema (may be empty on older servers/clients)."""
    try:
        info = client.get_collection(collection)
        schema = getattr(info, "payload_schema", None)
        if isinstance(schema, dict):
            return set(schema.keys())
    except Exception:
        pass
    return set()


def main():
    client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

    # 1) Ensure collections exist with the correct vector config
    ensure_collection(client, MATCH_DETAILS_COLLECTION, EMBEDDING_DIM, Distance.COSINE)
    ensure_collection(client, MATCH_STATS_COLLECTION, EMBEDDING_DIM, Distance.COSINE)

    # 2) Build index plan per collection (always include "text")
    for collection, fields in COLLECTION_INDEX_FIELDS.items():
        print(f"\n🔹 Processing indices for: {collection}")
        fields_to_index = set(fields) | {"text"}

        existing = get_existing_indexed_fields(client, collection)

        for field in fields_to_index:
            if field in existing:
                print(f"   ✅ Already indexed: {collection}.{field}")
                continue

            schema = infer_schema(field)
            try:
                create_index(client, collection, field, schema)
            except Exception as e:
                print(f"   ❌ Failed index for {collection}.{field} → {e}")

    print("\n🎉 Done.")


if __name__ == "__main__":
    main()
