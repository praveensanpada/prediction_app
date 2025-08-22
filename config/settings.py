# === config/settings.py ===
import os
from dotenv import load_dotenv

load_dotenv()

# Port
PORT = int(os.getenv("PORT", 8000))

#TP Source
SOURCE_URL = os.getenv("SOURCE_URL")
SOURCE_URL_1 = os.getenv("SOURCE_URL_1")
SOURCE_URL_2 = os.getenv("SOURCE_URL_2")
SOURCE_URL_3 = os.getenv("SOURCE_URL_3")
SOURCE_URL_4 = os.getenv("SOURCE_URL_4")

#Mongo URL
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB = os.getenv("MONGO_DB")

#LLM
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

#Vector URL
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")

#Vector Config
# EMBEDDING_MODEL = "all-MiniLM-L6-v2"
# EMBEDDING_DIM = 384
# MATCH_DETAILS_COLLECTION = "matches_details"
# MATCH_STATS_COLLECTION = "stats_details"
# COLLECTION_INDEX_FIELDS = {
#     MATCH_DETAILS_COLLECTION: [
#         "match_id", "away_team_id", "away_team", "away_team_name", "home_team", "home_team_id", "home_team_name", "league_id", "league_name", "ground_name", "match_format", "venue_id", "player_id", "full_name", "position"
#     ],
#     MATCH_STATS_COLLECTION: [
#         "match_id", "away_team_id", "away_team", "away_team_name", "home_team", "home_team_id", "home_team_name", "league_id", "league_name", "ground_name", "match_format", "venue_id"
#     ]
# }

# Vector Config
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# Collections
MATCH_DETAILS_COLLECTION = "match_details"
MATCH_STATS_COLLECTION   = "match_stats"

# Index fields per collection
COLLECTION_INDEX_FIELDS = {
    MATCH_DETAILS_COLLECTION: [
        "match_id", "away_team_id", "away_team", "away_team_name",
        "home_team", "home_team_id", "home_team_name", "league_id",
        "league_name", "ground_name", "match_format", "venue_id",
        "player_id", "full_name", "position",
    ],
    MATCH_STATS_COLLECTION: [
        "match_id", "away_team_id", "away_team", "away_team_name",
        "home_team", "home_team_id", "home_team_name", "league_id",
        "league_name", "ground_name", "match_format", "venue_id",
    ],
}

# IMPORTANT: choose how your IDs are stored in payload:
#   - If payload has numbers (e.g., match_id: 88311 without quotes) → use "INTEGER"
#   - If payload has strings (e.g., match_id: "88311") → use "KEYWORD"
ID_INDEX_TYPE = "INTEGER"  # or "KEYWORD"