# === controllers/cron_controller.py ===
from config.settings import SOURCE_URL, SOURCE_URL_1, SOURCE_URL_2, SOURCE_URL_3, SOURCE_URL_4
from utils.logger import get_logger
import requests
from datetime import datetime
from models.cron_model import CronModel
from libraries.api_client import APIClient
from libraries.ai_model import AIModel
from libraries.qdrant_client import QdrantMatchPusher
from langchain_core.documents import Document

cron_model = CronModel()
api_client = APIClient()
ai_model = AIModel()

logger = get_logger(__name__)

def get_upcoming_matches_list():
    try:
        result = requests.get(SOURCE_URL)
        result.raise_for_status()
        fixtures = result.json()
        if fixtures:
            matches = [
                {
                    "match_id": fixture.get("season_game_uid"),
                    "league_id": fixture.get("league_id"),
                    "league_name": fixture.get("league_name"),
                    "home_team": fixture.get("home"),
                    "away_team": fixture.get("away"),
                    "match_format": fixture.get("format"),
                    "match_scheduled_date": fixture.get("season_scheduled_date"),
                    "lineup_announce": fixture.get("playing_announce")
                }
                for fixture in fixtures
            ]
            logger.info(f'✅ Upcoming matches fetched successfully at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "200",
                "responseMessage" : "Upcoming matches fetched successfully.",
                "responseData" : matches
            }
        else:
            logger.info(f'❌ Upcoming matches fetched error at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "400",
                "responseMessage" : "Upcoming matches fetched error.",
                "responseData" : {}
            }
    except Exception as e:
        logger.info(f'❌ Failed to fetch upcoming matches: {e}')
        return {
            "responseCode": "500",
            "responseMessage" : "Failed to fetch upcoming matches.",
            "responseData" : {}
        }
    
def get_upcoming_matches_cron():
    try:
        result = requests.get(SOURCE_URL)
        result.raise_for_status()
        fixtures = result.json()
        if fixtures:
            for fixture in fixtures:

                # if fixture["season_game_uid"] != "91916":
                #     continue

                payload = {}
                payload["sports_id"] = "7"
                payload["season_game_uid"] = fixture["season_game_uid"]
                payload["league_id"] = fixture["league_id"]

                result_1 = api_client.post(SOURCE_URL_1, payload)
                result_2 = api_client.post(SOURCE_URL_2, payload)
                result_3 = api_client.post(SOURCE_URL_3, payload)
                result_4 = api_client.post(SOURCE_URL_4, payload)

                match_details = {}
                match_details["match_id"] = fixture.get("season_game_uid", "")
                match_details["league_id"] = fixture.get("league_id", "")
                match_details["league_name"] = fixture.get("league_name", "")
                match_details["home_team"] = fixture.get("home", "")
                match_details["away_team"] = fixture.get("away", "")
                match_details["match_format"] = fixture.get("format", "")
                match_details["match_scheduled_date"] = fixture.get("season_scheduled_date", "")
                match_details["lineup_announce"] = fixture.get("playing_announce", "")

                if result_1 and result_1.get("data"):
                    result_1_data = result_1.get("data")
                    match_details["ground_name"] = result_1_data.get("ground_name", "")
                    match_details["venue_id"] = result_1_data.get("venue_id", "")
                    match_details["match_title"] = result_1_data.get("subtitle", "")
                    match_details["home_team_id"] = result_1_data.get("home_uid", "")
                    match_details["away_team_id"] = result_1_data.get("away_uid", "")
                    match_details["home_team_name"] = result_1_data.get("home_team", "")
                    match_details["away_team_name"] = result_1_data.get("away_team", "")

                if result_4 and result_4.get("data"):
                    result_4_data = result_4.get("data")

                    home_squad = [
                        {
                            "team_id": player.get("team_uid"),
                            "player_id": player.get("player_uid"),
                            "full_name": player.get("full_name"),
                            "nick_name": player.get("nick_name"),
                            "position": player.get("position"),
                            "last_match_played": player.get("last_match_played")
                        }
                        for player in result_4_data
                        if player.get("team_uid") ==  match_details["home_team_id"]
                    ]
                    match_details["home_team_squad"] = home_squad

                    away_squad = [
                        {
                            "team_id": player.get("team_uid"),
                            "player_id": player.get("player_uid"),
                            "full_name": player.get("full_name"),
                            "nick_name": player.get("nick_name"),
                            "position": player.get("position"),
                            "last_match_played": player.get("last_match_played")
                        }
                        for player in result_4_data
                        if player.get("team_uid") ==  match_details["away_team_id"]
                    ]
                    match_details["away_team_squad"] = away_squad
                
                match_stats = {}
                match_stats["match_id"] = fixture.get("season_game_uid", "")
                match_stats["league_id"] = fixture.get("league_id", "")
                match_stats["league_name"] = fixture.get("league_name", "")
                match_stats["home_team"] = fixture.get("home", "")
                match_stats["away_team"] = fixture.get("away", "")
                match_stats["match_format"] = fixture.get("format", "")
                match_stats["match_scheduled_date"] = fixture.get("season_scheduled_date", "")
                match_stats["lineup_announce"] = fixture.get("playing_announce", "")

                if result_1 and result_1.get("data"):
                    result_1_data = result_1.get("data")
                    match_stats["ground_name"] = result_1_data.get("ground_name", "")
                    match_stats["venue_id"] = result_1_data.get("venue_id", "")
                    match_stats["match_title"] = result_1_data.get("subtitle", "")
                    match_stats["home_team_id"] = result_1_data.get("home_uid", "")
                    match_stats["away_team_id"] = result_1_data.get("away_uid", "")
                    match_stats["home_team_name"] = result_1_data.get("home_team", "")
                    match_stats["away_team_name"] = result_1_data.get("away_team", "")

                if result_2 and result_2.get("data"):
                    result_2_data = result_2.get("data")

                    toss_trend = result_2_data.get("toss_trend", {})
                    if toss_trend:
                        match_stats["bat_first_win_on_this_venue"] = toss_trend.get("bat_first_win", "")
                        match_stats["bat_second_win_on_this_venue"] = toss_trend.get("bat_second_win", "")
                        match_stats["total_matches_played_on_this_venue"] = toss_trend.get("total_matches", "")

                    statement_tip = result_2_data.get("statement_tip", {})
                    if statement_tip:
                        match_stats["pitch_support_type"] = statement_tip.get("bat_type", "")
                        match_stats["bowling_support_type"] = statement_tip.get("bow_type", "")

                    recent_matches_stats = result_2_data.get("recent_matches_stats", {})
                    if recent_matches_stats:
                        match_stats["avg_first_inning_score"] = recent_matches_stats.get("avg_first_score", "")
                        match_stats["avg_second_inning_score"] = recent_matches_stats.get("avg_second_score", "")
                        match_stats["avg_first_inning_wicket"] = recent_matches_stats.get("avg_first_wicket", "")
                        match_stats["avg_second_inning_wicket"] = recent_matches_stats.get("avg_second_wicket", "")

                    venue_pitch_report = result_2_data.get("venue_pitch_report", {})
                    if venue_pitch_report:
                        match_stats["pitch_support_description"] = venue_pitch_report.get("pitch_support", "")
                        match_stats["bowling_support_description"] = venue_pitch_report.get("bowling_support", "")
                        match_stats["weather_report_description"] = venue_pitch_report.get("weather_report", "")

                    weather = result_2_data.get("weather", {})
                    if weather:
                        match_stats["temperature"] = weather.get("temp", "")
                        match_stats["clouds"] = weather.get("clouds", "")
                        match_stats["weather"] = weather.get("weather", "")
                        match_stats["humidity"] = weather.get("humidity", "")
                        match_stats["visibility"] = weather.get("visibility", "")
                        match_stats["wind_speed"] = weather.get("wind_speed", "")
                        match_stats["weather_desc"] = weather.get("weather_desc", "")
                
                if result_3 and result_3.get("data"):
                    result_3_data = result_3.get("data")

                    score_prediction = result_3_data.get("score_prediction", {})
                    if score_prediction and score_prediction[match_details["home_team_id"]]:
                        score_data = score_prediction[match_details["home_team_id"]]
                        match_stats["home_team_score_prediction"] = score_data["score"]
                        match_stats["home_team_wicket_prediction"] = score_data["wickets"]
                    if score_prediction and score_prediction[match_details["away_team_id"]]:
                        score_data = score_prediction[match_details["away_team_id"]]
                        match_stats["away_team_score_prediction"] = score_data["score"]
                        match_stats["away_team_wicket_prediction"] = score_data["wickets"]

                    win_margin_data = result_3_data.get("win_margin_data", {})
                    if win_margin_data:
                        win_team_id = win_margin_data.get("team_uid", "")
                        if win_team_id == match_details["home_team_id"]:
                            match_stats["win_team_id"] = match_details["home_team_id"]
                            match_stats["win_team_name"] = match_details["home_team_name"]
                            match_stats["win_team_win_probability"] = win_margin_data.get("win_probability", "")
                            match_stats["win_team_run"] = win_margin_data.get("run", "")
                            match_stats["win_team_wicket"] = win_margin_data.get("wicket", "")
                        elif win_team_id == match_details["away_team_id"]:
                            match_stats["win_team_id"] = match_details["away_team_id"]
                            match_stats["win_team_name"] = match_details["away_team_name"]
                            match_stats["win_team_win_probability"] = win_margin_data.get("win_probability", "")
                            match_stats["win_team_run"] = win_margin_data.get("run", "")
                            match_stats["win_team_wicket"] = win_margin_data.get("wicket", "")      
                
                match_details_desciption = cron_model.get_match_description("match_details")
                match_details_summary = ai_model.generate_documentation(match_details, match_details_desciption)
                match_details["match_details_summary"] = match_details_summary

                match_stats_desciption = cron_model.get_match_description("match_stats")
                match_stats_summary = ai_model.generate_documentation(match_stats, match_stats_desciption)
                match_stats["match_stats_summary"] = match_stats_summary
            
                upsert_match_detail_res = cron_model.upsert_match_detail_by_id(match_stats["match_id"], match_details)
                upsert_match_stats_res = cron_model.upsert_match_stats_by_id(match_stats["match_id"], match_stats)

            logger.info(f'✅ upsert_match_detail_res and upsert_match_stats_res successfully at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "200",
                "responseMessage" : "upsert_match_detail_res and upsert_match_stats_res successfully.",
                "responseData" : {}
            }
        else:
            logger.info(f'❌ upsert_match_detail_res and upsert_match_stats_res {match_stats["match_id"]} error at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "400",
                "responseMessage" : "upsert_match_detail_res and upsert_match_stats_res error.",
                "responseData" : {}
            }
    except Exception as e:
        logger.info(f'❌ Failed to upsert_match_detail_res and upsert_match_stats_res {match_stats["match_id"]}: {e}')
        return {
            "responseCode": "500",
            "responseMessage" : "Failed to upsert_match_detail_res and upsert_match_stats_res.",
            "responseData" : {}
        }
    
def get_upcoming_matches_embeding():
    try:
        result = requests.get(SOURCE_URL)
        result.raise_for_status()
        fixtures = result.json()
        if fixtures:

            match_details_docs = []
            match_stats_docs = []

            for fixture in fixtures:

                match_detail_res = cron_model.get_match_details_by_id(fixture["season_game_uid"])
                if match_detail_res and match_detail_res["match_details_summary"]:
                    match_detail_metadata = {}
                    match_detail_metadata["match_id"] = match_detail_res["match_id"]
                    match_detail_metadata["away_team"] = match_detail_res["away_team"]
                    match_detail_metadata["away_team_id"] = match_detail_res["away_team_id"]
                    match_detail_metadata["away_team_name"] = match_detail_res["away_team_name"]
                    match_detail_metadata["ground_name"] = match_detail_res["ground_name"]
                    match_detail_metadata["home_team"] = match_detail_res["home_team"]
                    match_detail_metadata["home_team_id"] = match_detail_res["home_team_id"]
                    match_detail_metadata["home_team_name"] = match_detail_res["home_team_name"]
                    match_detail_metadata["league_id"] = match_detail_res["league_id"]
                    match_detail_metadata["league_name"] = match_detail_res["league_name"]
                    match_detail_metadata["match_format"] = match_detail_res["match_format"]
                    match_detail_metadata["match_title"] = match_detail_res["match_title"]
                    match_detail_metadata["match_scheduled_date"] = match_detail_res["match_scheduled_date"]
                    match_detail_metadata["venue_id"] = match_detail_res["venue_id"]
                    match_details_docs.append(Document(page_content=match_detail_res["match_details_summary"], metadata=match_detail_metadata))
                      
                match_stats_res = cron_model.get_match_stats_by_id(fixture["season_game_uid"])
                if match_stats_res and match_stats_res["match_stats_summary"]:
                    match_stats_metadata = {}
                    match_stats_metadata["match_id"] = match_stats_res["match_id"]
                    match_stats_metadata["away_team"] = match_stats_res["away_team"]
                    match_stats_metadata["away_team_id"] = match_stats_res["away_team_id"]
                    match_stats_metadata["away_team_name"] = match_stats_res["away_team_name"]
                    match_stats_metadata["ground_name"] = match_stats_res["ground_name"]
                    match_stats_metadata["home_team"] = match_stats_res["home_team"]
                    match_stats_metadata["home_team_id"] = match_stats_res["home_team_id"]
                    match_stats_metadata["home_team_name"] = match_stats_res["home_team_name"]
                    match_stats_metadata["league_id"] = match_stats_res["league_id"]
                    match_stats_metadata["league_name"] = match_stats_res["league_name"]
                    match_stats_metadata["match_format"] = match_stats_res["match_format"]
                    match_stats_metadata["match_title"] = match_stats_res["match_title"]
                    match_stats_metadata["match_scheduled_date"] = match_stats_res["match_scheduled_date"]
                    match_stats_metadata["venue_id"] = match_stats_res["venue_id"]
                    match_stats_docs.append(Document(page_content=match_stats_res["match_stats_summary"], metadata=match_stats_metadata))

            pusher = QdrantMatchPusher(collection_name="match_details")
            pusher.push_matches(match_details_docs)

            pusher = QdrantMatchPusher(collection_name="match_stats")
            pusher.push_matches(match_stats_docs)

            logger.info(f'✅ Upcoming matches embeding successfully at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "200",
                "responseMessage" : "Upcoming matches embeding successfully.",
                "responseData" : {}
            }
        else:
            logger.info(f'❌ Upcoming matches embeding error at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return {
                "responseCode": "400",
                "responseMessage" : "Upcoming matches embeding error.",
                "responseData" : {}
            }
    except Exception as e:
        logger.info(f'❌ Failed to embeding upcoming matches: {e}')
        return {
            "responseCode": "500",
            "responseMessage" : "Failed to embeding upcoming matches.",
            "responseData" : {}
        }