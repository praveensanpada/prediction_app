# libraries/ai_model.py
from config.settings import OPENAI_API_KEY
from openai import OpenAI
from typing import Dict
import json

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not found in config.settings")

SYSTEM_PROMPT = "You are an expert cricket analyst and documentation assistant."

class AIModel:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)

    def call_ai_api(
        self,
        prompt: str,
        model: str = "gpt-4o-mini",  
        max_tokens: int = 2048,
        temperature: float = 0.7
    ) -> str:
        """
        Call OpenAI API (new interface) with dynamic prompt.
        Optimized for faster response.
        """
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error: {e}"

    def generate_documentation(self, match_data: Dict, master_description: Dict) -> str:
        """
        Generate detailed documentation for cricket match data.
        Optimized JSON formatting for faster AI processing.
        """

        match_json = json.dumps(match_data, separators=(',', ':'))
        master_json = json.dumps(master_description, separators=(',', ':'))

        documentation_prompt = (
            f"I have the master description keys for cricket/fantasy match data:\n{master_json}\n\n"
            f"And the upcoming match/fantasy data JSON is:\n{match_json}\n\n"
            "Please generate a comprehensive, detailed summary of this match data, explaining each key and its value. "
            "Organize the summary under meaningful sections for easy reading and fantasy analysis:\n\n"
            "1. Match & Tournament Info: Include match title, match ID, league name, match format, and scheduled date/time.\n"
            "2. Teams Overview: Provide home and away team names, team IDs, and any relevant squad info.\n"
            "3. Player Details: List all players with roles (BAT, BOW, AR, WK), last match played, and nicknames.\n"
            "4. Venue & Pitch Info: Include ground name, venue ID, total matches played at venue, pitch support description and type.\n"
            "5. Weather & Conditions: Temperature, humidity, clouds, visibility, wind speed, weather description, and forecast summary.\n"
            "6. Historical & Predicted Scores: Average first and second inning scores, predicted team scores, predicted wickets, and winning probabilities.\n"
            "7. Bowling & Batting Support: Describe bowling support type/description and batting/pitch support insights.\n"
            "8. Match Outcome Prediction: Include predicted win team, run difference, wicket difference, and winning chance.\n\n"
            "Produce a human-readable, structured summary in paragraph form, clearly explaining each data point, "
            "and group related information together so it can be directly used for fantasy cricket analysis and reporting."
        )

        return self.call_ai_api(prompt=documentation_prompt)
