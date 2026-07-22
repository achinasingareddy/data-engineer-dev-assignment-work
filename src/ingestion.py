"""
---------------------------------------------------------
File: ingestion.py
Description:
Ingest University Chapters data from ArcGIS REST API
and land the raw payload into the Bronze layer.
---------------------------------------------------------
"""
import os
import json
import uuid
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO)

BASE_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/"
    "arcgis/rest/services/UniversityChapters_Public/"
    "FeatureServer/0/query"
)
# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

COMMON_PATH ="/Volumes/dev_engineering_catalog/dev_engineering_schema/dev_engineering_volume"
BRONZE_PATH = f"{COMMON_PATH}/bronze/university_chapters"

class BronzeIngestion:

    def __init__(self, base_url, bronze_path):
        self.base_url = base_url
        self.bronze_path = bronze_path
        self.logger = logging.getLogger(__name__)
# ----------------------------------------------------------
# Generate Run ID
# ----------------------------------------------------------

    def generate_run_id(self):
        return f"{datetime.utcnow():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"

# ----------------------------------------------------------
# Create Bronze Folder
# ----------------------------------------------------------

    def create_directory(self, run_id):
        folder = os.path.join(self.bronze_path, run_id)
        os.makedirs(folder, exist_ok=True)
        return folder
    
# ----------------------------------------------------------
# Call ArcGIS REST API
# ----------------------------------------------------------

    def fetch_data(self):
        params = {
            "where": "State IN ('CA','OR','WA')",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "json"
        }

        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()
# ----------------------------------------------------------
# Save JSON 
# ----------------------------------------------------------

    def save_json(self, folder, filename, data, json_type):

        with open(os.path.join(folder, filename), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        self.logger.info(f"{json_type} saved to {os.path.join(folder, filename)}")

    def run(self):
        try:
            run_id = self.generate_run_id()
            folder = self.create_directory(run_id)
            data = self.fetch_data()
            self.save_json(folder, "raw.json", data,"Raw JSON " )
            features = data.get("features", [])
            self.logger.info(f"Retrieved {len(features)} records")
            metadata = {
                "run_id": run_id,
                "ingest_timestamp_utc": datetime.utcnow().isoformat(),
                "record_count": len(features),
                "source": "ArcGIS University Chapters API",
                "states": [
                "CA",
                "OR",
                "WA"
                ]
            }
            self.save_json(folder, "metadata.json", metadata, "Meta Data JSON ")
            self.logger.info("Bronze ingestion completed successfully.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API Error : {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected Error : {e}")
            
BronzeIngestion(BASE_URL, BRONZE_PATH).run()