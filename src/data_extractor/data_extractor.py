import requests
import csv
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv('API_KEY')
SYMBOL = os.getenv('SYMBOL')


def extract_data():
    logging.basicConfig(level=logging.INFO)
    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": SYMBOL,
            "apikey": API_KEY,
            "datatype": "csv"
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        raw_data_dir = os.path.join(base_dir, "data", "raw")

        today = datetime.now().strftime("%Y%m%d")

        os.makedirs(raw_data_dir, exist_ok=True)
        file_path = os.path.join(raw_data_dir, f"stock_prices_{today}.csv")
        #os.makedirs("/data/raw", exist_ok=True)
        #file_path = f"/data/raw/stock_prices_{today}.csv"

        with open(file_path, 'w') as f:
            f.write(response.text)

        logging.info(f"Data extracted to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    extract_data()
