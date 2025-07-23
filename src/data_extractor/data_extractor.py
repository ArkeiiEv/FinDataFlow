import requests
import csv
import os
import logging
from datetime import datetime

API_URL = "https://www.alphavantage.co/query"
API_KEY = "U8JFI4JGIG1L23F1"
SYMBOL = "IBM"


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

        today = datetime.now().strftime("%Y%m%d")
        os.makedirs("/data/raw", exist_ok=True)
        file_path = f"/data/raw/stock_prices_{today}.csv"

        with open(file_path, 'w') as f:
            f.write(response.text)

        logging.info(f"Data extracted to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    extract_data()
