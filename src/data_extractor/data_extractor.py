import requests
import csv
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

API_URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv('API_KEY')
SYMBOL = os.getenv('SYMBOL')

if not API_KEY:
    logging.error("API_KEY environment variable not set!")
    raise ValueError("API_KEY is required.")
if not SYMBOL:
    logging.error("SYMBOL environment variable not set!")
    raise ValueError("SYMBOL is required.")


def extract_data():
    try:
        logging.info(f"Starting data extraction for symbol: {SYMBOL}")
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": SYMBOL,
            "apikey": API_KEY,
            "datatype": "csv"
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        output_dir = "/data/raw"
        os.makedirs(output_dir, exist_ok=True)

        today = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(output_dir, f"stock_prices_{today}.csv")

        with open(file_path, 'w') as f:
            f.write(response.text)

        logging.info(f"Data extracted successfully to {file_path}")
        return file_path
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Network or API request failed: {req_err}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction: {str(e)}")
        raise


if __name__ == "__main__":
    extract_data()