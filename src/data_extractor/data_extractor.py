import requests
import csv
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

API_URL = "https://www.alphavantage.co/query"


def extract_data(api_key: str, symbol: str, output_dir: str = "/data/raw"):
    try:
        logging.info(f"Starting data extraction for symbol: {symbol}")
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": api_key,
            "datatype": "csv"
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

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
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
    api_key = os.getenv('API_KEY')
    symbol = os.getenv('SYMBOL')
    if not api_key or not symbol:
        logging.error("API_KEY или SYMBOL не найдены в .env файле.")
    else:
        extract_data(api_key, symbol)