import requests
import pandas as pd  # Добавлен импорт pandas
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
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        data = response.json()  # CHANGED: Process JSON response

        if "Time Series (Daily)" not in data:
            logging.error(f"API error or no data for symbol {symbol}: {data}")
            raise ValueError(f"API error or no data for symbol {symbol}. Check API key or symbol.")

        time_series = data["Time Series (Daily)"]

        # ADDED: Convert JSON to DataFrame and rename columns
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        })
        df.index.name = 'timestamp'
        df = df.sort_index(ascending=True)
        df['symbol'] = symbol

        os.makedirs(output_dir, exist_ok=True)

        today = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(output_dir,
                                 f"stock_prices_{symbol}_{today}.csv")

        df.to_csv(file_path)

        logging.info(f"Data extracted successfully to {file_path}")
        return file_path
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Network or API request failed: {req_err}")
        raise
    except ValueError as val_err:
        logging.error(f"Data processing error: {val_err}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction: {str(e)}")
        raise


if __name__ == "__main__":
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
    api_key = os.getenv('API_KEY')
    symbol = os.getenv('SYMBOL')
    if not api_key or not symbol:
        logging.error("API_KEY or SYMBOL not found in .env file.")
    else:
        extract_data(api_key, symbol)