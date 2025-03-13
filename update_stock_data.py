import psycopg2
import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'stocks_db',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',
    'port': 5432
}

def get_tracked_symbols():
    """Fetch all stock symbols that already exist in the database."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT symbol FROM stock_prices;")
                tracked_symbols = {row[0] for row in cursor.fetchall()}
        return tracked_symbols
    except Exception as e:
        print(f"❌ Error fetching tracked symbols: {e}")
        return set()

def update_data():
    """Fetch the latest stock data and insert it into the database."""
    tracked_symbols = get_tracked_symbols()

    if not tracked_symbols:
        print("⚠️ No companies to update.")
        return

    for symbol in tracked_symbols:
        print(f"🔄 Updating data for {symbol}...")

        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period="1d", interval="1d")

            if df.empty:
                print(f"⚠️ No data found for {symbol}. Skipping...")
                continue

            latest = df.iloc[-1]
            timestamp = latest.name.to_pydatetime()

            stock_values = (
                symbol,
                timestamp,
                float(latest["Open"]),
                float(latest["High"]),
                float(latest["Low"]),
                float(latest["Close"]),
                int(latest["Volume"])
            )

            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp) DO UPDATE
                        SET open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume;
                        """,
                        stock_values
                    )
                    conn.commit()

            print(f"✅ Updated data for {symbol}.")

        except Exception as e:
            print(f"❌ Error updating stock data for {symbol}: {e}")

if __name__ == "__main__":
    update_data()

import sys
sys.exit(0)  # ✅ Force script to exit successfully, even if there are warnings
