import requests
import psycopg2
import time
import os
import pandas as pd
import yfinance as yf
import numpy as np

# ✅ Environment Variables
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")  # Ensure this is set in Docker
SYMBOL = "AAPL"
INTERVAL = "5min"

# ✅ Database Connection Parameters
DB_PARAMS = {
    'dbname': 'stocks_db',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',
    'port': 5432
}

def wait_for_db():
    """Wait until PostgreSQL is ready."""
    retries = 10
    delay = 5
    for i in range(retries):
        try:
            with psycopg2.connect(**DB_PARAMS) as conn:
                print("✅ PostgreSQL is ready!")
                return
        except psycopg2.OperationalError:
            print(f"⏳ Waiting for PostgreSQL ({i+1}/{retries})...")
            time.sleep(delay)
    print("❌ Could not connect to PostgreSQL. Exiting.")
    exit(1)

def create_table():
    """Create the stock_prices table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10),
        timestamp TIMESTAMP UNIQUE,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC
    );
    """
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
        print("✅ Table stock_prices ensured.")
    except Exception as e:
        print(f"❌ Error creating table: {e}")

def fetch_and_store():
    """Fetch stock data from Yahoo Finance and store it in PostgreSQL."""
    while True:
        try:
            print("🔄 Fetching new stock data...")
            stock = yf.Ticker(SYMBOL)
            latest_data = stock.history(period="1d", interval="5m").iloc[-1]

            # ✅ Convert everything to Python-native types
            stock_values = (
                SYMBOL,
                latest_data.name.to_pydatetime(),  # Convert Pandas Timestamp → Python datetime
                float(latest_data["Open"]),  # Ensure float type
                float(latest_data["High"]),
                float(latest_data["Low"]),
                float(latest_data["Close"]),
                int(latest_data["Volume"])   # Ensure int type
            )

            print(f"✅ Converted Data Types: {[(val, type(val)) for val in stock_values]}")

            # ✅ Insert into PostgreSQL
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp) DO NOTHING;
                        """,
                        stock_values
                    )
                    conn.commit()

            print(f"✅ Inserted data for {SYMBOL}. Waiting 30 seconds...")
            time.sleep(30)

        except Exception as e:
            print(f"❌ Error fetching stock data: {e}")
            time.sleep(10)



if __name__ == "__main__":
    wait_for_db()
    create_table()
    fetch_and_store()
