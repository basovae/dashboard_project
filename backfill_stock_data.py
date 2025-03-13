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

def ensure_table_exists():
    """Ensures that stock_prices table exists before inserting data."""
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    CONSTRAINT unique_stock_time UNIQUE (symbol, timestamp)
                );
            """)
            conn.commit()

# Ensure the table exists before fetching data
ensure_table_exists()

NEW_COMPANIES = ["NVDA", "AAPL", "GOOGL"]  # Add new stocks as needed
START_DATE = "2019-03-08"
END_DATE = datetime.today().strftime("%Y-%m-%d")  # Get today's date

def get_existing_symbols():
    """Fetch all existing stock symbols from the database to avoid duplicates."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT symbol FROM stock_prices;")
                existing_symbols = {row[0] for row in cursor.fetchall()}
        return existing_symbols
    except Exception as e:
        print(f"❌ Error fetching existing symbols: {e}")
        return set()

def backfill_data():
    """Fetch historical data for new companies and insert it into the database."""
    new_symbols = NEW_COMPANIES  # Always backfill all defined stocks
    ##existing_symbols = get_existing_symbols()
    ##new_symbols = [symbol for symbol in NEW_COMPANIES if symbol not in existing_symbols]

    if not new_symbols:
        print("✅ No new companies to backfill.")
        return

    for symbol in new_symbols:
        print(f"🔄 Backfilling data for {symbol}...")

        try:
            stock = yf.Ticker(symbol)
            df = stock.history(start=START_DATE, end=END_DATE, interval="1d")
            print(f"📊 Data for {symbol}:\n", df.head())  # Print first 5 rows

            if df.empty:
                print(f"⚠️ No data found for {symbol}. Skipping...")
                continue

            df.reset_index(inplace=True)
            df["symbol"] = symbol  # Add symbol column

            stock_data = [
                (
                    row["symbol"],
                    row["Date"].to_pydatetime(),
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    int(row["Volume"])
                )
                for _, row in df.iterrows()
            ]

            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cursor:
                    insert_query = """
                    INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO NOTHING;
                    """
                    cursor.executemany(insert_query, stock_data)
                    conn.commit()

            print(f"✅ Successfully backfilled {len(stock_data)} records for {symbol}.")

        except Exception as e:
            print(f"❌ Error backfilling stock data for {symbol}: {e}")

if __name__ == "__main__":
    backfill_data()
