import psycopg2
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from config import DB_CONFIG
from index_manager import (
    ensure_tables_exist, 
    get_indexes_for_backfill, 
    update_backfill_status
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('smart_backfill')

# Define the backfill parameters
DEFAULT_START_DATE = "2019-01-01"
MAX_BACKFILL_DAYS = 730  # Limit to 2 years of data

def get_date_range_gaps(symbol):
    """
    Analyze existing data for a symbol and identify missing date ranges.
    Returns:
        - A list of (start_date, end_date) tuples representing gaps
        - The earliest date in the database for this symbol
        - The latest date in the database for this symbol
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Check if any data exists for this symbol
                cursor.execute(
                    "SELECT COUNT(*) FROM stock_prices WHERE symbol = %s", 
                    (symbol,)
                )
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # No data exists, need full backfill
                    start_date = datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d")
                    end_date = datetime.today()
                    
                    # Limit the backfill period to avoid excessive API calls
                    if (end_date - start_date).days > MAX_BACKFILL_DAYS:
                        start_date = end_date - timedelta(days=MAX_BACKFILL_DAYS)
                        
                    return [(start_date, end_date)], None, None
                
                # Find the earliest and latest dates for this symbol
                cursor.execute(
                    "SELECT MIN(timestamp), MAX(timestamp) FROM stock_prices WHERE symbol = %s", 
                    (symbol,)
                )
                earliest_date, latest_date = cursor.fetchone()
                
                # Find date gaps in the time series
                # For hourly data, we're primarily concerned with missing days
                cursor.execute("""
                    WITH days AS (
                        SELECT DISTINCT date_trunc('day', timestamp) AS day 
                        FROM stock_prices 
                        WHERE symbol = %s
                    ),
                    day_series AS (
                        SELECT generate_series(
                            date_trunc('day', MIN(day)),
                            date_trunc('day', MAX(day)),
                            interval '1 day'
                        ) AS day_date
                        FROM days
                    )
                    SELECT day_date
                    FROM day_series
                    WHERE day_date NOT IN (SELECT day FROM days)
                    ORDER BY day_date;
                """, (symbol,))
                
                missing_days = [row[0] for row in cursor.fetchall()]
                
                # Group consecutive days into ranges
                date_ranges = []
                if missing_days:
                    start = missing_days[0]
                    end = start
                    
                    for i in range(1, len(missing_days)):
                        current = missing_days[i]
                        if (current - end).days <= 1:
                            end = current
                        else:
                            # Add a day to include the full day in the range
                            date_ranges.append((start, end + timedelta(days=1)))
                            start = current
                            end = current
                    
                    # Add the last range
                    date_ranges.append((start, end + timedelta(days=1)))
                
                # Check if we need to backfill from the DEFAULT_START_DATE to the earliest date
                desired_start = datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d")
                if earliest_date and desired_start < earliest_date:
                    # Limit how far back we go
                    max_lookback = datetime.today() - timedelta(days=MAX_BACKFILL_DAYS)
                    if desired_start < max_lookback:
                        desired_start = max_lookback
                    
                    if desired_start < earliest_date:
                        date_ranges.append((desired_start, earliest_date))
                
                # Check if we need to backfill from the latest date to today
                today = datetime.today()
                if latest_date and latest_date < today:
                    date_ranges.append((latest_date + timedelta(hours=1), today))
                
                return date_ranges, earliest_date, latest_date
                
    except Exception as e:
        logger.error(f"Error analyzing date ranges for {symbol}: {e}")
        # Default to full backfill if error occurs
        start_date = datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d")
        end_date = datetime.today()
        
        # Limit the backfill period to avoid excessive API calls
        if (end_date - start_date).days > MAX_BACKFILL_DAYS:
            start_date = end_date - timedelta(days=MAX_BACKFILL_DAYS)
            
        return [(start_date, end_date)], None, None

def backfill_symbol(symbol):
    """Backfill missing historical data for a single symbol with hourly data."""
    logger.info(f"Starting smart backfill for {symbol}")
    update_backfill_status(symbol, 'IN_PROGRESS', 'Analyzing existing data to identify gaps')
    
    try:
        # Get missing date ranges
        date_ranges, earliest_date, latest_date = get_date_range_gaps(symbol)
        
        if not date_ranges:
            logger.info(f"No gaps found for {symbol}. Data is complete.")
            update_backfill_status(
                symbol, 
                'COMPLETED', 
                f"No gaps found. Data is complete from {earliest_date} to {latest_date}"
            )
            return True
        
        # Log what we're about to do
        range_str = "; ".join([f"{start} to {end}" for start, end in date_ranges])
        logger.info(f"Found {len(date_ranges)} gaps for {symbol}: {range_str}")
        update_backfill_status(symbol, 'IN_PROGRESS', f"Backfilling {len(date_ranges)} gaps: {range_str}")
        
        # Initialize Yahoo Finance ticker once
        stock = yf.Ticker(symbol)
        all_new_records = 0
        
        # Process each date range
        for start_date, end_date in date_ranges:
            logger.info(f"Processing gap for {symbol}: {start_date} to {end_date}")
            
            # For hourly data, Yahoo Finance limits to ~60 days per request
            # Break down large gaps into smaller chunks
            chunk_size = 60  # days
            current_start = start_date
            
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=chunk_size), end_date)
                chunk_start = current_start.strftime("%Y-%m-%d")
                chunk_end = current_end.strftime("%Y-%m-%d")
                
                logger.info(f"Fetching chunk for {symbol}: {chunk_start} to {chunk_end}")
                
                # Fetch 1-hour data
                try:
                    df_chunk = stock.history(
                        start=chunk_start,
                        end=chunk_end,
                        interval="1h"
                    )
                    
                    if not df_chunk.empty:
                        # Prepare and insert data
                        df_chunk.reset_index(inplace=True)
                        df_chunk["symbol"] = symbol
                        
                        stock_data = []
                        for _, row in df_chunk.iterrows():
                            if isinstance(row["Datetime"], pd.Timestamp):
                                timestamp = row["Datetime"].to_pydatetime()
                            else:
                                timestamp = row["Datetime"]
                                
                            stock_data.append((
                                row["symbol"],
                                timestamp,
                                float(row["Open"]) if not pd.isna(row["Open"]) else None,
                                float(row["High"]) if not pd.isna(row["High"]) else None,
                                float(row["Low"]) if not pd.isna(row["Low"]) else None,
                                float(row["Close"]) if not pd.isna(row["Close"]) else None,
                                int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
                            ))
                        
                        # Insert into database with conflict handling
                        with psycopg2.connect(**DB_CONFIG) as conn:
                            with conn.cursor() as cursor:
                                insert_query = """
                                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (symbol, timestamp) DO NOTHING;
                                """
                                cursor.executemany(insert_query, stock_data)
                                conn.commit()
                                
                        logger.info(f"Added {len(stock_data)} records for {symbol} chunk")
                        all_new_records += len(stock_data)
                    else:
                        logger.warning(f"No data found for chunk {chunk_start} to {chunk_end}")
                
                except Exception as e:
                    logger.error(f"Error fetching chunk {chunk_start} to {chunk_end}: {e}")
                    # Continue with the next chunk despite errors
                
                current_start = current_end + timedelta(days=1)
        
        # Final update on success
        if all_new_records > 0:
            # Check for any remaining gaps
            new_ranges, new_earliest, new_latest = get_date_range_gaps(symbol)
            if new_ranges:
                remaining_gaps = len(new_ranges)
                status = 'PARTIAL'
                note = f"Added {all_new_records} records but {remaining_gaps} gaps remain"
            else:
                status = 'COMPLETED'
                note = f"Successfully added {all_new_records} records, all gaps filled"
        else:
            status = 'PARTIAL'
            note = "No new records added. Check API availability or data source."
        
        update_backfill_status(symbol, status, note)
        logger.info(f"Backfill for {symbol} completed with status: {status}")
        return True
        
    except Exception as e:
        error_msg = f"❌ Error backfilling stock data for {symbol}: {str(e)}"
        logger.error(error_msg)
        update_backfill_status(symbol, 'FAILED', error_msg[:500])  # Limit note length
        return False

def run_smart_backfill_job():
    """Run the smart backfill job for all pending symbols."""
    # Ensure all required tables exist
    ensure_tables_exist()
    
    # Get symbols that need backfilling
    symbols = get_indexes_for_backfill()
    
    if not symbols:
        logger.info("No symbols to backfill.")
        return True
    
    logger.info(f"Starting smart backfill job for {len(symbols)} symbols")
    
    success_count = 0
    failure_count = 0
    
    for symbol in symbols:
        if backfill_symbol(symbol):
            success_count += 1
        else:
            failure_count += 1
    
    logger.info(f"Smart backfill job completed. Success: {success_count}, Failed: {failure_count}")
    return success_count > 0 and failure_count == 0

if __name__ == "__main__":
    run_smart_backfill_job()