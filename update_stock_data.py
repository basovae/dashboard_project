import psycopg2
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from config import DB_CONFIG
from index_manager import (
    ensure_tables_exist, 
    get_indexes_for_update, 
    update_last_update_timestamp
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('update_job')

def get_last_data_timestamp(symbol):
    """Get the timestamp of the latest data point for a symbol."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT MAX(timestamp) FROM stock_prices 
                    WHERE symbol = %s;
                """, (symbol,))
                
                last_timestamp = cursor.fetchone()[0]
                
                if last_timestamp:
                    return last_timestamp
                
                return None
                
    except Exception as e:
        logger.error(f"Error getting last timestamp for {symbol}: {e}")
        return None

def update_symbol(symbol):
    """Update hourly data for a single symbol."""
    logger.info(f"Starting update for {symbol}")
    
    try:
        # Get the last data point timestamp
        last_timestamp = get_last_data_timestamp(symbol)
        
        if not last_timestamp:
            logger.warning(f"No existing data found for {symbol}, consider running backfill")
            update_last_update_timestamp(symbol, False)
            return False
        
        # Calculate the start date for update (with some overlap for safety)
        start_date = (last_timestamp - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        
        logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
        
        # Get the stock data
        stock = yf.Ticker(symbol)
        
        # For hourly updates, we may need to chunk if the gap is large
        all_data = []
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Use smaller chunks for hourly data (Yahoo Finance limitation)
        chunk_size = 7  # days - smaller chunk for hourly data
        
        while current_start < end_datetime:
            current_end = min(current_start + timedelta(days=chunk_size), end_datetime)
            chunk_start = current_start.strftime("%Y-%m-%d")
            chunk_end = current_end.strftime("%Y-%m-%d")
            
            logger.info(f"Fetching chunk for {symbol}: {chunk_start} to {chunk_end}")
            
            # Fetch 1-hour data
            df_chunk = stock.history(
                start=chunk_start,
                end=chunk_end,
                interval="1h"
            )
            
            if not df_chunk.empty:
                all_data.append(df_chunk)
                logger.info(f"Retrieved {len(df_chunk)} hourly records for chunk")
            else:
                logger.warning(f"No hourly data found for chunk {chunk_start} to {chunk_end}")
                
            current_start = current_end + timedelta(days=1)
        
        # Combine all chunks
        if all_data:
            df = pd.concat(all_data)
            logger.info(f"Total records for {symbol}: {len(df)}")
        else:
            logger.warning(f"No new data found for {symbol}")
            # Still mark as successful since no new data is not an error
            update_last_update_timestamp(symbol, True)
            return True
        
        if df.empty:
            logger.warning(f"Empty dataframe for {symbol}")
            # Still mark as successful since no new data is not an error
            update_last_update_timestamp(symbol, True)
            return True
        
        # Prepare data for insertion
        df.reset_index(inplace=True)
        df["symbol"] = symbol
        
        # Convert to list of tuples for batch insertion or update
        stock_data = []
        for _, row in df.iterrows():
            # Handle both Timestamp and datetime objects in index
            if isinstance(row["Date"], pd.Timestamp):
                timestamp = row["Date"].to_pydatetime()
            else:
                timestamp = row["Date"]
                
            stock_data.append((
                row["symbol"],
                timestamp,
                float(row["Open"]) if not pd.isna(row["Open"]) else None,
                float(row["High"]) if not pd.isna(row["High"]) else None,
                float(row["Low"]) if not pd.isna(row["Low"]) else None,
                float(row["Close"]) if not pd.isna(row["Close"]) else None,
                int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
            ))
        
        # Insert or update data in the database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                upsert_query = """
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """
                cursor.executemany(upsert_query, stock_data)
                conn.commit()
        
        # Update the last update timestamp
        update_last_update_timestamp(symbol, True)
        logger.info(f"✅ Successfully updated {len(stock_data)} records for {symbol}")
        return True
        
    except Exception as e:
        error_msg = f"❌ Error updating stock data for {symbol}: {str(e)}"
        logger.error(error_msg)
        update_last_update_timestamp(symbol, False)
        return False

def run_update_job():
    """Run the update job for all symbols that need updating."""
    # Ensure all required tables exist
    ensure_tables_exist()
    
    # Get symbols that need updating
    symbols = get_indexes_for_update()
    
    if not symbols:
        logger.info("No symbols to update.")
        return True
    
    logger.info(f"Starting update job for {len(symbols)} symbols")
    
    success_count = 0
    failure_count = 0
    
    for symbol in symbols:
        if update_symbol(symbol):
            success_count += 1
        else:
            failure_count += 1
    
    logger.info(f"Update job completed. Success: {success_count}, Failed: {failure_count}")
    return success_count > 0 and failure_count == 0

if __name__ == "__main__":
    run_update_job()