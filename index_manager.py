import psycopg2
from config import DB_CONFIG
from datetime import datetime, timedelta
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('index_manager')

def ensure_tables_exist():
    """Ensure both stock_prices and stock_indexes tables exist with proper schema."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            # Create stock_prices table if it doesn't exist
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
            
            # Create stock_indexes table if it doesn't exist with data quality fields
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_indexes (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL UNIQUE,
                    display_name VARCHAR(100),
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    backfill_status VARCHAR(20) DEFAULT 'PENDING',
                    backfill_start_date TIMESTAMP,
                    backfill_end_date TIMESTAMP,
                    last_update_attempt TIMESTAMP,
                    last_successful_update TIMESTAMP,
                    update_frequency VARCHAR(20) DEFAULT 'HOURLY',
                    data_start_date TIMESTAMP,
                    data_end_date TIMESTAMP,
                    record_count INTEGER,
                    day_coverage_pct NUMERIC,
                    hour_coverage_pct NUMERIC,
                    missing_days INTEGER,
                    gap_stats JSONB,
                    notes TEXT
                );
                
                -- Create indexes if they don't exist
                CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices(symbol);
                CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp ON stock_prices(timestamp);
                CREATE INDEX IF NOT EXISTS idx_stock_indexes_symbol ON stock_indexes(symbol);
                CREATE INDEX IF NOT EXISTS idx_stock_indexes_status ON stock_indexes(backfill_status);
            """)
            
            # Create views
            cursor.execute("""
                CREATE OR REPLACE VIEW indexes_needing_backfill AS
                SELECT si.symbol, si.backfill_status, si.added_date,
                       si.data_start_date, si.data_end_date, si.day_coverage_pct
                FROM stock_indexes si
                LEFT JOIN (
                    SELECT DISTINCT symbol FROM stock_prices
                ) sp ON si.symbol = sp.symbol
                WHERE (
                    sp.symbol IS NULL 
                    OR si.backfill_status = 'PENDING' 
                    OR si.backfill_status = 'FAILED'
                    OR si.backfill_status = 'PARTIAL'
                )
                AND si.is_active = TRUE;
            """)
            
            cursor.execute("""
                CREATE OR REPLACE VIEW indexes_for_updates AS
                SELECT si.symbol, si.update_frequency, si.last_successful_update,
                       si.data_end_date
                FROM stock_indexes si
                JOIN (
                    SELECT DISTINCT symbol FROM stock_prices
                ) sp ON si.symbol = sp.symbol
                WHERE (
                    si.backfill_status = 'COMPLETED'
                    OR si.backfill_status = 'PARTIAL'
                )
                AND si.is_active = TRUE;
            """)
            
            # Create view for data quality
            cursor.execute("""
                CREATE OR REPLACE VIEW index_data_quality AS
                SELECT 
                    symbol,
                    MIN(timestamp) as first_date,
                    MAX(timestamp) as last_date,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT DATE(timestamp)) as days_with_data,
                    (SELECT COUNT(*) FROM (
                        SELECT DISTINCT symbol FROM stock_prices
                    ) s) as total_symbols
                FROM stock_prices
                GROUP BY symbol;
            """)
            
            conn.commit()
            logger.info("Tables and views created/verified successfully")

def add_index(symbol, display_name=None, update_frequency='HOURLY', notes=None):
    """
    Add a new stock index to track.
    Returns True if successfully added, False if the symbol already exists.
    """
    if display_name is None:
        display_name = symbol
        
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO stock_indexes 
                    (symbol, display_name, update_frequency, notes) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE
                    SET is_active = TRUE,
                        display_name = EXCLUDED.display_name,
                        update_frequency = EXCLUDED.update_frequency,
                        notes = CASE WHEN %s IS NOT NULL THEN %s ELSE stock_indexes.notes END
                    RETURNING id;
                """, (symbol, display_name, update_frequency, notes, notes, notes))
                
                index_id = cursor.fetchone()[0]
                conn.commit()
                
                # Update the data quality statistics for this symbol
                update_index_statistics(symbol)
                
                logger.info(f"Successfully added/updated index {symbol} (ID: {index_id})")
                return True
                
    except Exception as e:
        logger.error(f"Error adding index {symbol}: {e}")
        return False

def get_indexes_for_backfill():
    """Get a list of all indexes that need backfilling."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT symbol FROM indexes_needing_backfill
                    ORDER BY 
                        CASE WHEN day_coverage_pct IS NULL THEN 0 ELSE day_coverage_pct END ASC,
                        added_date ASC;
                """)
                
                symbols = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(symbols)} indexes needing backfill")
                return symbols
                
    except Exception as e:
        logger.error(f"Error getting indexes for backfill: {e}")
        return []

def get_indexes_for_update():
    """Get a list of all indexes that need regular updates."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT symbol FROM indexes_for_updates
                    ORDER BY 
                        data_end_date ASC NULLS FIRST,
                        last_successful_update ASC NULLS FIRST;
                """)
                
                symbols = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(symbols)} indexes for regular updates")
                return symbols
                
    except Exception as e:
        logger.error(f"Error getting indexes for updates: {e}")
        return []

def update_backfill_status(symbol, status, notes=None):
    """Update the backfill status for a symbol."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                if status == 'IN_PROGRESS':
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET backfill_status = %s,
                            backfill_start_date = CURRENT_TIMESTAMP,
                            notes = CASE WHEN %s IS NOT NULL THEN %s ELSE notes END
                        WHERE symbol = %s;
                    """, (status, notes, notes, symbol))
                
                elif status == 'COMPLETED':
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET backfill_status = %s,
                            backfill_end_date = CURRENT_TIMESTAMP,
                            last_successful_update = CURRENT_TIMESTAMP,
                            notes = CASE WHEN %s IS NOT NULL THEN %s ELSE notes END
                        WHERE symbol = %s;
                    """, (status, notes, notes, symbol))
                
                elif status == 'PARTIAL':
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET backfill_status = %s,
                            backfill_end_date = CURRENT_TIMESTAMP,
                            last_successful_update = CURRENT_TIMESTAMP,
                            notes = CASE WHEN %s IS NOT NULL THEN %s ELSE notes END
                        WHERE symbol = %s;
                    """, (status, notes, notes, symbol))
                
                else:  # 'FAILED' or other statuses
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET backfill_status = %s,
                            notes = CASE WHEN %s IS NOT NULL THEN %s ELSE notes END
                        WHERE symbol = %s;
                    """, (status, notes, notes, symbol))
                
                conn.commit()
                
                # Update the statistics for this symbol
                update_index_statistics(symbol)
                
                logger.info(f"Updated backfill status for {symbol} to {status}")
                return True
                
    except Exception as e:
        logger.error(f"Error updating backfill status for {symbol}: {e}")
        return False

def update_last_update_timestamp(symbol, success=True):
    """Update the last update timestamp for regular index updates."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                if success:
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET last_update_attempt = CURRENT_TIMESTAMP,
                            last_successful_update = CURRENT_TIMESTAMP
                        WHERE symbol = %s;
                    """, (symbol,))
                else:
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET last_update_attempt = CURRENT_TIMESTAMP
                        WHERE symbol = %s;
                    """, (symbol,))
                    
                conn.commit()
                
                # Update the statistics for this symbol
                update_index_statistics(symbol)
                
                logger.info(f"Updated timestamp for {symbol} (success: {success})")
                return True
                
    except Exception as e:
        logger.error(f"Error updating timestamp for {symbol}: {e}")
        return False

def deactivate_index(symbol, notes=None):
    """
    Deactivate an index (soft delete).
    Returns True if successfully deactivated.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE stock_indexes
                    SET is_active = FALSE,
                        notes = CASE WHEN %s IS NOT NULL THEN %s ELSE notes END
                    WHERE symbol = %s;
                """, (notes, notes, symbol))
                
                conn.commit()
                logger.info(f"Deactivated index {symbol}")
                return True
                
    except Exception as e:
        logger.error(f"Error deactivating index {symbol}: {e}")
        return False

def update_index_statistics(symbol):
    """
    Update data quality statistics for a symbol in the stock_indexes table.
    This helps track the completeness of data for each index.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Check if any data exists for this symbol
                cursor.execute(
                    "SELECT COUNT(*) FROM stock_prices WHERE symbol = %s", 
                    (symbol,)
                )
                record_count = cursor.fetchone()[0]
                
                if record_count == 0:
                    # No records - update with nulls
                    cursor.execute("""
                        UPDATE stock_indexes
                        SET record_count = 0,
                            data_start_date = NULL,
                            data_end_date = NULL,
                            day_coverage_pct = NULL,
                            hour_coverage_pct = NULL,
                            missing_days = NULL,
                            gap_stats = NULL
                        WHERE symbol = %s;
                    """, (symbol,))
                    conn.commit()
                    return True
                
                # Get basic statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as record_count,
                        MIN(timestamp) as first_record,
                        MAX(timestamp) as last_record,
                        COUNT(DISTINCT DATE(timestamp)) as unique_days
                    FROM stock_prices
                    WHERE symbol = %s
                """, (symbol,))
                
                stats = cursor.fetchone()
                record_count = stats[0]
                first_date = stats[1]
                last_date = stats[2]
                unique_days = stats[3]
                
                # Calculate days covered percentage
                days_span = (last_date - first_date).days + 1
                if days_span > 0:
                    # Adjust for weekends (approximately 5/7 of days are trading days)
                    trading_days = int(days_span * 5/7)
                    day_coverage_pct = round((unique_days / trading_days) * 100, 2)
                else:
                    day_coverage_pct = 100.0  # Same day data is "complete"
                
                # Find missing days (gaps)
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
                    ),
                    missing AS (
                        SELECT day_date
                        FROM day_series
                        WHERE 
                            EXTRACT(DOW FROM day_date) NOT IN (0, 6)  -- Exclude weekends
                            AND day_date NOT IN (SELECT day FROM days)
                        ORDER BY day_date
                    )
                    SELECT COUNT(*), array_agg(day_date) FROM missing;
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
                
                # Check if we need to backfill from the desired start date to the earliest date
                # Get the configured start date from environment or default
                from config import BACKFILL_START_DATE, MAX_BACKFILL_DAYS
                desired_start = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d")
                
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
                    # Only add if the gap is more than 4 hours (to avoid redundant API calls)
                    if (today - latest_date).total_seconds() > 4 * 3600:
                        date_ranges.append((latest_date + timedelta(hours=1), today))
                
                return date_ranges, earliest_date, latest_date
                
    except Exception as e:
        logger.error(f"Error analyzing date ranges for {symbol}: {e}")
        # Default to full backfill if error occurs
        from config import BACKFILL_START_DATE, MAX_BACKFILL_DAYS
        start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d")
        end_date = datetime.today()
        
        # Limit the backfill period to avoid excessive API calls
        if (end_date - start_date).days > MAX_BACKFILL_DAYS:
            start_date = end_date - timedelta(days=MAX_BACKFILL_DAYS)
            
        return [(start_date, end_date)], None, None

    # Example usage
    if __name__ == "__main__":
        # Ensure tables exist
        ensure_tables_exist()
        
        # Example: Add some indexes
        add_index("AAPL", "Apple Inc.", "HOURLY")
        add_index("MSFT", "Microsoft Corporation", "HOURLY")
        add_index("GOOGL", "Alphabet Inc.", "HOURLY")
        
        # Example: Get indexes for backfill and update
        backfill_symbols = get_indexes_for_backfill()
        update_symbols = get_indexes_for_update()

        print(f"Symbols needing backfill: {backfill_symbols}")
        print(f"Symbols for regular updates: {update_symbols}")
        hours_result = cursor.fetchone()
        if hours_result and hours_result[0] > 0:
            expected_hours = hours_result[0]
            actual_hours = hours_result[1]
            hour_coverage_pct = round((actual_hours / expected_hours) * 100, 2)
        else:
            hour_coverage_pct = None

        # Create gap statistics JSON
        gap_stats = {
            "missing_days_count": missing_days,
            "first_day": first_date.strftime("%Y-%m-%d") if first_date else None,
            "last_day": last_date.strftime("%Y-%m-%d") if last_date else None,
            "expected_days": days_span,
            "trading_days": int(days_span * 5/7) if days_span > 0 else 0,
            "days_with_data": unique_days,
            "gap_summary": {
                "dates": [d.strftime("%Y-%m-%d") for d in missing_dates][:10] if missing_dates else [],
                "has_more": len(missing_dates) > 10 if missing_dates else False
            }
        }

        # Update the stock_indexes table
        cursor.execute("""
            UPDATE stock_indexes
            SET record_count = %s,
                data_start_date = %s,
                data_end_date = %s,
                day_coverage_pct = %s,
                hour_coverage_pct = %s,
                missing_days = %s,
                gap_stats = %s
            WHERE symbol = %s;
        """, (
            record_count,
            first_date,
            last_date,
            day_coverage_pct,
            hour_coverage_pct,
            missing_days,
            json.dumps(gap_stats),
            symbol
        ))

        conn.commit()
        logger.info(f"Updated statistics for {symbol}")
        return True