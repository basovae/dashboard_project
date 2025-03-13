import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
import argparse
from config import DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('index_analyzer')

def analyze_index_data(symbol):
    """
    Analyze existing data for a specific symbol and provide detailed information.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            # Query basic stats
            stats_query = """
            SELECT 
                COUNT(*) as record_count,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record,
                COUNT(DISTINCT DATE(timestamp)) as unique_days
            FROM stock_prices
            WHERE symbol = %s
            """
            
            # Get hourly coverage statistics
            hourly_coverage_query = """
            WITH expected_hours AS (
                SELECT generate_series(
                    DATE_TRUNC('day', MIN(timestamp)),
                    DATE_TRUNC('day', MAX(timestamp)) + INTERVAL '1 day' - INTERVAL '1 second',
                    INTERVAL '1 hour'
                ) AS hour_slot
                FROM stock_prices
                WHERE symbol = %s
            ),
            actual_hours AS (
                SELECT DATE_TRUNC('hour', timestamp) AS hour_slot
                FROM stock_prices
                WHERE symbol = %s
                GROUP BY hour_slot
            )
            SELECT
                COUNT(e.hour_slot) AS expected_hour_count,
                COUNT(a.hour_slot) AS actual_hour_count,
                ROUND(COUNT(a.hour_slot)::numeric / COUNT(e.hour_slot)::numeric * 100, 2) AS coverage_percent
            FROM expected_hours e
            LEFT JOIN actual_hours a USING (hour_slot)
            WHERE EXTRACT(HOUR FROM e.hour_slot) BETWEEN 9 AND 16;  -- Market hours only (adjust as needed)
            """
            
            # Check for missing days
            missing_days_query = """
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
            """
            
            # Get data quality metrics
            quality_query = """
            SELECT 
                COUNT(*) FILTER (WHERE open IS NULL) AS null_open,
                COUNT(*) FILTER (WHERE high IS NULL) AS null_high,
                COUNT(*) FILTER (WHERE low IS NULL) AS null_low,
                COUNT(*) FILTER (WHERE close IS NULL) AS null_close,
                COUNT(*) FILTER (WHERE volume IS NULL OR volume = 0) AS null_volume,
                COUNT(*) AS total_records
            FROM stock_prices
            WHERE symbol = %s
            """
            
            # Execute queries
            df_stats = pd.read_sql(stats_query, conn, params=(symbol,))
            df_hourly = pd.read_sql(hourly_coverage_query, conn, params=(symbol, symbol))
            df_missing = pd.read_sql(missing_days_query, conn, params=(symbol,))
            df_quality = pd.read_sql(quality_query, conn, params=(symbol,))
            
            # Prepare the analysis report
            if df_stats.empty or df_stats.iloc[0]['record_count'] == 0:
                print(f"\n===== ANALYSIS FOR {symbol} =====")
                print("No data found for this symbol.")
                return
            
            # Basic stats
            stats = df_stats.iloc[0]
            first_date = stats['first_record']
            last_date = stats['last_record']
            days_span = (last_date - first_date).days + 1
            expected_days = days_span
            actual_days = stats['unique_days']
            day_coverage = round((actual_days / expected_days) * 100, 2) if expected_days > 0 else 0
            
            # Hourly coverage
            hourly = df_hourly.iloc[0] if not df_hourly.empty else {'coverage_percent': 0}
            
            # Quality metrics
            quality = df_quality.iloc[0]
            total = quality['total_records']
            null_percentages = {
                'open': round((quality['null_open'] / total) * 100, 2) if total > 0 else 0,
                'high': round((quality['null_high'] / total) * 100, 2) if total > 0 else 0,
                'low': round((quality['null_low'] / total) * 100, 2) if total > 0 else 0,
                'close': round((quality['null_close'] / total) * 100, 2) if total > 0 else 0,
                'volume': round((quality['null_volume'] / total) * 100, 2) if total > 0 else 0
            }
            
            # Print the report
            print(f"\n===== ANALYSIS FOR {symbol} =====")
            print(f"Total records: {stats['record_count']}")
            print(f"Date range: {first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')} ({days_span} days)")
            print(f"Day coverage: {actual_days}/{expected_days} days ({day_coverage}%)")
            print(f"Hourly coverage: {hourly['coverage_percent']}% of expected market hours")
            
            if not df_missing.empty:
                missing_count = len(df_missing)
                print(f"\nMissing days: {missing_count} days")
                if missing_count <= 10:
                    for date in df_missing['day_date']:
                        print(f"  - {date.strftime('%Y-%m-%d')}")
                else:
                    print(f"  First 10 missing days (of {missing_count}):")
                    for date in df_missing['day_date'][:10]:
                        print(f"  - {date.strftime('%Y-%m-%d')}")
            
            print("\nData quality:")
            for field, pct in null_percentages.items():
                status = "⚠️ WARNING" if pct > 5 else "✅ GOOD"
                print(f"  {field}: {pct}% null values - {status}")
            
            # Overall assessment
            overall_quality = "GOOD" if day_coverage > 95 and hourly['coverage_percent'] > 90 else "NEEDS ATTENTION"
            print(f"\nOverall data quality: {overall_quality}")
            
            # Add recommendations
            if day_coverage < 95 or hourly['coverage_percent'] < 90:
                print("\nRecommendations:")
                print("  - Run smart backfill to fill in missing data periods")
                if day_coverage < 95:
                    print("  - Investigate missing days (weekends and holidays are expected to be missing)")
                if any(pct > 5 for pct in null_percentages.values()):
                    print("  - Check data source quality for null values")
            
            return {
                'symbol': symbol,
                'record_count': stats['record_count'],
                'first_date': first_date,
                'last_date': last_date,
                'day_coverage': day_coverage,
                'hourly_coverage': hourly['coverage_percent'],
                'missing_days': len(df_missing),
                'quality': null_percentages
            }
            
    except Exception as e:
        logger.error(f"Error analyzing data for {symbol}: {e}")
        print(f"\n===== ANALYSIS FOR {symbol} =====")
        print(f"Error analyzing data: {e}")
        return None

def analyze_all_indexes():
    """
    Analyze all stock symbols in the database.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT symbol FROM stock_prices ORDER BY symbol;")
                symbols = [row[0] for row in cursor.fetchall()]
                
        if not symbols:
            print("No stock symbols found in the database.")
            return
        
        print(f"Found {len(symbols)} symbols in the database. Analyzing each symbol...")
        
        results = []
        for symbol in symbols:
            result = analyze_index_data(symbol)
            if result:
                results.append(result)
            
        # Print summary table
        print("\n===== SUMMARY OF ALL INDEXES =====")
        print(f"{'Symbol':<10} {'Records':<10} {'Date Range':<25} {'Day Coverage':<15} {'Hour Coverage':<15}")
        print("-" * 80)
        
        for r in results:
            date_range = f"{r['first_date'].strftime('%Y-%m-%d')} to {r['last_date'].strftime('%Y-%m-%d')}"
            print(f"{r['symbol']:<10} {r['record_count']:<10} {date_range:<25} {r['day_coverage']}%{' ':>5} {r['hourly_coverage']}%{' ':>5}")
            
    except Exception as e:
        logger.error(f"Error in analyze_all_indexes: {e}")
        print(f"Error analyzing indexes: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze stock index data quality')
    parser.add_argument('--symbol', '-s', help='Specific stock symbol to analyze')
    parser.add_argument('--all', '-a', action='store_true', help='Analyze all symbols')
    
    args = parser.parse_args()
    
    if args.symbol:
        analyze_index_data(args.symbol)
    elif args.all:
        analyze_all_indexes()
    else:
        print("Please specify either a symbol with --symbol or use --all to analyze all symbols")
        print("Example: python index_analyzer.py --symbol AAPL")
        print("Example: python index_analyzer.py --all")