# Stock Dashboard Project

A comprehensive system for tracking, fetching, processing, and visualizing stock data with intelligent backfilling.

## Features

- **Smart Backfilling**: Only fetches missing data periods instead of reloading everything
- **Data Quality Tracking**: Advanced analytics on data completeness and gaps
- **Stock Index Management**: System to track which stock indexes need attention
- **Data Processing**: Calculate moving averages and generate buy/sell signals
- **Web Dashboard**: Interactive visualization with Plotly charts
- **Automated Jobs**: Airflow-based scheduling for data updates

## Components

### 1. Index Management System

The system includes a sophisticated database table (`stock_indexes`) to track:

- Stock symbols with their backfill status and timestamps
- Data quality metrics for each stock (coverage percentage, gaps)
- Detailed statistics on data completeness

### 2. Smart Data Processing

- **index_manager.py**: Core component for managing stock indexes and data quality
- **index_analyzer.py**: Tool to analyze and visualize data quality for each stock
- **smart_backfill.py**: Intelligently identifies and fills only missing data periods
- **update_stock_data.py**: Efficiently updates only the latest data

### 3. Analytics and Reporting

- Data quality metrics for each stock:
  - Day coverage percentage
  - Hour coverage percentage
  - Missing days identification
  - Gap statistics

### 4. Airflow Pipeline

The updated pipeline includes:

1. Add/update default indexes
2. Smart backfill for indexes with missing data
3. Update latest data for existing indexes

## Database Schema

### Stock Indexes Table (Enhanced)

```sql
CREATE TABLE stock_indexes (
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

    -- Data quality metrics
    data_start_date TIMESTAMP,
    data_end_date TIMESTAMP,
    record_count INTEGER,
    day_coverage_pct NUMERIC,
    hour_coverage_pct NUMERIC,
    missing_days INTEGER,
    gap_stats JSONB,
    notes TEXT
);
```

## Using the System

### Analyzing Data Quality

```bash
# Check data quality for a specific symbol
docker exec stock_project-stock-fetcher python index_analyzer.py --symbol AAPL

# Get data quality report for all stocks
docker exec stock_project-stock-fetcher python index_analyzer.py --all
```

### Adding New Stock Symbols

```bash
# Add a single new symbol
docker exec stock_project-stock-fetcher python -c "from index_manager import add_index; add_index('TSLA', 'Tesla Inc.')"

# Add through environment variables
# Edit .env file and update DEFAULT_SYMBOLS
```

### Running Smart Backfill Manually

```bash
# Backfill all pending symbols
docker exec stock_project-stock-fetcher python smart_backfill.py

# Backfill a specific symbol
docker exec stock_project-stock-fetcher python -c "from smart_backfill import backfill_symbol; backfill_symbol('AAPL')"
```

## Technical Notes

1. Smart backfilling handles:

   - Completely missing symbols (full backfill)
   - Gaps within existing data (partial backfill)
   - Missing recent data (update from latest record)
   - Missing historical data (backfill early periods)

2. Data quality tracking accounts for:

   - Trading days vs. calendar days
   - Market hours for hourly data
   - Expected vs. actual data points

3. Error handling improvements:
   - Detailed logging
   - Proper error states
   - Recovery from partial failures

## Setup and Usage

1. Copy `.env.template` to `.env` and customize values
2. Build the Docker container: `docker-compose build`
3. Start services: `docker-compose up -d`
4. Access dashboard at `http://localhost:5050`
5. Access Airflow at `http://localhost:8080`
