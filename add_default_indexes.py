#!/usr/bin/env python3
"""Script to add default stock indexes to the tracking system."""

import logging
from config import DEFAULT_SYMBOLS, DEFAULT_UPDATE_FREQUENCY
from index_manager import ensure_tables_exist, add_index

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('index_manager')

# Stock display names (for readability in the dashboard)
STOCK_DISPLAY_NAMES = {
    'AAPL': 'Apple Inc.',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc. (Google)',
    'NVDA': 'NVIDIA Corporation',
    'AMZN': 'Amazon.com Inc.',
    'META': 'Meta Platforms Inc.',
    'TSLA': 'Tesla Inc.',
    'NFLX': 'Netflix Inc.',
    'AMD': 'Advanced Micro Devices Inc.',
    'INTC': 'Intel Corporation',
    'IBM': 'International Business Machines',
    'ORCL': 'Oracle Corporation',
    'CSCO': 'Cisco Systems Inc.',
    'ADBE': 'Adobe Inc.',
    'CRM': 'Salesforce Inc.',
    'PYPL': 'PayPal Holdings Inc.',
    'SPY': 'SPDR S&P 500 ETF Trust',
    'QQQ': 'Invesco QQQ Trust (NASDAQ-100 Index)',
    'DIA': 'SPDR Dow Jones Industrial Average ETF',
    'IWM': 'iShares Russell 2000 ETF'
}

def add_default_indexes():
    """Add default stock indexes to the tracking system."""
    # Ensure tables exist
    ensure_tables_exist()
    
    logger.info(f"Adding default indexes: {DEFAULT_SYMBOLS}")
    
    success_count = 0
    for symbol in DEFAULT_SYMBOLS:
        # Get display name if available, otherwise use symbol
        display_name = STOCK_DISPLAY_NAMES.get(symbol, symbol)
        
        if add_index(symbol, display_name, DEFAULT_UPDATE_FREQUENCY):
            success_count += 1
    
    logger.info(f"Successfully added/updated {success_count} of {len(DEFAULT_SYMBOLS)} indexes")
    return success_count == len(DEFAULT_SYMBOLS)

if __name__ == "__main__":
    add_default_indexes()