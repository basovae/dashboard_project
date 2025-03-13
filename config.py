import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'stocks_db'),
    'user': os.getenv('DB_USER', 'myuser'),
    'password': os.getenv('DB_PASSWORD', 'mypassword'),
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Default stock symbols to track
DEFAULT_SYMBOLS = os.getenv('DEFAULT_SYMBOLS', 'AAPL,MSFT,GOOGL,NVDA').split(',')

# Data fetch configuration
BACKFILL_START_DATE = os.getenv('BACKFILL_START_DATE', '2019-01-01')
MAX_BACKFILL_DAYS = int(os.getenv('MAX_BACKFILL_DAYS', '730'))

# Update frequency options
UPDATE_FREQUENCIES = ['HOURLY', 'DAILY']
DEFAULT_UPDATE_FREQUENCY = os.getenv('DEFAULT_UPDATE_FREQUENCY', 'HOURLY')

# Application configuration
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'