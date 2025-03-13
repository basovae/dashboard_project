import pandas as pd
import psycopg2
from flask import Flask, render_template
import plotly.graph_objs as go
from datetime import datetime, timedelta

app = Flask(__name__)

DB_PARAMS = {
    'dbname': 'stocks_db',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',  # ✅ Connects to PostgreSQL in Docker
    'port': 5432
}

def fetch_stock_data():
    """Fetch latest stock data from PostgreSQL"""
    with psycopg2.connect(**DB_PARAMS) as conn:
        query = """
            SELECT symbol, timestamp, close
            FROM stock_prices
            WHERE timestamp > NOW() - INTERVAL '7 days' 
            ORDER BY timestamp ASC;
        """
        df = pd.read_sql(query, conn)
    return df

def calculate_trend(df):
    """Calculate trend and buy/sell signals using moving averages."""
    df['SMA_10'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(10).mean())  # 10-period SMA
    df['SMA_30'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(30).mean())  # 30-period SMA
    df['Signal'] = df.apply(lambda row: 'BUY' if row['SMA_10'] > row['SMA_30'] else 'SELL', axis=1)
    return df

@app.route('/')
def index():
    df = fetch_stock_data()
    if df.empty:
        return render_template('index.html', graph_html="<h3>No data available</h3>")

    df = calculate_trend(df)
    
    # Generate Trend Plot
    graphs = []
    for symbol in df['symbol'].unique():
        symbol_df = df[df['symbol'] == symbol]
        trace1 = go.Scatter(x=symbol_df['timestamp'], y=symbol_df['close'], mode='lines', name=f'{symbol} Close Price')
        trace2 = go.Scatter(x=symbol_df['timestamp'], y=symbol_df['SMA_10'], mode='lines', name=f'{symbol} 10-period SMA')
        trace3 = go.Scatter(x=symbol_df['timestamp'], y=symbol_df['SMA_30'], mode='lines', name=f'{symbol} 30-period SMA')

        graphs.append(go.Figure([trace1, trace2, trace3]))

    return render_template('index.html', graph_html=graphs, df=df)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
