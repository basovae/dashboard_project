from flask import Flask, render_template
import pandas as pd
import psycopg2
import plotly.express as px

app = Flask(__name__)

# Database connection details
DB_PARAMS = {
    "host": "postgres",
    "database": "stocks_db",
    "user": "myuser",
    "password": "mypassword",
    "port": "5432",
}

def get_stock_data():
    """Fetch stock data from PostgreSQL."""
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT symbol, timestamp, close FROM stock_prices
        ORDER BY timestamp DESC LIMIT 100
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@app.route("/")
def home():
    df = get_stock_data()
    
    # Create a Plotly line chart
    fig = px.line(df, x="timestamp", y="close", color="symbol", title="Stock Price Trends")
    graph_html = fig.to_html(full_html=False)

    return render_template("index.html", graph_html=graph_html)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
