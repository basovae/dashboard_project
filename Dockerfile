# Use Python image
FROM python:3.8

# Set working directory
WORKDIR /app

# Copy all required files into the container
COPY requirements.txt .
COPY backfill_stock_data.py .  
COPY update_stock_data.py .  

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the dashboard script and templates
COPY dashboard.py .  
COPY templates/index.html .  


# Default command (can be overridden)
CMD ["python", "fetch_stock_data.py"]
