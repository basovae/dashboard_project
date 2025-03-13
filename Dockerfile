# Use Python image
FROM python:3.8

# Set working directory
WORKDIR /app

# Copy configuration files first
COPY config.py .
COPY .env .
COPY index_manager.py .

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all data processing scripts
COPY backfill_stock_data.py .  
COPY update_stock_data.py .
COPY add_default_indexes.py .  

# Copy dashboard files
COPY dashboard.py .  
COPY templates/index.html templates/

# Default command (keeps container running)
CMD ["tail", "-f", "/dev/null"]