# 1️⃣ Use the official Python image as the base
FROM python:3.9

# 2️⃣ Set the working directory inside the container
WORKDIR /app

# 3️⃣ Copy project files into the container
COPY . .

# 4️⃣ Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 5️⃣ Set the default command to run the stock data fetching script
CMD ["python", "fetch_stock_data.py"]
