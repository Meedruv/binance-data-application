📊 Binance Data Application

A lightweight Python application that fetches real-time Binance Futures market data every minute, stores it in CSV, and exposes a simple Flask UI.
A Celery scheduler + Redis is used to automate minute-level data collection.

🚀 Features

⏱ Real-time Data Collection
Automatically fetches updated Binance futures data every 1 minute.

📁 CSV Export
Stores instrument metadata and price data into CSV files.

🧩 Modular Binance Fetcher
Clean class-based implementation for all Binance API fetching logic.

🟦 Flask Web Interface
View the collected instruments from a simple frontend page.

🧵 Asynchronous Background Tasks (Celery)
Uses Celery Beat + Worker for scheduling and task execution.

🟥 Redis Queue
Redis is used as the message broker for Celery.
