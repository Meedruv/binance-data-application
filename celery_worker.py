from celery_app import celery_app
import tasks_binance  # ensure tasks are registered

if __name__ == "__main__":
    celery_app.start()