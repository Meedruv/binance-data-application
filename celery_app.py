from celery import Celery
from celery.schedules import crontab



# Minimal Celery app for the project. Adjust broker/backend URLs as needed.
celery_app = Celery(
    'binance',
    broker='redis://127.0.0.1:6379/0',
    backend='redis://127.0.0.1:6379/1'
)

celery_app.conf.update(
    timezone='Asia/Singapore',
    beat_schedule={
    # Runs every 1 minute
    "fetch-binance-every-1-minute": {
        "task": "tasks_binance.collect_binance",
        "schedule": 60,  # seconds
    }
    }
)
