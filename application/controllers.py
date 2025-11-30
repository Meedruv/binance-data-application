from flask import current_app as app
from flask import render_template
from flask import jsonify, request
from threading import Thread

from application.binance_client import BinanceFetcher

# module-level fetcher instance reused by endpoints; keeps cache/status in memory
fetcher = BinanceFetcher()

# try to import Celery task; if unavailable we'll fall back to the background thread
try:
    from tasks_binance import collect_binance
    from celery_app import celery_app
    _CELERY_AVAILABLE = True
except Exception:
    collect_binance = None
    celery_app = None
    _CELERY_AVAILABLE = False


@app.route("/")
def homepage():
    return render_template("binance_extract_home.html")


@app.route('/api/collect', methods=['POST'])
def start_collect():
    # prefer Celery-backed job if available
    if _CELERY_AVAILABLE and collect_binance is not None:
        # enqueue task
        async_result = collect_binance.delay()
        return jsonify({'status': 'started', 'task_id': async_result.id}), 202

    # fallback: start background collection if not already running
    if fetcher.status.get('running'):
        return jsonify({'error': 'Collection already running'}), 409

    def run_and_save():
        rows = fetcher.collect_all()
        fetcher.to_csv(rows)

    thread = Thread(target=run_and_save, daemon=True)
    thread.start()
    return jsonify({'status': 'started', 'mode': 'thread'}), 202


@app.route('/api/collect/status')
def collect_status():
    # if a task_id query param is supplied and Celery is available, return task state
    task_id = request.args.get('task_id')
    if task_id and _CELERY_AVAILABLE and celery_app is not None:
        res = celery_app.AsyncResult(task_id)
        info = {'state': res.state}
        try:
            info['meta'] = res.info or {}
        except Exception:
            info['meta'] = {}
        return jsonify(info)

    # otherwise, return in-memory fetcher status (thread fallback)
    return jsonify(fetcher.status)