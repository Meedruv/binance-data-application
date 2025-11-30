from typing import List
from celery_app import celery_app
from application.binance_client import BinanceFetcher


@celery_app.task(bind=True, name="tasks_binance.collect_binance")
def collect_binance(self) -> dict:
    """Celery task: collect all datatypes/instruments and write CSV.

    The task updates its state with progress meta: {'progress': n, 'total': total}
    """
    fetcher = BinanceFetcher()

    # fetch datatypes
    datatypes = fetcher.fetch_datatypes()

    # compute total instruments to process (may perform network calls)
    total = 0
    instruments_by_dt = {}
    for dt in datatypes:
        try:
            insts = fetcher.get_instruments(dt)
        except Exception:
            insts = []
        instruments_by_dt[dt] = insts
        total += len(insts)

    progress = 0
    all_rows: List[List[str]] = []

    for dt, insts in instruments_by_dt.items():
        # process instruments for this datatype
        for inst in insts:
            try:
                from_date, to_date = fetcher.get_dates(dt, inst)
            except Exception:
                from_date, to_date = "", ""
            all_rows.append([dt, inst, from_date, to_date])
            progress += 1
            # update task state
            self.update_state(state='PROGRESS', meta={'progress': progress, 'total': total})

    # sort and save
    all_rows.sort(key=lambda x: (x[0], x[1], x[2]))
    fetcher.to_csv(all_rows)
    fetcher.save_cache()

    return {'status': 'completed', 'rows': len(all_rows)}

@celery_app.task(bind=True, name="tasks_binance.check_and_run")
def check_and_run(self):
    print("Checking for Binance updates...")
    fetcher = BinanceFetcher()

    if fetcher.new_data_available():
        print("✔ New data found! Running crawler.")
        collect_binance.delay()
    else:
        print("No new data.")
