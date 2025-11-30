import json
import re
import csv
from pathlib import Path
from typing import List, Tuple, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


class BinanceFetcher:
    """logics for querying from binance requests.

    Public methods:
    - fetch_datatypes()
    - get_instruments(data_type)
    - get_dates(data_type, instrument)
    - collect_datatype(data_type)
    - collect_all()
    - to_csv(rows, path)
    - save_cache()
    - new_data_available()
    """

    def __init__(
        self,
        base_url: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/",
        base_prefix: str = "data/futures/um/daily/",
        cache_path: str = "cache.json",
        max_workers: int = 15,
        timeout: int = 10,
    ):
        self.base_url = base_url
        self.base_prefix = base_prefix
        self.cache_path = Path(cache_path)
        self.max_workers = max_workers
        self.timeout = timeout

        self.session = requests.Session()
        # sensible retry policy for transient network issues
        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._load_cache()

        # simple status object used by web endpoints
        self.status: Dict[str, Any] = {
            "running": False,
            "progress": 0,
            "total": 0,
            "errors": 0,
        }

    def _load_cache(self) -> None:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}
        else:
            self.cache = {}

    def save_cache(self) -> None:
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _fetch(self, url: str) -> str:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def fetch_datatypes(self) -> List[str]:
        url = f"{self.base_url}?delimiter=/&prefix={self.base_prefix}"
        xml = self._fetch(url)
        return re.findall(rf"<Prefix>{re.escape(self.base_prefix)}([^/]+)/</Prefix>", xml)

    def get_instruments(self, data_type: str) -> List[str]:
        url = f"{self.base_url}?delimiter=/&prefix={self.base_prefix}{data_type}/"
        xml = self._fetch(url)
        return re.findall(rf"<Prefix>{re.escape(self.base_prefix)}{data_type}/([^/]+)/</Prefix>", xml)

    def get_dates(self, data_type: str, instrument: str) -> Tuple[str, str]:
        cache_key = f"{data_type}-{instrument}"
        if cache_key in self.cache:
            val = self.cache[cache_key]
            return (val[0], val[1])

        url = f"{self.base_url}?prefix={self.base_prefix}{data_type}/{instrument}/"
        xml = self._fetch(url)
        timestamps = re.findall(r"<LastModified>(.*?)</LastModified>", xml)
        dates = sorted([ts.split("T")[0] for ts in timestamps])
        if dates:
            result = (dates[0], dates[-1])
        else:
            result = ("", "")

        self.cache[cache_key] = result
        return result

    def collect_datatype(self, data_type: str) -> List[List[str]]:
        instruments = self.get_instruments(data_type)
        rows: List[List[str]] = []
        self.status.update({"running": True, "progress": 0, "total": len(instruments)})

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.get_dates, data_type, inst): inst for inst in instruments}
            for future in as_completed(futures):
                inst = futures[future]
                try:
                    from_date, to_date = future.result()
                except Exception:
                    from_date, to_date = "", ""
                    self.status["errors"] = self.status.get("errors", 0) + 1
                rows.append([data_type, inst, from_date, to_date])
                self.status["progress"] += 1

        self.save_cache()
        self.status["running"] = False
        return rows

    def collect_all(self) -> List[List[str]]:
        datatypes = self.fetch_datatypes()
        all_rows: List[List[str]] = []
        for dt in datatypes:
            all_rows.extend(self.collect_datatype(dt))
        all_rows.sort(key=lambda x: (x[0], x[1], x[2]))
        return all_rows

    def to_csv(self, rows: List[List[str]], path: str = "binance_instruments.csv") -> None:
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["datatype", "instrument", "from_date", "to_date"])
                writer.writerows(rows)
        except Exception:
            pass

    def new_data_available(self):

        url = f"{self.base_url}?delimiter=/&prefix={self.base_prefix}aggTrades/BTCUSDT/"
        xml = self._fetch(url)
        #latest = xml.split("<LastModified>")[-1].split("</LastModified>")[0].split("T")[0]
        print(xml)
        timestamps = re.findall(r"<LastModified>(.*?)</LastModified>", xml)
        dates = sorted([ts.split("T")[0] for ts in timestamps])
        latest = dates[0]

        print(f"Latest date on server for BTCUSDT: {latest}")
        if not os.path.exists('binance_instruments.csv'):
            return True
        
        with open('binance_instruments.csv', "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['instrument'] == 'BTCUSDT':
                    last_date = row['to_date']
                    print(f"Last date in local CSV for BTCUSDT: {last_date}")
                    if latest != last_date:
                        return True
                    else:
                        return False   
                    
    
