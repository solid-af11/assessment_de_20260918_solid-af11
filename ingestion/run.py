"""
Single entrypoint for loading one logical date across all configured cities.

Used by both the Airflow DAG task and the notebook, so the logic lives
in exactly one place.
"""
import logging
from datetime import date, timedelta
from pathlib import Path

import yaml

from ingestion.extract import fetch_city_range
from ingestion.load import load_records_for_date

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "cities.yml"


def load_cities() -> list[dict]:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config["cities"]


def run_for_date(target_date: date) -> dict[str, int]:
    """
    Extract + load weather for every configured city, for a single logical date.
    Returns a dict of {city: row_count} for visibility/logging.
    """
    cities = load_cities()
    results = {}

    for city in cities:
        records = fetch_city_range(
            city_name=city["name"],
            latitude=city["latitude"],
            longitude=city["longitude"],
            timezone=city["timezone"],
            start_date=target_date,
            end_date=target_date,
        )
        count = load_records_for_date(city["name"], target_date, records)
        results[city["name"]] = count

    return results


def run_for_date_range(start_date: date, end_date: date) -> dict[str, dict[str, int]]:
    """
    Backfill helper: runs run_for_date() for every date in [start_date, end_date] inclusive.
    """
    results = {}
    current = start_date
    while current <= end_date:
        results[current.isoformat()] = run_for_date(current)
        current += timedelta(days=1)
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python -m ingestion.run YYYY-MM-DD")
        sys.exit(1)

    target = date.fromisoformat(sys.argv[1])
    logging.basicConfig(level=logging.INFO)
    result = run_for_date(target)
    print(result)
