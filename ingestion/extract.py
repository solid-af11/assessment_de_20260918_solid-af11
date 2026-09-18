"""
Extract daily weather data from the Open-Meteo historical archive API.

No API key required. Docs: https://open-meteo.com/en/docs/historical-weather-api
"""
import logging
from datetime import date
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
]

REQUEST_TIMEOUT_SECONDS = 15


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_city_range(
    city_name: str,
    latitude: float,
    longitude: float,
    timezone: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily weather for one city over an inclusive date range.

    Returns a list of raw records (one per day), with API fields left
    unmodified plus the city name attached for downstream loading.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ",".join(DAILY_FIELDS),
        "timezone": timezone,
    }

    logger.info(
        "Fetching weather for %s from %s to %s",
        city_name,
        start_date,
        end_date,
    )
    response = requests.get(
        ARCHIVE_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    payload = response.json()

    daily = payload.get("daily", {})
    dates = daily.get("time", [])

    records = []
    for i, day in enumerate(dates):
        record = {"city": city_name, "date": day}
        for field in DAILY_FIELDS:
            values = daily.get(field, [])
            record[field] = values[i] if i < len(values) else None
        records.append(record)

    return records
