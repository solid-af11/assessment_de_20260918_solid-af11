"""
Load extracted weather records into raw.weather_daily (Postgres).

Re-run safety: delete existing rows for (city, date) before inserting,
so re-running the same logical date never duplicates rows.
"""
import logging
import os
from datetime import date, datetime, timezone as dt_timezone
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "weather_daily"


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "postgres"),
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
        user=os.environ.get("WAREHOUSE_USER", "de"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "de"),
    )


def ensure_raw_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
                city TEXT NOT NULL,
                date DATE NOT NULL,
                temperature_2m_max DOUBLE PRECISION,
                temperature_2m_min DOUBLE PRECISION,
                temperature_2m_mean DOUBLE PRECISION,
                precipitation_sum DOUBLE PRECISION,
                windspeed_10m_max DOUBLE PRECISION,
                loaded_at TIMESTAMPTZ NOT NULL
            );
            """
        )
    conn.commit()


def delete_existing(conn, city: str, target_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE city = %s AND date = %s;",
            (city, target_date),
        )
    conn.commit()


def insert_records(conn, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0

    loaded_at = datetime.now(dt_timezone.utc)
    rows = [
        (
            r["city"],
            r["date"],
            r.get("temperature_2m_max"),
            r.get("temperature_2m_min"),
            r.get("temperature_2m_mean"),
            r.get("precipitation_sum"),
            r.get("windspeed_10m_max"),
            loaded_at,
        )
        for r in records
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO {RAW_SCHEMA}.{RAW_TABLE}
                (city, date, temperature_2m_max, temperature_2m_min,
                 temperature_2m_mean, precipitation_sum, windspeed_10m_max, loaded_at)
            VALUES %s
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def load_records_for_date(city: str, target_date: date, records: list[dict[str, Any]]) -> int:
    """
    Idempotently load one city's records for one logical date.
    Deletes any existing rows for (city, date) first, then inserts fresh ones.
    """
    conn = get_connection()
    try:
        ensure_raw_table(conn)
        delete_existing(conn, city, target_date)
        count = insert_records(conn, records)
        logger.info("Loaded %d row(s) for %s on %s", count, city, target_date)
        return count
    finally:
        conn.close()
