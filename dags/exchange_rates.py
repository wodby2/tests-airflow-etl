"""Loads the European Central Bank's daily euro exchange rates into the warehouse.

Every day Airflow runs the DAG for the previous day: it downloads that day's
rates from the Frankfurter API, turns them into one row per currency, and
upserts the rows into PostgreSQL. A second step rebuilds the monthly averages.
"""

from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

CURRENCIES = ["USD", "GBP", "JPY", "CHF", "CNY"]


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    # Runs the days since start_date that have not run yet.
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["etl"],
)
def exchange_rates():
    @task
    def extract() -> dict:
        # The day this run covers: the run on November 13 loads November 12.
        day = get_current_context()["data_interval_start"].to_date_string()
        response = requests.get(
            f"https://api.frankfurter.dev/v1/{day}",
            params={"symbols": ",".join(CURRENCIES)},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        # The ECB publishes no rates on weekends and holidays; the API then
        # returns the last business day, which an earlier run already loaded.
        if payload["date"] != day:
            return {"date": day, "rates": {}}
        return payload

    @task
    def transform(payload: dict) -> list[tuple]:
        return [
            (payload["date"], currency, rate)
            for currency, rate in sorted(payload["rates"].items())
        ]

    @task
    def load(rows: list[tuple]) -> int:
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS exchange_rates (
                day date NOT NULL,
                currency char(3) NOT NULL,
                rate numeric(18, 6) NOT NULL,
                PRIMARY KEY (day, currency)
            )
            """
        )
        # Upserting makes reruns and backfills safe: a day loaded twice keeps one row per currency.
        hook.insert_rows(
            "exchange_rates",
            rows,
            target_fields=["day", "currency", "rate"],
            replace=True,
            replace_index=["day", "currency"],
        )
        return len(rows)

    @task
    def report(loaded: int) -> None:
        PostgresHook(postgres_conn_id="warehouse").run(
            """
            CREATE TABLE IF NOT EXISTS monthly_rates (
                month date NOT NULL,
                currency char(3) NOT NULL,
                average_rate numeric(18, 6) NOT NULL,
                days integer NOT NULL,
                PRIMARY KEY (month, currency)
            );
            INSERT INTO monthly_rates
            SELECT date_trunc('month', day)::date, currency, avg(rate), count(*)
            FROM exchange_rates
            GROUP BY 1, 2
            ON CONFLICT (month, currency) DO UPDATE
            SET average_rate = EXCLUDED.average_rate, days = EXCLUDED.days;
            """
        )

    report(load(transform(extract())))


exchange_rates()
