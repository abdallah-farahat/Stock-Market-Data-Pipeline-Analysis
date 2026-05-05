"""
etl.py  –  Extract / Transform / Load for Stock Market Pipeline
Each function is designed to be called independently by Airflow tasks.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf

from audit import DB_CONFIG, DataAuditor, RunLogger, get_conn

logger = logging.getLogger(__name__)

TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM", "JNJ", "V"]


# ─────────────────────────────────────────────────────────────
#  EXTRACT
# ─────────────────────────────────────────────────────────────
def get_last_loaded_date(ticker: str) -> Optional[date]:
    """Return the most recent date already in fact_stock_prices for a ticker."""
    sql = "SELECT MAX(date) FROM fact_stock_prices WHERE ticker = %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ticker,))
            result = cur.fetchone()[0]
    return result


def is_trading_day(d: date) -> bool:
    """Return False for weekends. Does not account for public holidays."""
    return d.weekday() < 5  # Mon=0 … Fri=4


def extract_stock_data(ticker: str, dag_run_id: str = "") -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Yahoo Finance.

    Incremental logic:
      - If data already exists for this ticker, fetch only records after
        the last loaded date (watermark per ticker, not global).
      - On first run, perform a full historical load from 2020-01-01.

    Weekend / holiday handling:
      - yfinance only returns trading days (Mon–Fri, excluding market
        holidays). Gaps in the date sequence due to weekends or public
        holidays are expected and are NOT treated as missing data errors.
      - An empty result is only flagged as a warning when the requested
        start date is a weekday, indicating genuinely missing data rather
        than a normal non-trading-day gap.
    """
    run = RunLogger(task_name="extract", dag_run_id=dag_run_id, ticker=ticker)
    with run:
        last_date = get_last_loaded_date(ticker)

        if last_date:
            start = last_date + timedelta(days=1)
            logger.info("[EXTRACT] %s — incremental from %s", ticker, start)
        else:
            start = date(2020, 1, 1)
            logger.info("[EXTRACT] %s — full load from %s", ticker, start)

        df = yf.download(ticker, start=start, end=date.today(),
                         auto_adjust=True, progress=False)

        if df.empty:
            # Distinguish between "no new trading days" (expected on weekends /
            # holidays) and "missing data on a weekday" (potential data issue).
            if is_trading_day(start) and start < date.today():
                logger.warning(
                    "[EXTRACT] %s — no data returned for a weekday range "
                    "starting %s. Yahoo Finance may be rate-limiting or the "
                    "market was closed (public holiday).", ticker, start
                )
            else:
                logger.info(
                    "[EXTRACT] %s — no new data (start=%s is a weekend or "
                    "today — expected, not an error).", ticker, start
                )
            run.records_found = 0
            run.details = {"start": str(start), "rows_fetched": 0,
                           "reason": "empty_response"}
            return pd.DataFrame()

        df = df.reset_index()
        df.columns = [c.lower() if isinstance(c, str) else c[0].lower()
                      for c in df.columns]
        df["ticker"] = ticker
        df["date"]   = pd.to_datetime(df["date"]).dt.date

        run.records_found = len(df)
        run.details = {"start": str(start), "rows_fetched": len(df)}
        logger.info("[EXTRACT] %s — fetched %d rows", ticker, len(df))

    return df


# ─────────────────────────────────────────────────────────────
#  TRANSFORM
# ─────────────────────────────────────────────────────────────
def transform_stock_data(df: pd.DataFrame, ticker: str,
                         dag_run_id: str = "", run_id: int = None) -> pd.DataFrame:
    """
    Clean + enrich a raw OHLCV DataFrame.
    Runs DataAuditor checks and calculates derived columns.
    """
    if df.empty:
        return df

    run = RunLogger(task_name="transform", dag_run_id=dag_run_id, ticker=ticker)
    with run:
        # ── Audit / quality checks ──────────────────────────
        auditor = DataAuditor(run_id=run.run_id, ticker=ticker)
        df = auditor.run_all(df)

        summary = auditor.summary()
        run.records_invalid = summary["errors"]
        run.details = summary

        if df.empty:
            logger.warning("[TRANSFORM] %s — all rows rejected by audit", ticker)
            return df

        # ── Sort chronologically ─────────────────────────────
        df = df.sort_values("date").reset_index(drop=True)

        # ── Daily Return ─────────────────────────────────────
        df["daily_return"] = df["close"].pct_change().round(6)

        # ── 7-day Moving Average ─────────────────────────────
        df["moving_avg_7"] = (
            df["close"].rolling(window=7, min_periods=1).mean().round(4)
        )

        # ── 7-day Volatility (std of daily returns) ──────────
        df["volatility_7"] = (
            df["daily_return"].rolling(window=7, min_periods=2).std().round(6)
        )

        # ── Re-run return consistency check after calculation ─
        auditor.check_daily_return_consistency(df)

        run.records_found    = len(df)
        run.records_invalid  = summary["errors"]
        logger.info("[TRANSFORM] %s — %d clean rows after transformations", ticker, len(df))

    return df


# ─────────────────────────────────────────────────────────────
#  LOAD
# ─────────────────────────────────────────────────────────────
def load_to_database(df: pd.DataFrame, ticker: str,
                     dag_run_id: str = "") -> dict:
    """
    Upsert transformed rows into fact_stock_prices.
    Uses INSERT … ON CONFLICT DO NOTHING for idempotency.
    """
    if df.empty:
        logger.info("[LOAD] %s — nothing to load", ticker)
        return {"inserted": 0, "skipped": 0}

    run = RunLogger(task_name="load", dag_run_id=dag_run_id, ticker=ticker)
    with run:
        columns = ["date", "ticker", "open", "high", "low", "close",
                   "volume", "daily_return", "moving_avg_7", "volatility_7"]

        # Keep only columns that exist in df
        cols = [c for c in columns if c in df.columns]
        records = df[cols].where(pd.notnull(df[cols]), None).to_dict("records")

        insert_sql = f"""
            INSERT INTO fact_stock_prices ({", ".join(cols)})
            VALUES ({", ".join(["%(" + c + ")s" for c in cols])})
            ON CONFLICT (date, ticker) DO NOTHING
        """

        inserted = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                for rec in records:
                    cur.execute(insert_sql, rec)
                    inserted += cur.rowcount

        skipped = len(records) - inserted
        run.records_found    = len(records)
        run.records_inserted = inserted
        run.records_skipped  = skipped
        run.details = {"ticker": ticker, "attempted": len(records)}

        logger.info("[LOAD] %s — inserted=%d  skipped(dup)=%d",
                    ticker, inserted, skipped)

    return {"inserted": inserted, "skipped": skipped}


# ─────────────────────────────────────────────────────────────
#  Convenience: run full pipeline for one ticker
# ─────────────────────────────────────────────────────────────
def run_pipeline_for_ticker(ticker: str, dag_run_id: str = "") -> dict:
    raw_df    = extract_stock_data(ticker, dag_run_id)
    clean_df  = transform_stock_data(raw_df, ticker, dag_run_id)
    result    = load_to_database(clean_df, ticker, dag_run_id)
    return result
