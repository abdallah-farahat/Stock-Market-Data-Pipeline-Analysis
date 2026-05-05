"""
audit.py  –  Data Quality Auditor for Stock Market Pipeline
Logs every pipeline run and quality check into PostgreSQL audit tables.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import os

import pandas as pd
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Config  — reads from STOCK_DB_* env vars set in docker-compose
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("STOCK_DB_HOST", "localhost"),
    "port":     int(os.environ.get("STOCK_DB_PORT", 5432)),
    "dbname":   os.environ.get("STOCK_DB_NAME", "stock_db"),
    "user":     os.environ.get("STOCK_DB_USER", "airflow"),
    "password": os.environ.get("STOCK_DB_PASSWORD", "airflow"),
}

TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM", "JNJ", "V"]


# ─────────────────────────────────────────────────────────────
#  Connection helper
# ─────────────────────────────────────────────────────────────
@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
#  Run Logger  – writes to audit_pipeline_log
# ─────────────────────────────────────────────────────────────
@dataclass
class RunLogger:
    task_name:        str
    dag_run_id:       str = ""
    ticker:           Optional[str] = None
    run_id:           Optional[int] = None
    records_found:    int = 0
    records_inserted: int = 0
    records_skipped:  int = 0
    records_invalid:  int = 0
    details:          dict = field(default_factory=dict)

    def start(self) -> "RunLogger":
        """Insert a RUNNING row and store the generated run_id."""
        sql = """
            INSERT INTO audit_pipeline_log
                (dag_run_id, task_name, ticker, status)
            VALUES (%s, %s, %s, 'RUNNING')
            RETURNING run_id
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (self.dag_run_id, self.task_name, self.ticker))
                self.run_id = cur.fetchone()[0]
        logger.info("[AUDIT] Started run_id=%s task=%s ticker=%s",
                    self.run_id, self.task_name, self.ticker)
        return self

    def finish(self, status: str = "SUCCESS", error: str = None):
        """Update the row with final counts + status."""
        if self.run_id is None:
            logger.warning("[AUDIT] finish() called before start()")
            return
        sql = """
            UPDATE audit_pipeline_log
            SET status            = %s,
                finished_at       = NOW(),
                records_found     = %s,
                records_inserted  = %s,
                records_skipped   = %s,
                records_invalid   = %s,
                error_message     = %s,
                details           = %s
            WHERE run_id = %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    status,
                    self.records_found,
                    self.records_inserted,
                    self.records_skipped,
                    self.records_invalid,
                    error,
                    Json(self.details),
                    self.run_id,
                ))
        logger.info("[AUDIT] Finished run_id=%s status=%s inserted=%d skipped=%d invalid=%d",
                    self.run_id, status,
                    self.records_inserted, self.records_skipped, self.records_invalid)

    # ── context manager usage ────────────────────────────────
    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.finish(status="FAILED", error=str(exc_val))
        else:
            self.finish(status="SUCCESS")
        return False   # do not suppress exceptions


# ─────────────────────────────────────────────────────────────
#  Quality Issue Logger  – writes to audit_quality_issues
# ─────────────────────────────────────────────────────────────
def log_issue(
    run_id:     int,
    check_name: str,
    severity:   str,
    description: str,
    ticker:     str  = None,
    issue_date: date = None,
    raw_value:  str  = None,
):
    sql = """
        INSERT INTO audit_quality_issues
            (run_id, ticker, issue_date, check_name, severity, description, raw_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id, ticker, issue_date, check_name,
                               severity, description, raw_value))
    logger.warning("[QUALITY] %s | %s | %s | %s", severity, check_name, ticker, description)


# ─────────────────────────────────────────────────────────────
#  Core Audit Checks
# ─────────────────────────────────────────────────────────────
class DataAuditor:
    """
    Run a battery of quality checks on a DataFrame of stock prices.

    Usage:
        auditor = DataAuditor(run_id=42, ticker="AAPL")
        clean_df = auditor.run_all(df)
        print(auditor.summary())
    """

    REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
    NUMERIC_COLUMNS  = ["open", "high", "low", "close", "volume"]

    def __init__(self, run_id: int, ticker: str):
        self.run_id  = run_id
        self.ticker  = ticker
        self._issues: list[dict] = []

    # ── helpers ─────────────────────────────────────────────
    def _issue(self, check: str, severity: str, desc: str,
               issue_date=None, raw=None):
        self._issues.append(dict(
            check_name=check, severity=severity, description=desc,
            issue_date=issue_date, raw_value=str(raw) if raw is not None else None,
        ))
        log_issue(self.run_id, check, severity, desc,
                  ticker=self.ticker, issue_date=issue_date, raw_value=str(raw) if raw else None)

    # ── individual checks ────────────────────────────────────
    def check_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            self._issue("missing_columns", "ERROR",
                        f"Required columns missing: {missing}")
        return df

    def check_null_values(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                continue
            nulls = df[col].isna().sum()
            if nulls > 0:
                self._issue("null_values", "ERROR",
                            f"Column '{col}' has {nulls} null value(s)")
        return df.dropna(subset=self.REQUIRED_COLUMNS)

    def check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        dupes = df.duplicated(subset=["date", "ticker"] if "ticker" in df.columns
                              else ["date"]).sum()
        if dupes > 0:
            self._issue("duplicate_rows", "ERROR",
                        f"{dupes} duplicate (date, ticker) row(s) found")
        return df.drop_duplicates(
            subset=["date", "ticker"] if "ticker" in df.columns else ["date"]
        )

    def check_negative_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col not in df.columns:
                continue
            neg_mask = df[col] <= 0
            if neg_mask.any():
                bad_dates = df.loc[neg_mask, "date"].tolist()
                self._issue("negative_price", "ERROR",
                            f"Column '{col}' has {neg_mask.sum()} non-positive value(s)",
                            raw=bad_dates[:5])
        return df[~df[["open", "high", "low", "close"]].le(0).any(axis=1)]

    def check_high_low_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        if not {"high", "low"}.issubset(df.columns):
            return df
        bad_mask = df["high"] < df["low"]
        if bad_mask.any():
            for _, row in df[bad_mask].iterrows():
                self._issue("high_lt_low", "ERROR",
                            f"High ({row['high']}) < Low ({row['low']})",
                            issue_date=row["date"],
                            raw=f"high={row['high']} low={row['low']}")
        return df[~bad_mask]

    def check_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        if "volume" not in df.columns:
            return df
        zero_vol = (df["volume"] == 0).sum()
        if zero_vol > 0:
            self._issue("zero_volume", "WARNING",
                        f"{zero_vol} row(s) with zero volume")
        neg_vol = (df["volume"] < 0).sum()
        if neg_vol > 0:
            self._issue("negative_volume", "ERROR",
                        f"{neg_vol} row(s) with negative volume")
            df = df[df["volume"] >= 0]
        return df

    def check_date_format(self, df: pd.DataFrame) -> pd.DataFrame:
        if "date" not in df.columns:
            return df
        try:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        except Exception as e:
            self._issue("date_format", "ERROR", f"Cannot parse dates: {e}")
        return df

    def check_daily_return_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flag daily returns outside ±50 % as suspicious."""
        if "daily_return" not in df.columns:
            return df
        extreme = df[df["daily_return"].abs() > 0.50]
        for _, row in extreme.iterrows():
            self._issue("extreme_return", "WARNING",
                        f"Daily return {row['daily_return']:.2%} exceeds ±50%",
                        issue_date=row["date"],
                        raw=row["daily_return"])
        return df

    # ── run all ──────────────────────────────────────────────
    def run_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run all structural quality checks on raw OHLCV data.
        Note: check_daily_return_consistency is intentionally excluded here
        because daily_return has not been computed yet at this stage.
        It is called explicitly in transform_stock_data() after calculation.
        """
        logger.info("[AUDIT] Running quality checks for %s, rows=%d",
                    self.ticker, len(df))
        df = self.check_missing_columns(df)
        df = self.check_date_format(df)
        df = self.check_null_values(df)
        df = self.check_duplicates(df)
        df = self.check_negative_prices(df)
        df = self.check_high_low_integrity(df)
        df = self.check_volume(df)
        logger.info("[AUDIT] Checks done. Issues found: %d  |  Clean rows: %d",
                    len(self._issues), len(df))
        return df

    def summary(self) -> dict:
        errors   = [i for i in self._issues if i["severity"] == "ERROR"]
        warnings = [i for i in self._issues if i["severity"] == "WARNING"]
        return {
            "ticker":        self.ticker,
            "total_issues":  len(self._issues),
            "errors":        len(errors),
            "warnings":      len(warnings),
            "checks_passed": len(self._issues) == 0,
        }
