"""
pipeline.py — Python Data Pipeline Engineering Lab
Omnichannel Retail ETL: Extract -> Transform -> Validate -> Load

Builds an idempotent, incrementally-loadable Star Schema (SQLite) from a
messy multi-batch retail orders workbook, quarantining any row that fails
data-quality rules instead of letting one bad row stop the whole run.

Run:  python pipeline.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging — every extract/transform/load step reports what it did
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("retail_pipeline")


# ---------------------------------------------------------------------------
# Task 1 — Pipeline configuration
# ---------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    input_path: str                 # path to the source Excel workbook (never modified)
    output_db: str                  # path to the SQLite database file
    batches: list[str]              # e.g. ["orders_batch_1", "orders_batch_2"], processed in order
    error_mode: Literal["quarantine", "fail_fast"] = "quarantine"
    quarantine_path: str = "quarantine.csv"
    run_log_path: str = "pipeline_run_log.csv"


# ---------------------------------------------------------------------------
# Business-rule reference values (from data_dictionary + README)
# ---------------------------------------------------------------------------
PAYMENT_METHOD_MAP = {
    "cash": "Cash",
    "credit card": "Credit Card",
    "bank transfer": "Bank Transfer",
    "promptpay": "PromptPay",
}
SALES_CHANNEL_MAP = {
    "store": "Store",
    "online": "Online",
    "marketplace": "Marketplace",
    "e-commerce": "Online",   # data_dictionary rule: map E-Commerce -> Online
}
APPROVED_PAYMENT_METHODS = set(PAYMENT_METHOD_MAP.values())
APPROVED_SALES_CHANNELS = {"Store", "Online", "Marketplace"}

QTY_MIN, QTY_MAX = 1, 20
DISCOUNT_MIN, DISCOUNT_MAX = 0, 100


# ---------------------------------------------------------------------------
# Task 1 — Extract
# ---------------------------------------------------------------------------
def extract_sheet(input_path: str, sheet_name: str) -> pd.DataFrame:
    """Read one sheet from the source workbook as strings (no silent type
    coercion at extract time — that belongs in Transform). Logs the sheet
    name, row count and elapsed time; never edits the source file."""
    started = time.time()
    log.info("EXTRACT start | sheet=%s", sheet_name)
    try:
        df = pd.read_excel(input_path, sheet_name=sheet_name, dtype=str)
    except Exception as exc:
        log.error("EXTRACT failed | sheet=%s | %s: %s", sheet_name, type(exc).__name__, exc)
        raise
    elapsed = time.time() - started
    log.info("EXTRACT done  | sheet=%s | rows=%d | %.3fs", sheet_name, len(df), elapsed)
    return df


# ---------------------------------------------------------------------------
# Task 2 — Transform helpers
# ---------------------------------------------------------------------------
def _safe_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_payment_method(raw: Optional[str]) -> Optional[str]:
    if raw is None or pd.isna(raw):
        return None
    return PAYMENT_METHOD_MAP.get(str(raw).strip().lower())


def normalize_sales_channel(raw: Optional[str]) -> Optional[str]:
    if raw is None or pd.isna(raw):
        return None
    return SALES_CHANNEL_MAP.get(str(raw).strip().lower())


def transform_orders(
    raw: pd.DataFrame,
    valid_customer_ids: set[str],
    valid_product_ids: set[str],
    inactive_product_ids: set[str],
) -> pd.DataFrame:
    """Coerce types safely, normalize categorical fields, and evaluate every
    data-quality rule from the data_dictionary. Returns the frame with two
    new columns added: `reason_codes` (list[str], empty = valid) and the
    cleaned/typed versions of every field. No row is dropped here — that
    split happens in validate()."""
    df = raw.copy()

    # --- safe type coercion (errors -> NaT / NaN, never a crash) ----------
    df["order_datetime_parsed"] = _safe_datetime(df["order_datetime"])
    df["updated_at_parsed"] = _safe_datetime(df["updated_at"])
    df["quantity_num"] = _safe_numeric(df["quantity"])
    df["unit_price_num"] = _safe_numeric(df["unit_price"])
    df["discount_pct_num"] = _safe_numeric(df["discount_pct"])

    # --- normalize categoricals ---------------------------------------
    df["payment_method_norm"] = df["payment_method"].apply(normalize_payment_method)
    df["sales_channel_norm"] = df["sales_channel"].apply(normalize_sales_channel)

    # --- evaluate each data-quality rule, collect reason codes ---------
    reasons = [[] for _ in range(len(df))]

    def flag(mask: pd.Series, code: str) -> None:
        for i in df.index[mask]:
            reasons[df.index.get_loc(i)].append(code)

    flag(df["customer_id"].isna(), "MISSING_CUSTOMER_ID")
    flag(df["customer_id"].notna() & ~df["customer_id"].isin(valid_customer_ids), "CUSTOMER_NOT_FOUND")

    flag(df["product_id"].isna(), "MISSING_PRODUCT_ID")
    flag(df["product_id"].notna() & ~df["product_id"].isin(valid_product_ids), "PRODUCT_NOT_FOUND")
    flag(df["product_id"].isin(inactive_product_ids), "PRODUCT_INACTIVE")

    bad_qty = df["quantity_num"].isna() | (df["quantity_num"] % 1 != 0) \
        | (df["quantity_num"] < QTY_MIN) | (df["quantity_num"] > QTY_MAX)
    flag(bad_qty, "INVALID_QUANTITY")

    bad_price = df["unit_price_num"].isna() | (df["unit_price_num"] <= 0)
    flag(bad_price, "INVALID_UNIT_PRICE")

    bad_discount = df["discount_pct_num"].isna() \
        | (df["discount_pct_num"] < DISCOUNT_MIN) | (df["discount_pct_num"] > DISCOUNT_MAX)
    flag(bad_discount, "INVALID_DISCOUNT_PCT")

    flag(df["order_datetime_parsed"].isna(), "INVALID_ORDER_DATETIME")
    flag(df["updated_at_parsed"].isna(), "INVALID_UPDATED_AT")

    flag(df["payment_method_norm"].isna(), "INVALID_PAYMENT_METHOD")
    flag(df["sales_channel_norm"].isna(), "INVALID_SALES_CHANNEL")

    df["reason_codes"] = [";".join(r) for r in reasons]
    df["is_valid"] = df["reason_codes"] == ""

    # --- derived measures (only meaningful for rows that will pass) ----
    df["gross_amount"] = df["quantity_num"] * df["unit_price_num"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct_num"].fillna(0) / 100)

    return df


def deduplicate(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep, per order_id, the record with the latest updated_at. Returns
    (deduped_df, number_of_duplicate_rows_removed)."""
    before = len(valid_df)
    deduped = (
        valid_df.sort_values("updated_at_parsed")
        .drop_duplicates(subset="order_id", keep="last")
        .copy()
    )
    removed = before - len(deduped)
    return deduped, removed


# ---------------------------------------------------------------------------
# Task 3 — Star schema DDL
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     TEXT UNIQUE NOT NULL,
    customer_name   TEXT,
    province        TEXT,
    segment         TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      TEXT UNIQUE NOT NULL,
    product_name    TEXT,
    category        TEXT,
    active_flag     TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date   TEXT UNIQUE NOT NULL,
    day         INTEGER,
    month       INTEGER,
    quarter     INTEGER,
    year        INTEGER
);

-- Grain: one validated order-product line per order_id
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id        TEXT PRIMARY KEY,
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity        INTEGER NOT NULL,
    unit_price      REAL NOT NULL,
    discount_pct    REAL NOT NULL,
    gross_amount    REAL NOT NULL,
    net_amount      REAL NOT NULL,
    payment_method  TEXT NOT NULL,
    sales_channel   TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    source_batch    TEXT NOT NULL,
    loaded_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    order_id        TEXT,
    order_datetime  TEXT,
    customer_id     TEXT,
    product_id      TEXT,
    quantity        TEXT,
    unit_price      TEXT,
    discount_pct    TEXT,
    payment_method  TEXT,
    sales_channel   TEXT,
    updated_at      TEXT,
    source_batch    TEXT,
    reason_code     TEXT,
    quarantined_at  TEXT,
    UNIQUE(order_id, source_batch)   -- re-running a batch refreshes, not duplicates, its quarantine rows
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    batch        TEXT,
    started_at   TEXT,
    ended_at     TEXT,
    rows_read    INTEGER,
    rows_valid   INTEGER,
    rows_rejected INTEGER,
    rows_duplicated INTEGER,
    rows_loaded  INTEGER,
    status       TEXT
);
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL)
    return conn


def load_dimensions(conn: sqlite3.Connection, customers: pd.DataFrame, products: pd.DataFrame) -> None:
    """Idempotent dimension load: INSERT OR IGNORE keyed on the natural key,
    so re-running the pipeline never creates duplicate dimension rows."""
    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO dim_customer (customer_id, customer_name, province, segment) "
            "VALUES (?, ?, ?, ?)",
            customers[["customer_id", "customer_name", "province", "segment"]].itertuples(index=False, name=None),
        )
        conn.executemany(
            "INSERT OR IGNORE INTO dim_product (product_id, product_name, category, active_flag) "
            "VALUES (?, ?, ?, ?)",
            products[["product_id", "product_name", "category", "active_flag"]].itertuples(index=False, name=None),
        )
    log.info("LOAD dims     | dim_customer=%d dim_product=%d (idempotent upsert)",
              len(customers), len(products))


def ensure_dates(conn: sqlite3.Connection, dates: pd.Series) -> None:
    rows = []
    for d in dates.dropna().unique():
        ts = pd.Timestamp(d)
        date_key = int(ts.strftime("%Y%m%d"))
        rows.append((date_key, ts.strftime("%Y-%m-%d"), ts.day, ts.month, (ts.month - 1) // 3 + 1, ts.year))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def load_quarantine(conn: sqlite3.Connection, rejected: pd.DataFrame, source_batch: str) -> None:
    if rejected.empty:
        return
    now = datetime.now().isoformat(timespec="seconds")
    cols = ["order_id", "order_datetime", "customer_id", "product_id", "quantity",
            "unit_price", "discount_pct", "payment_method", "sales_channel", "updated_at"]
    rows = [
        tuple(row) + (source_batch, reason, now)
        for row, reason in zip(rejected[cols].itertuples(index=False, name=None), rejected["reason_codes"])
    ]
    conn.executemany(
        f"INSERT OR REPLACE INTO quarantine ({', '.join(cols)}, source_batch, reason_code, quarantined_at) "
        f"VALUES ({', '.join(['?'] * (len(cols) + 3))})",
        rows,
    )


def upsert_fact(conn: sqlite3.Connection, deduped: pd.DataFrame, source_batch: str) -> int:
    """Insert new order_ids; update an existing order_id ONLY if the
    incoming updated_at is newer than what's stored (incremental rule).
    Re-running the exact same batch therefore loads 0 new/changed rows —
    this is what makes the pipeline idempotent."""
    if deduped.empty:
        return 0

    cust_map = dict(conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
    prod_map = dict(conn.execute("SELECT product_id, product_key FROM dim_product").fetchall())
    existing = dict(conn.execute("SELECT order_id, updated_at FROM fact_sales").fetchall())

    now = datetime.now().isoformat(timespec="seconds")
    to_insert, to_update = [], []
    for row in deduped.itertuples(index=False):
        date_key = int(pd.Timestamp(row.order_datetime_parsed).strftime("%Y%m%d"))
        new_updated_at = row.updated_at_parsed.isoformat(sep=" ")
        values = (
            row.order_id, date_key, cust_map[row.customer_id], prod_map[row.product_id],
            int(row.quantity_num), float(row.unit_price_num), float(row.discount_pct_num),
            float(row.gross_amount), float(row.net_amount),
            row.payment_method_norm, row.sales_channel_norm,
            new_updated_at, source_batch, now,
        )
        if row.order_id not in existing:
            to_insert.append(values)
        elif new_updated_at > existing[row.order_id]:
            to_update.append(values)
        # else: already loaded with an equal/newer updated_at -> skip (idempotent no-op)

    if to_insert:
        conn.executemany(
            "INSERT INTO fact_sales (order_id, date_key, customer_key, product_key, quantity, "
            "unit_price, discount_pct, gross_amount, net_amount, payment_method, sales_channel, "
            "updated_at, source_batch, loaded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            to_insert,
        )
    if to_update:
        conn.executemany(
            "UPDATE fact_sales SET date_key=?, customer_key=?, product_key=?, quantity=?, "
            "unit_price=?, discount_pct=?, gross_amount=?, net_amount=?, payment_method=?, "
            "sales_channel=?, updated_at=?, source_batch=?, loaded_at=? WHERE order_id=?",
            [(v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[0])
             for v in to_update],
        )
    return len(to_insert) + len(to_update)


def log_run(conn: sqlite3.Connection, batch: str, started_at: str, ended_at: str,
            rows_read: int, rows_valid: int, rows_rejected: int, rows_duplicated: int,
            rows_loaded: int, status: str) -> None:
    with conn:
        conn.execute(
            "INSERT INTO pipeline_run_log (batch, started_at, ended_at, rows_read, rows_valid, "
            "rows_rejected, rows_duplicated, rows_loaded, status) VALUES (?,?,?,?,?,?,?,?,?)",
            (batch, started_at, ended_at, rows_read, rows_valid, rows_rejected, rows_duplicated,
             rows_loaded, status),
        )


# ---------------------------------------------------------------------------
# Task 5 — Orchestration
# ---------------------------------------------------------------------------
def run_pipeline(config: PipelineConfig) -> dict:
    """extract -> transform -> validate -> load, one batch at a time.
    A row-level failure is quarantined; a whole-batch failure (bad file,
    unreadable sheet) is logged as 'failed' and does NOT roll back or
    delete any batch that already loaded successfully."""
    conn = get_connection(config.output_db)

    customers = extract_sheet(config.input_path, "customers")
    products = extract_sheet(config.input_path, "products")
    load_dimensions(conn, customers, products)
    valid_customer_ids = set(customers["customer_id"])
    valid_product_ids = set(products["product_id"])
    inactive_product_ids = set(products.loc[products["active_flag"] == "N", "product_id"])

    summary = {"batches": []}

    for batch in config.batches:
        started_dt = datetime.now()
        started_at = started_dt.isoformat(timespec="seconds")
        rows_read = rows_valid = rows_rejected = rows_duplicated = rows_loaded = 0
        try:
            raw = extract_sheet(config.input_path, batch)
            rows_read = len(raw)

            transformed = transform_orders(raw, valid_customer_ids, valid_product_ids, inactive_product_ids)
            valid_df = transformed[transformed["is_valid"]]
            rejected_df = transformed[~transformed["is_valid"]]
            rows_valid, rows_rejected = len(valid_df), len(rejected_df)

            if config.error_mode == "fail_fast" and rows_rejected > 0:
                raise ValueError(f"{rows_rejected} row(s) failed validation in {batch} (fail_fast mode)")

            deduped, rows_duplicated = deduplicate(valid_df)

            # Single atomic transaction for this batch's writes: either all of
            # dim_date/fact_sales/quarantine commit together, or none do —
            # batches that already committed earlier are untouched either way.
            with conn:
                ensure_dates(conn, deduped["order_datetime_parsed"])
                rows_loaded = upsert_fact(conn, deduped, batch)
                load_quarantine(conn, rejected_df, batch)

            status = "SUCCESS"
            ended_at = datetime.now().isoformat(timespec="seconds")
            log.info(
                "LOAD %-13s | read=%d valid=%d rejected=%d duplicated=%d loaded=%d | %s",
                batch, rows_read, rows_valid, rows_rejected, rows_duplicated, rows_loaded, status,
            )

        except Exception as exc:
            # Batch-level failure: record it, keep everything already committed, move on.
            rows_loaded = 0
            status = "FAILED"
            ended_at = datetime.now().isoformat(timespec="seconds")
            log.error("BATCH FAILED  | batch=%s | %s: %s", batch, type(exc).__name__, exc)

        log_run(conn, batch, started_at, ended_at, rows_read, rows_valid,
                rows_rejected, rows_duplicated, rows_loaded, status)
        summary["batches"].append({
            "batch": batch, "status": status, "rows_read": rows_read, "rows_valid": rows_valid,
            "rows_rejected": rows_rejected, "rows_duplicated": rows_duplicated, "rows_loaded": rows_loaded,
        })

    conn.close()
    return summary


# ---------------------------------------------------------------------------
# Export helpers (CSV deliverables + KPI summary)
# ---------------------------------------------------------------------------
def export_csvs(config: PipelineConfig) -> None:
    conn = sqlite3.connect(config.output_db)
    pd.read_sql("SELECT * FROM quarantine ORDER BY quarantined_at, order_id", conn).to_csv(
        config.quarantine_path, index=False)
    pd.read_sql("SELECT * FROM pipeline_run_log ORDER BY run_id", conn).to_csv(
        config.run_log_path, index=False)
    conn.close()


def print_kpi_summary(config: PipelineConfig) -> None:
    conn = sqlite3.connect(config.output_db)
    log_df = pd.read_sql("SELECT * FROM pipeline_run_log", conn)
    fact_count = pd.read_sql("SELECT COUNT(*) c FROM fact_sales", conn)["c"][0]
    net_total = pd.read_sql("SELECT COALESCE(SUM(net_amount),0) s FROM fact_sales", conn)["s"][0]
    conn.close()

    print("\n" + "=" * 72)
    print("PIPELINE RUN LOG")
    print("=" * 72)
    print(log_df.to_string(index=False))
    print("\n" + "=" * 72)
    print("KPI SUMMARY (cumulative, successful runs only)")
    print("=" * 72)
    ok = log_df[log_df.status == "SUCCESS"]
    print(f"rows read      : {ok.rows_read.sum()}")
    print(f"rows valid     : {ok.rows_valid.sum()}")
    print(f"rows rejected  : {ok.rows_rejected.sum()}")
    print(f"rows duplicated: {ok.rows_duplicated.sum()}")
    print(f"rows loaded    : {ok.rows_loaded.sum()}  (note: idempotent re-runs load 0 new rows)")
    print(f"fact_sales rows (final, deduplicated): {fact_count}")
    print(f"net sales total: {net_total:,.2f}")


# ---------------------------------------------------------------------------
# Main — demonstrates: batch_1, batch_1 rerun (idempotency), batch_2, batch_3
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    SOURCE = "/mnt/user-data/uploads/Python_Data_Pipeline_Lab_Dataset__1_.xlsx"
    DB = "retail_dw.db"

    # start clean so this script is safe to re-run from scratch during grading
    Path(DB).unlink(missing_ok=True)

    base_cfg = dict(input_path=SOURCE, output_db=DB, error_mode="quarantine")

    print("\n### RUN 1: orders_batch_1 (first load) ###")
    run_pipeline(PipelineConfig(batches=["orders_batch_1"], **base_cfg))

    print("\n### RUN 2: orders_batch_1 AGAIN (idempotency check — fact rows must not grow) ###")
    run_pipeline(PipelineConfig(batches=["orders_batch_1"], **base_cfg))

    print("\n### RUN 3: orders_batch_2 (incremental load) ###")
    run_pipeline(PipelineConfig(batches=["orders_batch_2"], **base_cfg))

    print("\n### RUN 4: orders_batch_3 (incremental load) ###")
    summary = run_pipeline(PipelineConfig(batches=["orders_batch_3"], **base_cfg))

    export_csvs(PipelineConfig(batches=[], **base_cfg))
    print_kpi_summary(PipelineConfig(batches=[], **base_cfg))
