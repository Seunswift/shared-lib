# aicoe_etl

Run Trino and ClickHouse queries, and load Trino query results into ClickHouse, from a notebook or script. The module handles connections, type conversion and batching, so you only write queries.

## Setup

```
pip install trino clickhouse-connect pandas
```

1. Put the `aicoe_etl` folder next to your notebook or script.
2. Make sure there is **no file named `trino.py`** in that folder. It hides the real `trino` package and breaks the import.

## Connect

### Option A: in code

```python
import getpass
from aicoe_etl import EtlClient, TrinoConfig, ClickHouseConfig

etl = EtlClient(
    trino=TrinoConfig(host="TRINO_HOST", port=443, user="aicoe_svc",
                      password=getpass.getpass("Trino password: "),
                      catalog="hive", schema="aicoe", verify=False),
    clickhouse=ClickHouseConfig(host="CH_HOST", port=8123, user="default",
                                password="CH_PASSWORD", database="sample_db"),
)
etl.ping()      # tests both connections
```

You only need to pass the side you use (Trino only, or ClickHouse only).

### Option B: environment variables (keeps passwords out of code)

| Trino | ClickHouse |
|---|---|
| `TRINO_HOST`, `TRINO_PORT` (443), `TRINO_USER`, `TRINO_PASSWORD` | `CH_HOST`, `CH_PORT` (8123), `CH_USER` (default), `CH_PASSWORD` |
| `TRINO_CATALOG` (hive), `TRINO_SCHEMA` (default) | `CH_DATABASE` (default), `CH_SECURE` (false) |
| `TRINO_HTTP_SCHEME` (https), `TRINO_VERIFY` (false) | |

```python
etl = EtlClient.from_env()
```

## What you can do

```python
# Trino
etl.trino_tables()                                   # tables in the schema
etl.trino_columns("aicoe.sample")                    # columns, Trino type, ClickHouse type it would get
etl.trino_query("SELECT * FROM aicoe.sample LIMIT 10")   # DataFrame

# ClickHouse
etl.clickhouse_tables()
etl.clickhouse_columns("customer_master")
etl.clickhouse_query("SELECT count() FROM customer_master")   # DataFrame; DDL works too

# Raw extract: (rows, [(column, trino_type), ...])
rows, description = etl.extract("SELECT * FROM aicoe.sample LIMIT 100")
```

Add `output="rows"` to any query method to get `(rows, columns)` instead of a DataFrame.
`trino_query`, `extract` and `clickhouse_query` load the whole result into memory. For large tables use `run_etl`, or add a `LIMIT`.

## Load data: Trino to ClickHouse

```python
etl.run_etl(
    source_query="SELECT * FROM aicoe.customer_master WHERE tbl_dt = DATE '2026-11-15'",
    target_table="customer_master",        # or "database.table"
    order_by_cols=["msisdn"],              # optional, see below
    non_nullable_cols=[],                  # optional
    batch_size=200_000,                    # rows per batch (default 50,000)
    mode="append",                         # or "replace"
)
```

Returns `{'table', 'rows', 'created', 'out_of_range'}`.

**What it does:** runs the query on Trino, creates the ClickHouse table if it doesn't exist (column types inferred from the Trino types), then reads, converts and inserts the rows batch by batch, so a large table never has to fit in memory.

### Parameters

- **`order_by_cols`**: the table's MergeTree sort key. It makes queries that filter on those columns much faster. Empty means no sort key (`ORDER BY tuple()`). These columns become non-nullable, so they must never be NULL in the source. It only applies when the table is created.
- **`non_nullable_cols`**: columns you know are always populated. They get a plain type instead of `Nullable(...)`.
- **`mode="append"`** (default): creates the table if missing, then adds rows.
- **`mode="replace"`**: drops the target table, recreates it, then loads. This **deletes existing data in that table**, including earlier days if you load one day at a time.

### Duplicates warning

With `append`, if a load fails halfway, the batches already inserted stay in ClickHouse. Re-running then loads those rows again. After a failure, check the row count in ClickHouse before re-running (or use `replace` for full reloads).

### Existing tables

If the target table already exists, its columns must match the query's columns. Otherwise the load stops with a message listing the differences.

## Type handling

| Trino | ClickHouse |
|---|---|
| varchar, char, json | `String` |
| tinyint, smallint, integer, bigint | `Int8`, `Int16`, `Int32`, `Int64` |
| real, double | `Float32`, `Float64` |
| decimal(p,s) | `Decimal(p, s)` |
| boolean | `UInt8` |
| date | `Date32` (1900 to 2299) |
| timestamp | `DateTime` (1970 to 2106, second precision) |
| anything else (array, map, row, uuid, time...) | `String` (converted with `str()`) |

All columns are `Nullable(...)` unless listed in `non_nullable_cols` or `order_by_cols`.

**Out-of-range dates:** a date or timestamp outside the range above (for example a 1960 birth date in a `timestamp` column, or a `0001-01-01` placeholder) is loaded as `NULL`. The load logs a warning per column with the count and example values. If the column is non-nullable, the load stops with an error instead.

## Troubleshooting

| Error | Likely cause |
|---|---|
| `No module named 'trino.auth'` or `'trino' is not a package` | A `trino.py` file is in the folder. Delete it (and the `__pycache__` folder), then restart the kernel. |
| `error 401: Unauthorized` (Trino) | Missing or wrong Trino username or password. |
| `error 404: Not Found` (Trino) | Wrong host or port. Use the exact hostname and port that work for your team (a proxy may reject the raw IP). |
| ClickHouse authentication failed | `CH_PASSWORD` missing or wrong. |
| `already exists with different columns` | The query's columns differ from the existing table. Fix the query, or use `mode="replace"`. |
| No logs for a long time at the start | Trino is still running the query. Progress appears once rows start arriving. |

Progress is logged automatically (`EtlClient(..., verbose=False)` turns it off).
