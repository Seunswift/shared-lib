"""EtlClient: one object for Trino queries, ClickHouse queries and Trino -> ClickHouse ETL."""
import logging

from . import convert
from ._util import to_output
from .clickhouse_target import ClickHouseTarget
from .config import ClickHouseConfig, TrinoConfig
from .trino_source import TrinoSource

log = logging.getLogger("aicoe_etl")


def enable_logging(level=logging.INFO):
    """Show progress messages (connection, row counts, inserts) in the notebook/console."""
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(h)
        log.propagate = False
    log.setLevel(level)


class EtlClient:
    """
    Example
    -------
    >>> etl = EtlClient(
    ...     trino=TrinoConfig(host="...", user="...", password="...", catalog="hive", schema="aicoe"),
    ...     clickhouse=ClickHouseConfig(host="...", password="...", database="sample_db"))
    >>> etl.trino_columns("aicoe.sample")
    >>> etl.trino_query("SELECT * FROM aicoe.sample LIMIT 10")
    >>> etl.run_etl("SELECT * FROM aicoe.sample", "sample_test", order_by_cols=["id"])

    You only need to pass the config for the side(s) you use.
    """

    def __init__(self, trino: TrinoConfig = None, clickhouse: ClickHouseConfig = None,
                 verbose: bool = True):
        self._trino_cfg, self._ch_cfg = trino, clickhouse
        self._trino = self._ch = None
        if verbose:
            enable_logging()

    @classmethod
    def from_env(cls, verbose: bool = True):
        """Build from TRINO_* and CH_* environment variables (whichever are set)."""
        import os
        trino = TrinoConfig.from_env() if os.environ.get("TRINO_HOST") else None
        ch = ClickHouseConfig.from_env() if os.environ.get("CH_HOST") else None
        return cls(trino=trino, clickhouse=ch, verbose=verbose)

    # -- lazy handles -------------------------------------------------------
    @property
    def trino(self) -> TrinoSource:
        if self._trino is None:
            if self._trino_cfg is None:
                raise ValueError("No Trino config - pass trino=TrinoConfig(...).")
            self._trino = TrinoSource(self._trino_cfg)
        return self._trino

    @property
    def clickhouse(self) -> ClickHouseTarget:
        if self._ch is None:
            if self._ch_cfg is None:
                raise ValueError("No ClickHouse config - pass clickhouse=ClickHouseConfig(...).")
            self._ch = ClickHouseTarget(self._ch_cfg)
        return self._ch

    def close(self):
        if self._ch is not None:
            self._ch.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- connectivity -------------------------------------------------------
    def ping(self):
        """Test every configured connection. Returns a dict of results."""
        out = {}
        if self._trino_cfg:
            out["trino"] = str(self.trino.ping())
        if self._ch_cfg:
            out["clickhouse"] = str(self.clickhouse.client.server_version)
        log.info("Connections OK: %s", out)
        return out

    # -- Trino --------------------------------------------------------------
    def trino_query(self, sql: str, output: str = "df"):
        """Run any query on Trino. output='df' (DataFrame, default) or 'rows' -> (rows, cols)."""
        rows, desc = self.trino.execute(sql)
        return to_output(rows, [d[0] for d in desc], output)

    def trino_tables(self, schema: str = None):
        """List tables in a Trino schema ('schema' or 'catalog.schema'; default = config)."""
        return self.trino.tables(schema)

    def trino_columns(self, table: str, output: str = "df"):
        """Columns of a Trino table with the ClickHouse type each would get."""
        cols = self.trino.columns(table)
        rows = [(n, t, convert.trino_type_to_clickhouse(t, True)) for n, t in cols]
        return to_output(rows, ["column", "trino_type", "clickhouse_type"], output)

    def extract(self, query: str):
        """Run a SELECT on Trino -> (rows, description) with description = [(name, trino_type)]."""
        return self.trino.execute(query)

    # -- ClickHouse ---------------------------------------------------------
    def clickhouse_query(self, sql: str, output: str = "df"):
        """Run any statement on ClickHouse. Reads return a DataFrame/rows; DDL returns empty."""
        rows, cols = self.clickhouse.run(sql)
        return to_output(rows, cols, output)

    def clickhouse_tables(self, database: str = None):
        return self.clickhouse.tables(database)

    def clickhouse_columns(self, table: str, output: str = "df"):
        cols = self.clickhouse.columns(table)
        return to_output(cols, ["column", "clickhouse_type"], output)

    # -- ETL ----------------------------------------------------------------
    def run_etl(self, source_query: str, target_table: str, order_by_cols=None,
                non_nullable_cols=None, batch_size: int = 500_000, mode: str = "replace"):
        """
        Trino SELECT -> ClickHouse table.

        mode='append'  create the table if missing, then add rows to it.
        mode='replace' (default)drop the target table first, recreate it from the query, then load.

        Rows are streamed in batches of `batch_size`, so large tables don't have to fit
        in memory. order_by_cols become the MergeTree ORDER BY and are never Nullable.
        Returns {'table', 'rows', 'created', 'out_of_range'}.
        """
        if mode not in ("append", "replace"):
            raise ValueError("mode must be 'append' or 'replace'.")
        order_by_cols = list(order_by_cols or [])
        non_nullable = set(non_nullable_cols or []) | set(order_by_cols)

        ch = self.clickhouse
        description, batches = self.trino.stream(source_query, batch_size)
        try:
            names = [n for n, _ in description]
            unknown = (set(order_by_cols) | set(non_nullable)) - set(names)
            if unknown:
                raise ValueError(f"Column(s) {sorted(unknown)} not in the query result {names}.")

            schema = convert.build_clickhouse_schema(description, non_nullable)
            out_of_range = {}
            converters = convert.build_converters(description, out_of_range, non_nullable)
            target = ch.full_name(target_table)

            if mode == "replace":
                ch.drop_table(target_table)
            exists = ch.table_exists(target_table)
            if exists:
                existing = {n for n, _ in ch.columns(target_table)}
                missing, extra = set(names) - existing, existing - set(names)
                if missing or extra:
                    raise ValueError(
                        f"{target} already exists with different columns. "
                        f"In query but not in table: {sorted(missing)}. "
                        f"In table but not in query: {sorted(extra)}. "
                        f"Fix the query, or use mode='replace' to rebuild the table.")
            else:
                ch.create_table(target_table, schema, order_by_cols)

            total = 0
            for batch in batches:
                ch.insert(target_table, convert.convert_rows(batch, converters), names)
                total += len(batch)
                log.info("Inserted %d rows so far", total)
        finally:
            batches.close()

        for col, info in out_of_range.items():
            log.warning("Column %r: %d value(s) were outside ClickHouse's date/time range and were "
                        "loaded as NULL (examples: %s)", col, info["count"], ", ".join(info["examples"]))
        if total == 0:
            log.warning("Query returned 0 rows - table created (if missing) but nothing inserted.")
        log.info("Done: %d rows -> %s", total, target)
        return {"table": target, "rows": total, "created": not exists, "out_of_range": out_of_range}
