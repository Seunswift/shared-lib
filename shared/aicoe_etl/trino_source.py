"""Trino side: connect, run queries, list tables / columns, stream results."""
import logging

from ._util import clean_sql, split_identifier, sql_literal
from .config import TrinoConfig

log = logging.getLogger("aicoe_etl")


class TrinoSource:
    def __init__(self, cfg: TrinoConfig):
        self.cfg = cfg

    # -- connection ---------------------------------------------------------
    def connect(self):
        try:
            from trino.auth import BasicAuthentication
            from trino.dbapi import connect
        except ImportError as e:
            raise ImportError(
                "Could not import the 'trino' package. Run `pip install trino` and make sure "
                "no file named trino.py sits next to your notebook (it shadows the package)."
            ) from e
        c = self.cfg
        kwargs = dict(host=c.host, port=c.port, user=c.user, catalog=c.catalog,
                      schema=c.schema, http_scheme=c.http_scheme, verify=c.verify, **c.extra)
        if c.password:
            kwargs["auth"] = BasicAuthentication(c.user, c.password)
        return connect(**kwargs)

    # -- queries ------------------------------------------------------------
    def execute(self, sql: str):
        """Run a query; returns (rows, description) with description = [(name, type)]."""
        sql = clean_sql(sql)
        conn = self.connect()
        try:
            cur = conn.cursor()
            log.info("Trino: running query...")
            cur.execute(sql)
            rows = cur.fetchall()      # also waits for completion and surfaces errors
            desc = [(d[0], d[1]) for d in (cur.description or [])]
            stats = getattr(cur, "stats", None) or {}
            log.info("Trino: %d row(s), %d column(s) in %.2fs", len(rows), len(desc),
                     stats.get("elapsedTimeMillis", 0) / 1000)
            return rows, desc
        finally:
            conn.close()

    def stream(self, sql: str, batch_size: int):
        """Run a SELECT and return (description, batches) where batches is a generator
        of row lists (<= batch_size each). Close the generator when done."""
        sql = clean_sql(sql)
        conn = self.connect()
        try:
            cur = conn.cursor()
            log.info("Trino: submitting query (this can take a while before the first rows arrive)...")
            cur.execute(sql)
            log.info("Trino: query accepted, results are starting to arrive")
            if not cur.description:
                raise ValueError("The source query returned no columns - it must be a SELECT.")
            desc = [(d[0], d[1]) for d in cur.description]
        except Exception:
            conn.close()
            raise

        def _gen():
            try:
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    yield rows
            finally:
                conn.close()

        return desc, _gen()

    # -- metadata -----------------------------------------------------------
    def _resolve(self, name: str, parts_expected: int):
        parts = split_identifier(name)
        if len(parts) > parts_expected:
            raise ValueError(f"Too many parts in name {name!r}.")
        defaults = [self.cfg.catalog, self.cfg.schema]
        return [p.lower() for p in (defaults[:parts_expected - len(parts)] + parts)]

    def tables(self, schema: str = None):
        """Table names in a schema ('schema' or 'catalog.schema'; default = config)."""
        catalog, sch = self._resolve(schema, 2) if schema else (self.cfg.catalog, self.cfg.schema)
        rows, _ = self.execute(
            f'SELECT table_name FROM "{catalog}".information_schema.tables '
            f"WHERE table_schema = {sql_literal(sch)} ORDER BY table_name")
        return [r[0] for r in rows]

    def columns(self, table: str):
        """[(column_name, trino_type)] for 'table', 'schema.table' or 'catalog.schema.table'."""
        catalog, sch, tbl = self._resolve(table, 3)
        rows, _ = self.execute(
            f'SELECT column_name, data_type FROM "{catalog}".information_schema.columns '
            f"WHERE table_schema = {sql_literal(sch)} AND table_name = {sql_literal(tbl)} "
            f"ORDER BY ordinal_position")
        if not rows:
            raise ValueError(f"Table {catalog}.{sch}.{tbl} not found in Trino (or has no columns).")
        return [(r[0], r[1]) for r in rows]

    def ping(self):
        rows, _ = self.execute("SELECT current_timestamp")
        return rows[0][0]
