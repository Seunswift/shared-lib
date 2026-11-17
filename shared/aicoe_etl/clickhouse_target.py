"""ClickHouse side: connect, run queries, list tables / columns, create, insert."""
import logging
import re

from ._util import clean_sql, split_identifier, sql_literal
from .config import ClickHouseConfig

log = logging.getLogger("aicoe_etl")

_READ_RE = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXISTS|EXPLAIN)\b", re.IGNORECASE)


class ClickHouseTarget:
    def __init__(self, cfg: ClickHouseConfig):
        self.cfg = cfg
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                import clickhouse_connect
            except ImportError as e:
                raise ImportError("Run `pip install clickhouse-connect`.") from e
            c = self.cfg
            self._client = clickhouse_connect.get_client(
                host=c.host, port=c.port, username=c.user, password=c.password,
                database=c.database, secure=c.secure, **c.extra)
        return self._client

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def _split(self, table: str):
        parts = split_identifier(table)
        if len(parts) == 1:
            return self.cfg.database, parts[0]
        if len(parts) == 2:
            return parts[0], parts[1]
        raise ValueError(f"Invalid ClickHouse table name {table!r} (use 'table' or 'database.table').")

    def full_name(self, table: str) -> str:
        db, tbl = self._split(table)
        return f"`{db}`.`{tbl}`"

    # -- queries ------------------------------------------------------------
    def run(self, sql: str):
        """Reads (SELECT/SHOW/DESCRIBE/...) return (rows, cols); anything else
        (CREATE, DROP, INSERT ... SELECT, ALTER) runs as a command -> ([], [])."""
        sql = clean_sql(sql)
        if _READ_RE.match(sql):
            r = self.client.query(sql)
            log.info("ClickHouse: %d row(s)", len(r.result_rows))
            return r.result_rows, list(r.column_names)
        self.client.command(sql)
        log.info("ClickHouse: command executed")
        return [], []

    # -- metadata -----------------------------------------------------------
    def tables(self, database: str = None):
        db = database or self.cfg.database
        r = self.client.query(f"SELECT name FROM system.tables WHERE database = {sql_literal(db)} "
                              f"ORDER BY name")
        return [row[0] for row in r.result_rows]

    def table_exists(self, table: str) -> bool:
        db, tbl = self._split(table)
        r = self.client.query(f"SELECT count() FROM system.tables WHERE database = {sql_literal(db)} "
                              f"AND name = {sql_literal(tbl)}")
        return r.result_rows[0][0] > 0

    def columns(self, table: str):
        """[(column_name, clickhouse_type)] in table order."""
        db, tbl = self._split(table)
        r = self.client.query(f"SELECT name, type FROM system.columns WHERE database = {sql_literal(db)} "
                              f"AND table = {sql_literal(tbl)} ORDER BY position")
        if not r.result_rows:
            raise ValueError(f"Table {db}.{tbl} not found in ClickHouse.")
        return [(row[0], row[1]) for row in r.result_rows]

    # -- write --------------------------------------------------------------
    def create_table(self, table: str, schema, order_by_cols=None):
        order_by_cols = order_by_cols or []
        cols_sql = ",\n    ".join(f"`{n}` {t}" for n, t in schema)
        order_sql = ", ".join(f"`{c}`" for c in order_by_cols) if order_by_cols else "tuple()"
        ddl = (f"CREATE TABLE IF NOT EXISTS {self.full_name(table)} (\n    {cols_sql}\n)\n"
               f"ENGINE = MergeTree()\nORDER BY {order_sql}")
        log.info("Creating table:\n%s", ddl)
        self.client.command(ddl)

    def drop_table(self, table: str):
        log.info("Dropping table %s", self.full_name(table))
        self.client.command(f"DROP TABLE IF EXISTS {self.full_name(table)}")

    def insert(self, table: str, rows, column_names):
        db, tbl = self._split(table)
        self.client.insert(table=tbl, data=rows, column_names=column_names, database=db)
