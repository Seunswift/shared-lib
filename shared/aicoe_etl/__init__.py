"""
aicoe_etl - run Trino / ClickHouse queries and Trino -> ClickHouse loads without
worrying about connections, types or batching.

    from aicoe_etl import EtlClient, TrinoConfig, ClickHouseConfig

    etl = EtlClient(trino=TrinoConfig(...), clickhouse=ClickHouseConfig(...))
    etl.ping()
    etl.trino_tables();  etl.trino_columns("aicoe.sample")
    etl.trino_query("SELECT ...");  etl.clickhouse_query("SELECT ...")
    etl.run_etl("SELECT ...", "target_table", order_by_cols=["id"])
"""
from .client import EtlClient, enable_logging
from .config import ClickHouseConfig, TrinoConfig

__all__ = ["EtlClient", "TrinoConfig", "ClickHouseConfig", "enable_logging"]
