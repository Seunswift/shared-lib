"""Trino -> ClickHouse type mapping and row value conversion.

Same rules as the original notebook, plus two fixes:
* Trino types with no ClickHouse equivalent (array, map, row, time, uuid, ...) are
  stored as String and converted with str() so the insert doesn't fail.
* Dates / timestamps outside ClickHouse's supported range (e.g. a 1950 birth date or
  0001-01-01 placeholder) no longer crash the insert: they are loaded as NULL and
  counted, and the ETL reports how many per column.
"""
import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

log = logging.getLogger("aicoe_etl")

_DECIMAL_RE = re.compile(r"decimal\((\d+),\s*(\d+)\)", re.IGNORECASE)

SIMPLE_TYPE_MAP = {
    "varchar": "String",
    "char": "String",
    "json": "String",
    "integer": "Int32",
    "int": "Int32",
    "bigint": "Int64",
    "smallint": "Int16",
    "tinyint": "Int8",
    "double": "Float64",
    "real": "Float32",
    "boolean": "UInt8",
    "date": "Date32",   # Date only covers 1970-2149; Date32 covers 1900-2299
    "timestamp": "DateTime",
}

# Ranges ClickHouse can store
_DATE_MIN, _DATE_MAX = date(1900, 1, 1), date(2299, 12, 31)                    # Date32
_DT_MIN, _DT_MAX = datetime(1970, 1, 1), datetime(2106, 2, 7, 6, 28, 15)       # DateTime

_TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
_DATE_FORMAT = "%Y-%m-%d"


def _base(trino_type: str) -> str:
    return trino_type.strip().lower().split("(")[0].split(" ")[0]


def trino_type_to_clickhouse(trino_type: str, nullable: bool = True) -> str:
    t = trino_type.strip().lower()
    m = _DECIMAL_RE.match(t)
    if m:
        base_type = f"Decimal({m.group(1)}, {m.group(2)})"
    else:
        base_type = SIMPLE_TYPE_MAP.get(_base(t), "String")
    return f"Nullable({base_type})" if nullable else base_type


def build_clickhouse_schema(description, non_nullable_cols=None):
    """description [(name, trino_type)] -> [(name, clickhouse_type)] in source order."""
    non_nullable_cols = set(non_nullable_cols or [])
    return [(name, trino_type_to_clickhouse(ttype, name not in non_nullable_cols))
            for name, ttype in description]


def _parse_timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    s = str(value)
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    log.warning("Could not parse timestamp value %r - inserting None", value)
    return None


def _parse_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), _DATE_FORMAT).date()
    except ValueError:
        log.warning("Could not parse date value %r - inserting None", value)
        return None


def _parse_decimal(value):
    if value is None or isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation:
        log.warning("Could not parse decimal value %r - inserting None", value)
        return None


def _to_str(value):
    return None if value is None else (value if isinstance(value, str) else str(value))


def _identity(value):
    return value


def _range_guard(name, parse, lo, hi, stats, non_nullable):
    """Wrap a parser: out-of-range values become None (counted in `stats`)."""
    def conv(value):
        v = parse(value)
        if v is None:
            return None
        cmp = v.replace(tzinfo=None) if isinstance(v, datetime) and v.tzinfo else v
        if cmp < lo or cmp > hi:
            if name in non_nullable:
                raise ValueError(f"Column {name!r}: value {value!r} is outside the range ClickHouse "
                                 f"supports and the column is non-nullable.")
            s = stats.setdefault(name, {"count": 0, "examples": []})
            s["count"] += 1
            if len(s["examples"]) < 3:
                s["examples"].append(str(value))
            return None
        return v
    return conv


def build_converters(description, stats=None, non_nullable=None):
    """One converter per column, chosen from the Trino type.
    Out-of-range dates/timestamps are counted into `stats` (a dict you pass in)."""
    stats = {} if stats is None else stats
    non_nullable = set(non_nullable or [])
    converters = []
    for name, ttype in description:
        base = _base(ttype)
        if base == "timestamp":
            converters.append(_range_guard(name, _parse_timestamp, _DT_MIN, _DT_MAX, stats, non_nullable))
        elif base == "date":
            converters.append(_range_guard(name, _parse_date, _DATE_MIN, _DATE_MAX, stats, non_nullable))
        elif base == "decimal":
            converters.append(_parse_decimal)
        elif base in SIMPLE_TYPE_MAP:
            converters.append(_identity)
        else:                       # unmapped type -> String column
            converters.append(_to_str)
    return converters


def convert_rows(rows, converters):
    return [tuple(c(v) for c, v in zip(converters, row)) for row in rows]
