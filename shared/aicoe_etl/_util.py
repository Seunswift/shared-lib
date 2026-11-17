"""Small shared helpers (SQL cleaning, quoting, DataFrame output)."""
import re

_LEADING_COMMENTS = re.compile(r"^(?:\s*(?:--[^\n]*(?:\n|$)|/\*.*?\*/))+", re.S)


def clean_sql(sql: str) -> str:
    """Strip leading comments, whitespace and trailing ';'."""
    if not sql or not sql.strip():
        raise ValueError("Empty query.")
    out = _LEADING_COMMENTS.sub("", sql).strip().rstrip(";").strip()
    if not out:
        raise ValueError("Empty query.")
    return out


def sql_literal(value: str) -> str:
    """Quote a value as a SQL string literal (safe for Trino and ClickHouse)."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def split_identifier(name: str):
    """'a.b.c' or '"a"."b"' -> ['a', 'b', 'c'] (lower-cased, quotes removed)."""
    parts = re.findall(r'"((?:[^"]|"")+)"|`([^`]+)`|([A-Za-z0-9_$@\-]+)', name)
    return [(q.replace('""', '"') or b or p) for q, b, p in parts]


def to_output(rows, cols, output: str):
    """output='df' -> pandas DataFrame ; output='rows' -> (rows, cols)."""
    if output == "rows":
        return rows, cols
    if output != "df":
        raise ValueError("output must be 'df' or 'rows'.")
    try:
        import pandas as pd
    except ImportError as e:  # pragma: no cover
        raise ImportError("pandas is required for output='df' (pip install pandas), "
                          "or pass output='rows'.") from e
    return pd.DataFrame([list(r) for r in rows], columns=cols)
