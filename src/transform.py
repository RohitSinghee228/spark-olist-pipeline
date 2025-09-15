from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim

def normalize_text_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Example simple transformation: trim + lower on given columns
    Keeps schema and other columns unchanged.
    """
    out = df
    for c in cols:
        out = out.withColumn(c, trim(lower(col(c))))
    return out
