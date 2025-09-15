from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from src.transforms import normalize_text_columns

def test_normalize_text_columns(spark):
    rows = [
        Row(customer_id="1", customer_city="  São Paulo ", customer_state=" SP "),
        Row(customer_id="2", customer_city="Rio", customer_state="RJ")
    ]
    df = spark.createDataFrame(rows)

    out = normalize_text_columns(df, ["customer_city", "customer_state"])

    # expected rows
    expected = [
        Row(customer_id="1", customer_city="são paulo", customer_state="sp"),
        Row(customer_id="2", customer_city="rio", customer_state="rj"),
    ]
    expected_df = spark.createDataFrame(expected)

    # Use chispa assert to ignore row order, metadata differences etc
    assert_df_equality(out, expected_df, ignore_row_order=True, ignore_column_order=True)
