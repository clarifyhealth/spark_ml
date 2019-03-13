import pytest

from etl.claim.custom_udf import claim_type_code, claim_type_txt
from pyspark.sql import Row


@pytest.mark.parametrize('input_data, expected_val , expected_col', [
    ([Row(source_cd="IP"),
      Row(source_cd="OP")],
     ["60", "40"],
     ["source_cd", "claim_type_cd"])
])
def test_claim_type_code(spark_session, input_data, expected_val, expected_col):
    df = spark_session.createDataFrame(input_data)

    result_df = df.withColumn("claim_type_cd", claim_type_code(df.source_cd))

    assert result_df.count() == 2

    assert result_df.columns == expected_col

    assert [row.claim_type_cd for row in result_df.collect()] == expected_val


@pytest.mark.parametrize('input_data, expected_val , expected_col', [
    ([Row(source_cd="IP"),
      Row(source_cd="OP")],
     ["Inpatient", "Outpatient"],
     ["source_cd", "claim_type_txt"])
])
def test_claim_type_txt(spark_session, input_data, expected_val, expected_col):
    df = spark_session.createDataFrame(input_data)

    result_df = df.withColumn("claim_type_txt", claim_type_txt(df.source_cd))

    assert result_df.count() == 2

    assert result_df.columns == expected_col

    assert [row.claim_type_txt for row in result_df.collect()] == expected_val
