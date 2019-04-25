import pytest

from etl.udf.utils import example_py_udf, example_scala_udf, example_vectorized_udf, example_built_in_udf
from pyspark.sql import Row
from etl.chain.dataframe_ext import transform


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="Hello this is my favourite test"),
                                         Row(input_col="This is cool"),
                                         Row(input_col="Time for some performance test"),
                                         Row(input_col="Clarify Tera Team"),
                                         Row(input_col="Doing things right and doing the right thing"),
                                         Row(input_col="Oh Model fit and predict")
                                     ]
                             )

                         ])
def test_chain_transforms(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    target_df = (df
                 .transform(lambda df_1: example_py_udf(df_1, "output_col_1", df_1["input_col"]))
                 .transform(lambda df_2: example_scala_udf(df_2, "output_col_2", "input_col"))
                 .transform(lambda df_3: example_vectorized_udf(df_3, "output_col_3", df_3["input_col"]))
                 .transform(lambda df_4: example_built_in_udf(df_4, "output_col_4", df_4["input_col"]))
                 )
    target_df.explain()

    target_df.show()

    assert target_df.count() == 6
