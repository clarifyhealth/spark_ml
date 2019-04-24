import pytest

from etl.custom.transformers import MyWordCounterPyUDF, MyWordCounterScalaUDF, MyWordCounterVectorizedUDF, \
    MyWordCounterBuiltInUDF
from pyspark.ml import Pipeline
from pyspark.sql import Row


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
def test_pipeline_transforms(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)
    tr1 = MyWordCounterPyUDF(inputCol="input_col", outputCol="output_col_1")
    tr2 = MyWordCounterScalaUDF(inputCol="input_col", outputCol="output_col_2")
    tr3 = MyWordCounterVectorizedUDF(inputCol="input_col", outputCol="output_col_3")
    tr4 = MyWordCounterBuiltInUDF(inputCol="input_col", outputCol="output_col_4")

    pipeline = Pipeline(stages=[tr1, tr2, tr3, tr4])
    pipeline_model = pipeline.fit(df)
    target_df = pipeline_model.transform(df)

    target_df.explain()

    target_df.show()

    assert target_df.count() == 6


