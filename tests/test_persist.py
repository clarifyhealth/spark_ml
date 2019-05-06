import pytest

from etl.custom.persist import StrToDoubleTransformer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import Row


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="1.0"),
                                         Row(input_col="2.0"),
                                         Row(input_col="3"),
                                         Row(input_col="4"),
                                         Row(input_col="6"),
                                         Row(input_col="7")
                                     ]
                             )

                         ])
def test_py_udf_transformer(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    tr = StrToDoubleTransformer(inputCol="input_col", outputCol="output_col")

    pipeline = Pipeline(stages=[tr])

    pipeline_model = pipeline.fit(df)

    pipeline_model.save("/Users/alvin/persist/test/")

    same_model = PipelineModel.load("/Users/alvin/persist/test/")

    tdf = same_model.transform(df)

    tdf.explain()

    tdf.show()

    assert tdf.count() == 6
