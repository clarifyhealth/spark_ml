from pyspark.sql.functions import expr, udf, pandas_udf, size, split
from pyspark.sql.types import LongType


@udf(returnType=LongType())
def word_count_udf(s):
    return len(s.split(" "))


@pandas_udf(returnType=LongType())
def word_count_pandas_udf(s):
    return s.str.split(" ").str.len()


def example_py_udf(df, out_col, in_col):
    return df.withColumn(out_col, word_count_udf(in_col))


def example_scala_udf(df, out_col, in_col):
    return df.withColumn(out_col, expr(f"word_count({in_col})"))


def example_vectorized_udf(df, out_col, in_col):
    return df.withColumn(out_col, word_count_pandas_udf(in_col))


def example_built_in_udf(df, out_col, in_col):
    return df.withColumn(out_col, size(split(in_col, " ")))
