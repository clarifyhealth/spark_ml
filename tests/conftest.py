import logging
from pathlib import Path

import findspark  # this needs to be the first import
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

findspark.init()


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    lib_dir = Path(__file__).parent.joinpath('udf/')

    session = SparkSession.builder.appName("pytest-pyspark-local-testing") \
        .master("local[2]") \
        .config("spark.jars", f"file://{lib_dir.joinpath('spark-demo-assembly-0.1.jar')}") \
        .enableHiveSupport().getOrCreate()
    session.udf.registerJavaFunction("word_count", "com.clarify.WordCount", LongType())

    request.addfinalizer(lambda: session.stop())

    quiet_py4j()
    return session
