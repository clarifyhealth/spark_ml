""" pytest fixtures that can be resued across tests. the filename needs to be conftest.py
"""
import logging

# make sure env variables are set correctly
import findspark  # this needs to be the first import
import pytest

from pyspark.sql import SparkSession

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
    session = SparkSession.builder.appName("pytest-pyspark-local-testing").master(
        "local[2]").enableHiveSupport().getOrCreate()
    request.addfinalizer(lambda: session.stop())

    quiet_py4j()
    return session
