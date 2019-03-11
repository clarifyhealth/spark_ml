import pytest

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")


def test_basic(spark_session):
    """Test a parallelize & collect."""
    input = ["hello world"]
    rdd = spark_session.sparkContext.parallelize(input)
    result = rdd.collect()
    assert result == input
