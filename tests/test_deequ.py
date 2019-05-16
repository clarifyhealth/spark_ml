import pytest

from pyspark.sql import Row
from validation.utils import VerificationSuite, Check


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(id=1, name="Thingy A", description="awesome thing.", priority="high",
                                             numViews=0),
                                         Row(id=2, name="Thingy B", description="available at http://thingb.com",
                                             priority=None, numViews=0),
                                         Row(id=3, name=None, description=None, priority="low", numViews=5),
                                         Row(id=4, name="Thingy D", description="checkout https://thingd.ca",
                                             priority="low", numViews=10),
                                         Row(id=5, name="Thingy E", description=None, priority="high", numViews=12)
                                     ]
                             )

                         ])
def test_verify(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    verification_suite = VerificationSuite()

    check = Check().hasSize("_ == 5").isComplete("id").isUnique("id")

    result = verification_suite.onData(df).addCheck(check).run()

    assert result is not None
