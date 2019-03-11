import os

import pytest

from ml.lr import score
from ml.lr import train
from .common import DATA_DIR

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")


def test_train_score_model(spark_session):
    config = os.path.join(DATA_DIR, 'param.json')
    source = os.path.join(DATA_DIR, 'churn-bigml.csv')
    test = os.path.join(DATA_DIR, 'churn-bigml-valid.csv')

    """
        Load Config
    """
    config_df = spark_session.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(config).first()

    """
        Assert the parameters in config
    """
    assert config_df['seed'] == 100
    assert config_df['numFolds'] == 2

    """
       Load data and do 80% train / 20% test split as per config.
    """
    source_df = spark_session.read.option("header", "true").option("inferSchema", "true").csv(source)

    """
        Assert the total count
    """
    assert source_df.count() == 3322

    """
        Build the model
    """
    model, weights, test_score = train.build_model(source_df, config_df)

    """
        Assert the test count
    """
    assert test_score.count() == 671

    """
        Load Test data
    """
    test_df = spark_session.read.option("header", "true").option("inferSchema", "true").csv(test)

    predictions_df = score.run_job(test_df, config_df, model)

    """
        Assert predictions count
    """
    assert predictions_df.count() == 11
