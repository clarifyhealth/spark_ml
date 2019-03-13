import argparse

from ml.common.builder import PipelineBuilder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import Row, SparkSession


def build_model(source_df, config_df):
    """

    Args:
        source_df:
        config_df:

    Returns:

    """
    config_dict = config_df.asDict()
    pipeline_builder = PipelineBuilder(source_df, config_dict)
    target_df = pipeline_builder.transform()
    (training_data, test_data) = target_df.randomSplit(config_dict['randomSplit'], seed=config_dict['seed'])
    # Create initial LogisticRegression model
    lr = LogisticRegression(labelCol="label", featuresCol="features")
    # Create ParamGrid for Cross Validation
    param_grid = (ParamGridBuilder()
                  .addGrid(lr.regParam, config_dict['regParam'])
                  .addGrid(lr.elasticNetParam, config_dict['elasticNetParam'])
                  .addGrid(lr.maxIter, config_dict['maxIter'])
                  .build())
    # Evaluate model
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    evaluator.setMetricName(config_dict['metricName'])
    # Create K-fold CrossValidator
    cv = CrossValidator(estimator=lr, estimatorParamMaps=param_grid, evaluator=evaluator,
                        numFolds=config_dict["numFolds"])
    # Run cross validations
    cv_model = cv.fit(training_data)
    # Use test set here so we can measure the accuracy of our model on new data
    test_predictions = cv_model.transform(test_data)
    evaluator.evaluate(test_predictions)
    # Extract weights
    coefficients = cv_model.bestModel.coefficients
    weights = []
    for index, feature in enumerate(pipeline_builder.features()):
        weights.append(Row(feature=feature, weight=float(coefficients[index]), intercept=cv_model.bestModel.intercept))
    return cv_model, weights, test_predictions


def main(main_args):
    """

    Args:
        main_args:

    Returns:

    """
    parser = argparse.ArgumentParser(description="train spark pipeline for simple logistic regression")
    parser.add_argument("--source", required=True, help="s3 source url")
    parser.add_argument("--config", required=True, help="s3 source url")
    parser.add_argument("--target", required=True, help="s3 target url")
    args = parser.parse_args(main_args)

    spark = SparkSession.builder.appName("lg_train").getOrCreate()

    target_path = args.target.rstrip("/")

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)

    logger.info("start reading source data")

    source_df = spark.read.load(args.source)

    config_df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(args.config).first()
    cv_model, weights, test_predictions = build_model(source_df, config_df)

    cv_model.save(target_path + "/cv_lr_model/")
    test_predictions.write.save(target_path + "/test_predictions/")

    weights_df = spark.createDataFrame(weights)
    weights_df.write.save(target_path + "/weights/")

    logger.info("done write parquet data")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
