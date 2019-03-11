import argparse

from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import SparkSession

from ml.common.builder import PipelineBuilder


def run_job(source_df, config_df, cv_model):
    config_dict = config_df.asDict()
    pipeline_builder = PipelineBuilder(source_df, config_dict)
    score_df = pipeline_builder.transform()
    score_predictions = cv_model.transform(score_df)
    return score_predictions


def main(main_args):
    """

    :param main_args:
    :return:
    """
    parser = argparse.ArgumentParser(description="train spark pipeline for simple logistic regression")
    parser.add_argument("--source", required=True, help="s3 source url")
    parser.add_argument("--config", required=True, help="s3 source url")
    parser.add_argument("--model", required=True, help="s3 source url")
    parser.add_argument("--target", required=True, help="s3 target url")
    args = parser.parse_args(main_args)

    spark = SparkSession.builder.appName("lg_score").getOrCreate()

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)

    logger.info("start reading source data")

    source_df = spark.read.load(args.source)
    config_df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(args.config).first()
    cv_model = CrossValidatorModel.load(args.model)

    score_predictions = run_job(source_df, config_df, cv_model)

    score_predictions.write.save(args.target)

    logger.info("done write parquet data")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
