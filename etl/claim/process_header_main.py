import argparse

from etl.claim.schema import tha_pat_reg_schema, tha_upk_schema, tha_hospital_ref_schema, tha_charges_schema
from etl.claim.transformations import handle_patient_registration, extract_patient_id, extract_organization_nm, \
    extract_total_payment_amount
from pyspark.sql import SparkSession


def run_job(spark, args, logger):
    """

    :param spark:
    :param args:
    :param logger:
    :return:
    """
    # Process tha_patient_registration
    logger.info("start processing source tha_patient_registration")

    pat_reg_df = spark.read.option("delimiter", "|").option("timestampFormat", "yyy-MM-dd HH:mm:ss.SSSSSS").schema(
        tha_pat_reg_schema).csv(args.tha_patient_registration)

    pat_reg_transformed_df = handle_patient_registration(pat_reg_df)

    logger.info("end processing source tha_patient_registration")

    # Process tha_upk for to extract patient id
    logger.info("start processing source tha_upk")

    tha_upk_df = spark.read.option("delimiter", "|").option("timestampFormat", "yyy-MM-dd HH:mm:ss.SSSSSS").schema(
        tha_upk_schema).csv(args.tha_upk)

    pat_id_df = extract_patient_id(pat_reg_transformed_df, tha_upk_df)

    logger.info("end processing source tha_upk")

    # Process tha_hospital_ref to extract organization_nm
    logger.info("start processing source tha_hospital_ref")

    tha_hospital_ref_df = spark.read.option("delimiter", "|").option("timestampFormat",
                                                                     "yyy-MM-dd HH:mm:ss.SSSSSS").schema(
        tha_hospital_ref_schema).csv(args.tha_hospital_ref)

    pat_org_nm_df = extract_organization_nm(pat_id_df, tha_hospital_ref_df)

    logger.info("end processing source tha_hospital_ref")

    # Process tha_charges to extract total_payment_amount
    logger.info("start processing source tha_charges")

    charges_df = spark.read.option("delimiter", "|").option("timestampFormat", "yyy-MM-dd HH:mm:ss.SSSSSS").schema(
        tha_charges_schema).csv(args.tha_charges)

    claim_header_df = extract_total_payment_amount(pat_org_nm_df, charges_df)
    
    logger.info("end processing source tha_charges")

    return claim_header_df


def parse_parameters(main_args):
    """

    :param main_args:
    :return:
    """
    parser = argparse.ArgumentParser(description="Claim Header ELT Pipeline")
    parser.add_argument("--tha_patient_registration", required=True, help="s3 source url")
    parser.add_argument("--tha_upk", required=True, help="s3 source url")
    parser.add_argument("--tha_charges", required=True, help="s3 source url")
    parser.add_argument("--tha_hospital_ref", required=True, help="s3 source url")
    parser.add_argument("--target", required=True, help="s3 target url")
    args = parser.parse_args(main_args)
    return args


def main(main_args):
    """

    :param main_args:
    :return:
    """
    args = parse_parameters(main_args)

    spark = SparkSession.builder.getOrCreate()

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)

    logger.info("Start pipeline")

    claim_header_df = run_job(args, logger, spark)

    logger.info(f"Start writing final output to {args.target}")

    claim_header_df.write.save(args.target)

    logger.info(f"Finish writing final output to {args.target}")

    logger.info("End pipeline")


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
