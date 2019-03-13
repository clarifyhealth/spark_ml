from etl.claim.custom_udf import claim_type_code, claim_type_txt, organization_state_cd, admit_type_cd, \
    admission_referral_source_cd
from pyspark.sql import functions as F


def handle_patient_registration(source_df):
    """

    :param source_df:
    :return:
    """
    pat_reg_df = source_df.withColumn("claim_id", F.md5(F.concat(F.lit("tha"),
                                                                 F.coalesce(source_df.fac, F.lit("")),
                                                                 F.coalesce(source_df.dkey, F.lit("")),
                                                                 F.coalesce(source_df.source_code, F.lit("")),
                                                                 F.coalesce(source_df.source_year, F.lit("")),
                                                                 F.coalesce(source_df.source_qtr, F.lit(""))
                                                                 )))

    pat_reg_final_df = pat_reg_df.withColumn("claim_type_cd", claim_type_code(pat_reg_df.source_code)) \
        .withColumn("claim_type_txt", claim_type_txt(pat_reg_df.source_code)) \
        .withColumn("claim_start_dt", pat_reg_df.adat) \
        .withColumn("claim_end_dt", pat_reg_df.ddat) \
        .withColumnRenamed("pdx", "primary_diagnosis_cd") \
        .withColumn("primary_diagnosis_code_type_cd", F.lit("icd10")) \
        .withColumnRenamed("pphysdocid", "attending_provider_npi") \
        .withColumn("organization_state_cd", organization_state_cd(pat_reg_df.hospstateabbr)) \
        .withColumn("utilization_day_cnt", pat_reg_df.los) \
        .withColumn("admit_dt", pat_reg_df.adat) \
        .withColumn("admit_type_cd", admit_type_cd(pat_reg_df.atype)) \
        .withColumn("length_of_stay_val", pat_reg_df.los) \
        .withColumn("discharge_dt", pat_reg_df.ddat) \
        .withColumn("admission_referral_source_cd", admission_referral_source_cd(pat_reg_df.asource)) \
        .withColumnRenamed("drg", "drg_cd") \
        .select("fac", "dkey", "claim_id", "claim_type_cd", "claim_type_txt", "claim_start_dt", "claim_end_dt",
                "primary_diagnosis_cd", "primary_diagnosis_code_type_cd", "attending_provider_npi",
                "organization_state_cd", "utilization_day_cnt", "admit_dt", "admit_type_cd", "length_of_stay_val",
                "discharge_dt", "admission_referral_source_cd", "drg_cd", "source_code", "source_year", "source_qtr"
                )

    return pat_reg_final_df


def extract_patient_id(pat_reg_df, tha_upk_df):
    """

    :param pat_reg_df:
    :param tha_upk_df:
    :return:
    """
    exclude = []
    select_col = [F.col(c) for c in pat_reg_df.columns if c not in exclude] + [
        F.concat(F.col("column_7"), F.col("column_8")).alias("patient_id")]

    result_df = pat_reg_df.join(tha_upk_df, ["dkey"]).select(select_col)

    return result_df


def extract_organization_nm(pat_reg_df, tha_hospital_ref_df):
    """

    :param pat_reg_df:
    :param tha_hospital_ref_df:
    :return:
    """
    exclude = ["source_file", "source_file_date", "load_date", "reload"]

    select_col = [F.col(c) for c in pat_reg_df.columns if c not in exclude] + [
        F.col("hospname").alias("organization_nm")]

    result_df = pat_reg_df.join(tha_hospital_ref_df, ["fac", "source_code", "source_year", "source_qtr"]).select(
        select_col)

    return result_df


def extract_total_payment_amount(pat_reg_df, tha_charges_df):
    """

    :param pat_reg_df:
    :param tha_charges_df:
    :return:
    """
    exclude = ["source_file", "source_file_date", "load_date", "reload"]

    select_col = [F.col(c) for c in pat_reg_df.columns if c not in exclude] + [
        F.col("tchrg").alias("total_payment_amt")]

    result_df = pat_reg_df.join(tha_charges_df, ["fac", "dkey", "source_code", "source_year", "source_qtr"]).select(
        select_col)

    return result_df
