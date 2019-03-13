import pyspark.sql.functions as F
from pyspark.sql.types import StringType


@F.udf(returnType=StringType())
def claim_type_code(source_cd):
    if source_cd == "IP":
        type_cd = "60"
    elif source_cd == "OP":
        type_cd = "40"
    else:
        type_cd = None
    return type_cd


@F.udf(returnType=StringType())
def claim_type_txt(source_cd):
    if source_cd == "IP":
        type_cd = "Inpatient"
    elif source_cd == "OP":
        type_cd = "Outpatient"
    else:
        type_cd = None
    return type_cd


@F.udf(returnType=StringType())
def organization_state_cd(state):
    if state == "TX":
        return "45"
    else:
        return None


@F.udf(returnType=StringType())
def admit_type_cd(admit_type):
    one_to_five = ["0", "1", "2", "3", "4", "5"]
    if admit_type in one_to_five:
        return admit_type
    else:
        return "9"


@F.udf(returnType=StringType())
def admission_referral_source_cd(referral_source_cd):
    possible_codes = {"0": "0", "1": "1", "2": "2", "3": "3", "4": "4", "5": "5", "6": "6", "7": "7", "8": "8",
                      "9": "9", "A": "A", "B": "B", "C": "C", "D": "D"}

    return possible_codes.get(referral_source_cd, "9")
