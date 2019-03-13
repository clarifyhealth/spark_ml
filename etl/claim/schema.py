from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

header = [StructField("fac", StringType(), True),
          StructField("dkey", StringType(), True)]

tail = [StructField("source_code", StringType(), True),
        StructField("source_year", StringType(), True),
        StructField("source_qtr", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_file_date", TimestampType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("reload", StringType(), True)]

# tha_patient_registration schema

tha_pat_reg_schema = StructType(header + [
    StructField("fiscalyear", IntegerType(), True),
    StructField("hospstateabbr", StringType(), True),
    StructField("patstabbr", StringType(), True),
    StructField("patcnty", StringType(), True),
    StructField("patzip", StringType(), True),
    StructField("patzipext", StringType(), True),
    StructField("patcont", StringType(), True),
    StructField("mrn", StringType(), True),
    StructField("aged", IntegerType(), True),
    StructField("agem", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("race", StringType(), True),
    StructField("asource", StringType(), True),
    StructField("atype", StringType(), True),
    StructField("ddat", TimestampType(), True),
    StructField("pstat", IntegerType(), True),
    StructField("mdc", IntegerType(), True),
    StructField("drg", IntegerType(), True),
    StructField("servline", IntegerType(), True),
    StructField("los", IntegerType(), True),
    StructField("acharge", DoubleType(), True),
    StructField("asourcetype", StringType(), True),
    StructField("ppayercode", StringType(), True),
    StructField("pphysdocid", StringType(), True),
    StructField("ppx", StringType(), True),
    StructField("pdx", StringType(), True),
    StructField("auditflag", IntegerType(), True),
    StructField("birthwt", IntegerType(), True),
    StructField("adat", TimestampType(), True),
    StructField("adx", StringType(), True),
    StructField("orflag", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("erflag", StringType(), True),
    StructField("pcpx", StringType(), True),
    StructField("billtype", StringType(), True),
    StructField("princpx", StringType(), True),
    StructField("rehab_flag", StringType(), True),
    StructField("admssn_hour", IntegerType(), True),
    StructField("dischrg_hour", IntegerType(), True)
] + tail)

# tha_upk schema

tha_upk_schema = StructType([
    StructField("dkey", StringType(), True),
    StructField("patssn", StringType(), True),
    StructField("patlname", StringType(), True),
    StructField("patfname", StringType(), True),
    StructField("patdob", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("patzip", StringType(), True),
    StructField("column_7", StringType(), True),
    StructField("column_8", StringType(), True),
    StructField("column_9", StringType(), True)
])

# tha_hospital_ref schema

tha_hospital_ref_schema = StructType([
                                         StructField("fac", StringType(), True),
                                         StructField("hospcnty", StringType(), True),
                                         StructField("hospname", StringType(), True),
                                         StructField("hospstateabbr", StringType(), True),
                                         StructField("address_line_1", StringType(), True),
                                         StructField("address_line_2", StringType(), True),
                                         StructField("city", StringType(), True),
                                         StructField("zip", StringType(), True)
                                     ] + tail)

# tha_charges schema
tha_charges_schema = StructType(header + [StructField("expchrg", DoubleType(), True),
                                          StructField("routchrg", DoubleType(), True),
                                          StructField("icuchrg", DoubleType(), True),
                                          StructField("surgchrg", DoubleType(), True),
                                          StructField("labchrg", DoubleType(), True),
                                          StructField("pharmchrg", DoubleType(), True),
                                          StructField("radchrg", DoubleType(), True),
                                          StructField("respchrg", DoubleType(), True),
                                          StructField("therachrg", DoubleType(), True),
                                          StructField("supchrg", DoubleType(), True),
                                          StructField("othchrg", DoubleType(), True),
                                          StructField("tchrg", DoubleType(), True),
                                          ] + tail)
