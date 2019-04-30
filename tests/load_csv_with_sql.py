import os

from .common import DATA_DIR


def test_load_csv(spark_session):
    location = os.path.join(DATA_DIR, 'churn-bigml.csv')

    my_sql = f"""
     CREATE TEMPORARY VIEW churn (
     key integer  ,
     state string  ,
     account_length integer ,
     area_code integer ,
     international_plan string ,
     voice_mail_plan string,
     number_vmail_messages integer ,
     total_day_minutes double ,
     total_day_calls integer ,
     total_day_charge double ,
     total_eve_minutes double ,
     total_eve_calls integer ,
     total_eve_charge double ,
     total_night_minutes double ,
     total_night_calls integer,
     total_night_charge double ,
     total_intl_minutes double ,
     total_intl_calls integer ,
     total_intl_charge double ,
     customer_service_calls integer, 
     churn string )
     USING csv OPTIONS (header true, path '{location}')
     """

    spark_session.sql(my_sql)

    df = spark_session.sql("select * from churn")

    df.show()
