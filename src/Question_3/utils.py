from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import (
    col,
    to_date,
    max,
    datediff,
    lit
)


# ---------------------------------------
# Create DataFrame
# ---------------------------------------
def create_login_df(spark):

    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_activity", StringType(), True),
        StructField("time_stamp", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


# ---------------------------------------
# Convert timestamp to login_date
# ---------------------------------------
def convert_to_login_date(df):

    return df.withColumn(
        "login_date",
        to_date(
            col("time_stamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )


# ---------------------------------------
# Write CSV
# ---------------------------------------
def write_csv(df):

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("nullValue", "NULL")
        .csv("dbfs:/FileStore/Question_3/login_csv")
    )


# ---------------------------------------
# Save Managed Table
# ---------------------------------------
def save_table(spark, df):

    spark.sql("CREATE SCHEMA IF NOT EXISTS user")

    (
        df.write
        .mode("overwrite")
        .saveAsTable("user.login_details")
    )


# ---------------------------------------
# Actions performed in last 7 days
# ---------------------------------------
def actions_last_7_days(df):

    latest_date = (
        df.select(
            max(
                to_date(col("time_stamp"))
            )
        ).first()[0]
    )

    return (
        df.withColumn(
            "login_date",
            to_date(col("time_stamp"))
        )
        .filter(
            datediff(
                lit(latest_date),
                col("login_date")
            ) < 7
        )
        .groupBy("user_id")
        .count()
        .withColumnRenamed(
            "count",
            "total_actions"
        )
    )