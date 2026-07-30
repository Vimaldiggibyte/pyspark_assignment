from pyspark.sql.types import *
from pyspark.sql.functions import udf, col


# --------------------------
# Create DataFrame
# --------------------------
def create_credit_card_df(spark):

    schema = StructType([
        StructField("card_number", StringType(), True)
    ])

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    return spark.createDataFrame(data, schema)


# --------------------------
# Read using List
# --------------------------
def read_using_list(spark):

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    return spark.createDataFrame(
        data,
        ["card_number"]
    )


# --------------------------
# Read using RDD
# --------------------------
def read_using_rdd(spark):

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    rdd = spark.sparkContext.parallelize(data)

    return spark.createDataFrame(
        rdd,
        ["card_number"]
    )


# --------------------------
# Increase Partitions
# --------------------------
def increase_partitions(df):

    return df.repartition(5)


# --------------------------
# Restore Original Partitions
# --------------------------
def decrease_partitions(df, original):

    return df.coalesce(original)


# --------------------------
# UDF
# --------------------------
def mask_card(card):

    return "*" * 12 + card[-4:]


mask_udf = udf(mask_card, StringType())


# --------------------------
# Add Masked Column
# --------------------------
def mask_credit_cards(df):

    return df.withColumn(
        "masked_card_number",
        mask_udf(col("card_number"))
    )