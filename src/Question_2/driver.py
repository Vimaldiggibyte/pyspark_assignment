from pyspark.sql import SparkSession

from utils import *

spark = (
    SparkSession.builder
    .appName("Question2")
    .getOrCreate()
)

# -------------------------
# Create DataFrame
# -------------------------

credit_card_df = create_credit_card_df(spark)

print("Original Data")

credit_card_df.show(truncate=False)

# -------------------------
# Original Partitions
# -------------------------

original = credit_card_df.rdd.getNumPartitions()

print("Original Partitions :", original)

# -------------------------
# Increase
# -------------------------

credit_card_df = increase_partitions(
    credit_card_df
)

print(
    "Partitions after repartition :",
    credit_card_df.rdd.getNumPartitions()
)

# -------------------------
# Decrease
# -------------------------

credit_card_df = decrease_partitions(
    credit_card_df,
    original
)

print(
    "Partitions after coalesce :",
    credit_card_df.rdd.getNumPartitions()
)

# -------------------------
# Mask Cards
# -------------------------

masked_df = mask_credit_cards(
    credit_card_df
)

masked_df.show(truncate=False)

spark.stop()