from pyspark.sql import SparkSession
import sys
import os

sys.path.append(os.path.dirname(__file__))

from utils import *


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Testing")
    .getOrCreate()
)

# --------------------------
# Test 1
# --------------------------

df = create_credit_card_df(spark)

assert df.count() == 5

print("✓ DataFrame Creation Passed")

# --------------------------
# Test 2
# --------------------------

original = df.rdd.getNumPartitions()

assert original >= 1

print("✓ Original Partition Test Passed")

# --------------------------
# Test 3
# --------------------------

df2 = increase_partitions(df)

assert df2.rdd.getNumPartitions() == 5

print("✓ Repartition Test Passed")

# --------------------------
# Test 4
# --------------------------

df3 = decrease_partitions(
    df2,
    original
)

assert df3.rdd.getNumPartitions() == original

print("✓ Coalesce Test Passed")

# --------------------------
# Test 5
# --------------------------

assert mask_card(
    "1234567891234567"
) == "************4567"

print("✓ UDF Test Passed")

# --------------------------
# Test 6
# --------------------------

masked = mask_credit_cards(df)

assert "masked_card_number" in masked.columns

print("✓ Masked Column Test Passed")

print("\n🎉 All Tests Passed!")

spark.stop()