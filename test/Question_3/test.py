from pyspark.sql import SparkSession

from src.Question_3.utils import (
    create_login_df,
    convert_to_login_date,
    write_csv,
    save_table,
    actions_last_7_days
)

spark = SparkSession.getActiveSession()

# ---------------------------------------
# Test 1 : DataFrame Creation
# ---------------------------------------

df = create_login_df(spark)

assert df.count() == 8

print("✓ DataFrame Test Passed")


# ---------------------------------------
# Test 2 : Column Names
# ---------------------------------------

expected_columns = [
    "log_id",
    "user_id",
    "user_activity",
    "time_stamp"
]

assert df.columns == expected_columns

print("✓ Column Name Test Passed")


# ---------------------------------------
# Test 3 : login_date Column
# ---------------------------------------

df2 = convert_to_login_date(df)

assert "login_date" in df2.columns

print("✓ Login Date Test Passed")


# ---------------------------------------
# Test 4 : Last 7 Days Actions
# ---------------------------------------

result = actions_last_7_days(df)

assert result.count() == 3

print("✓ Last 7 Days Query Test Passed")


# ---------------------------------------
# Test 5 : CSV Write
# ---------------------------------------

write_csv(df2)

print("✓ CSV Write Test Passed")


# ---------------------------------------
# Test 6 : Managed Table
# ---------------------------------------

save_table(spark, df2)

tables = spark.sql("SHOW TABLES IN user")

assert tables.filter(
    "tableName='login_details'"
).count() == 1

print("✓ Managed Table Test Passed")


print("\n🎉 All Tests Passed Successfully!")