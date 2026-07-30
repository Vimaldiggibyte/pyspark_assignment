from pyspark.sql import SparkSession
from utils import *

spark = (
    SparkSession.builder
    .appName("Question3")
    .getOrCreate()
)

# --------------------------
# Create DataFrame
# --------------------------

login_df = create_login_df(spark)

print("Original Data")

login_df.show(truncate=False)

# --------------------------
# Convert Timestamp
# --------------------------

login_df = convert_to_login_date(login_df)

print("After Conversion")

login_df.show(truncate=False)

# --------------------------
# Write CSV
# --------------------------

write_csv(
    login_df,
    "/tmp/login_csv"
)

print("CSV Written Successfully")

# --------------------------
# Save Managed Table
# --------------------------

save_table(login_df)

print("Managed Table Created")

# --------------------------
# Actions in Last 7 Days
# --------------------------

result = actions_last_7_days(login_df)

result.show()

spark.stop()