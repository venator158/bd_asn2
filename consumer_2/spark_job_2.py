# spark_job_2.py (Final Corrected Version)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, when, date_format, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("NetworkDiskAnalysis").getOrCreate()

# 2. Use the thresholds from your teammate's script
NET_IN_THRESHOLD = 5534.73
DISK_IO_THRESHOLD = 1737.83
TEAM_NO = "17"

# 3. Define Schemas to correctly read timestamps and numbers
net_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("net_in", DoubleType(), True),
    StructField("net_out", DoubleType(), True)
])

disk_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("disk_io", DoubleType(), True)
])

# 4. Load your local CSV data (no headers)
net_df = spark.read.csv("net_data.csv", header=False, schema=net_schema)
disk_df = spark.read.csv("disk_data.csv", header=False, schema=disk_schema)

# 5. Join the dataframes on timestamp and server_id
combined_df = net_df.join(disk_df, ["ts", "server_id"])

# 6. Perform the required Window-based Aggregation
windowed_agg = combined_df.groupBy(
    window(col("ts"), "30 seconds", "10 seconds"),
    col("server_id")
).agg(
    spark_round(max("net_in"), 2).alias("max_net_in"),
    spark_round(max("disk_io"), 2).alias("max_disk_io")
)

# 7. Apply your specific alerting logic
alerts_df = windowed_agg.withColumn("alert",
    when((col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") > DISK_IO_THRESHOLD), "Network flood + Disk thrash suspected")
    .when((col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") <= DISK_IO_THRESHOLD), "Possible DDoS")
    .when((col("max_disk_io") > DISK_IO_THRESHOLD) & (col("max_net_in") <= DISK_IO_THRESHOLD), "Disk thrash suspected")
    .otherwise("Normal")
)

# 8. Format the output to exactly match the assignment requirements
final_output = alerts_df.select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
    col("alert")
)
# ✅ --- CRITICAL FIX: The .filter() line has been REMOVED ---
# This will include all 14,400 rows ("Normal" and "Alert")

# 9. Save the final report to a SINGLE CSV file
output_filename = f"team_{TEAM_NO}_NET_DISK.csv" # Note the .csv extension

# Convert to Pandas DataFrame to save as a single file
final_output_pandas = final_output.toPandas()

# Save using pandas (this creates one file, not a folder)
final_output_pandas.to_csv(output_filename, index=False, header=True)

print(f"✅ Spark Job 2 finished. Alerts saved to the file: '{output_filename}'")
spark.stop()
