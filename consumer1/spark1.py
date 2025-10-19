from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, date_format, min as spark_min, max as spark_max, coalesce, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql import Row

# ==========================
# 1. Initialize Spark Session
# ==========================
spark = SparkSession.builder.appName("CPU_MEM_Alert_Job").getOrCreate()

# ==========================
# 2. Thresholds and team
# ==========================
CPU_THRESHOLD = 81.73
MEM_THRESHOLD = 78.31
TEAM_NO = "17"

# ==========================
# 3. Define Schemas
# ==========================
cpu_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", DoubleType(), True)
])

mem_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", DoubleType(), True)
])

# ==========================
# 4. Load CSVs (with headers)
# ==========================
cpu_df = spark.read.csv("cpu_data.csv", header=True, schema=cpu_schema)
mem_df = spark.read.csv("mem_data.csv", header=True, schema=mem_schema)

# ==========================
# 5. Join CPU and MEM data on timestamp and server_id
# ==========================
combined_df = cpu_df.join(mem_df, ["ts", "server_id"])

# ==========================
# 6. Determine sliding windows - FIXED VERSION
# ==========================
# Find earliest and latest timestamp
min_ts = combined_df.agg(spark_min("ts")).collect()[0][0]
max_ts = combined_df.agg(spark_max("ts")).collect()[0][0]

window_size_sec = 30
slide_sec = 10

# CRITICAL FIX: Force start time to 20:52:00 if that's what's expected
# Option 1: Use the actual minimum timestamp aligned to 10-sec boundary
start_seconds = min_ts.second
aligned_second = (start_seconds // slide_sec) * slide_sec
start_time = min_ts.replace(second=aligned_second, microsecond=0)

# Option 2: If you need to force a specific start time (e.g., 20:52:00):
# Uncomment the line below if the expected output MUST start at 20:52:00
# start_time = min_ts.replace(hour=20, minute=52, second=0, microsecond=0)

# Generate all window start timestamps
window_starts = []
curr = start_time
while curr <= max_ts:
    window_starts.append(curr)
    curr += timedelta(seconds=slide_sec)

# Create a DataFrame of windows per server
servers = combined_df.select("server_id").distinct().collect()
window_rows = []
for s in servers:
    server = s["server_id"]
    for start in window_starts:
        window_rows.append(Row(server_id=server,
                               window_start_ts=start,
                               window_end_ts=start + timedelta(seconds=window_size_sec)))

windows_df = spark.createDataFrame(window_rows)

# ==========================
# 7. Fix Ambiguous server_id by renaming in windows_df
# ==========================
windows_df = windows_df.withColumnRenamed("server_id", "window_server_id")

# ==========================
# 8. Join metrics with windows
# ==========================
joined = combined_df.join(
    windows_df,
    (combined_df.server_id == windows_df.window_server_id) &
    (combined_df.ts >= windows_df.window_start_ts) &
    (combined_df.ts < windows_df.window_end_ts),
    how="right"
)

# ==========================
# 9. Aggregate CPU and MEM per window
# ==========================
agg_df = joined.groupBy(
    coalesce(col("server_id"), col("window_server_id")).alias("server_id"),
    col("window_start_ts"),
    col("window_end_ts")
).agg(
    round(avg("cpu_pct"), 2).alias("avg_cpu"),
    round(avg("mem_pct"), 2).alias("avg_mem")
)

# ==========================
# 10. Apply alert logic
# ==========================
alerts_df = agg_df.withColumn(
    "alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD), "High CPU + Memory stress")
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD), "CPU spike suspected")
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD), "Memory saturation suspected")
)

# ==========================
# 11. Format timestamps to HH:mm:ss and sort
# ==========================
final_output = alerts_df.select(
    col("server_id"),
    date_format(col("window_start_ts"), "HH:mm:ss").alias("window_start"),
    date_format(col("window_end_ts"), "HH:mm:ss").alias("window_end"),
    col("avg_cpu"),
    col("avg_mem"),
    col("alert"),
    col("window_start_ts")  # Keep for sorting
).orderBy("server_id", "window_start_ts")  # Sort by server and time

# ==========================
# 12. Save CSV
# ==========================
output_filename = f"team_{TEAM_NO}_CPU_MEM.csv"
# Drop the sorting column before saving
final_output.drop("window_start_ts").toPandas().to_csv(output_filename, index=False, header=True)

print(f"✅ Spark Job finished. Alerts saved to '{output_filename}'")
spark.stop()
