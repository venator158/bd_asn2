# spark_job_2.py 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os


NET_CSV_FILE = 'net_data.csv'
DISK_CSV_FILE = 'disk_data.csv'
OUTPUT_FILE = 'team_17_NET_DISK.csv'

NET_IN_THRESHOLD = 5534.73
DISK_IO_THRESHOLD = 1737.83

# Initialize Spark
spark = SparkSession.builder \
    .appName("Consumer2-Clean") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schemas
net_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("net_in", DoubleType(), True),
    StructField("net_out", DoubleType(), True)
])

disk_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("disk_io", DoubleType(), True)
])

# Read data
net_df = spark.read.csv(NET_CSV_FILE, schema=net_schema, header=False)
disk_df = spark.read.csv(DISK_CSV_FILE, schema=disk_schema, header=False)

# Clean server_id
net_df = net_df.withColumn("server_id", lower(regexp_replace(col("server_id"), "\\s+", "")))
disk_df = disk_df.withColumn("server_id", lower(regexp_replace(col("server_id"), "\\s+", "")))

# Convert HH:mm:ss → seconds
def parse_time_to_seconds(ts_str):
    h, m, s = map(int, ts_str.split(':'))
    return h * 3600 + m * 60 + s

time_to_seconds_udf = udf(parse_time_to_seconds, IntegerType())
net_with_sec = net_df.withColumn("time_seconds", time_to_seconds_udf(col("ts")))
disk_with_sec = disk_df.withColumn("time_seconds", time_to_seconds_udf(col("ts")))

# Sliding windows (30s window, 10s slide)
net_windowed = net_with_sec.withColumn(
    "window_starts",
    array([floor((col("time_seconds") - lit(i * 10)) / 10) * 10 for i in range(3)])
).withColumn("window_start_sec", explode("window_starts")) \
 .filter((col("time_seconds") >= col("window_start_sec")) & 
         (col("time_seconds") < col("window_start_sec") + 30)) \
 .withColumn("window_end_sec", col("window_start_sec") + 30)

disk_windowed = disk_with_sec.withColumn(
    "window_starts",
    array([floor((col("time_seconds") - lit(i * 10)) / 10) * 10 for i in range(3)])
).withColumn("window_start_sec", explode("window_starts")) \
 .filter((col("time_seconds") >= col("window_start_sec")) & 
         (col("time_seconds") < col("window_start_sec") + 30)) \
 .withColumn("window_end_sec", col("window_start_sec") + 30)

# Aggregation
net_agg = net_windowed.groupBy("window_start_sec", "window_end_sec", "server_id").agg(
    max("net_in").alias("max_net_in"),
    avg("net_in").alias("avg_net_in"),
    max("net_out").alias("max_net_out"),
    avg("net_out").alias("avg_net_out")
)

disk_agg = disk_windowed.groupBy("window_start_sec", "window_end_sec", "server_id").agg(
    max("disk_io").alias("max_disk_io"),
    avg("disk_io").alias("avg_disk_io")
)

# Join Network + Disk
combined = net_agg.join(
    disk_agg,
    ["window_start_sec", "window_end_sec", "server_id"],
    "inner"
)

# Alert logic
with_alerts = combined.withColumn(
    "alert",
    when(
        (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") > DISK_IO_THRESHOLD),
        "Network flood + Disk thrash suspected"
    ).when(
        (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") <= DISK_IO_THRESHOLD),
        "Possible DDoS"
    ).when(
        (col("max_disk_io") > DISK_IO_THRESHOLD) & (col("max_net_in") <= NET_IN_THRESHOLD),
        "Disk thrash suspected"
    )
)

server_window_spec = Window.partitionBy("server_id").orderBy(col("window_start_sec"))
trimmed_alerts_df = with_alerts.withColumn("row_num", row_number().over(server_window_spec)) \
                               .filter(col("row_num") > 2)

def seconds_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

seconds_to_time_udf = udf(seconds_to_time, StringType())

# Final output schema
final_output = trimmed_alerts_df.withColumn(
    "window_start", seconds_to_time_udf(col("window_start_sec"))
).withColumn(
    "window_end", seconds_to_time_udf(col("window_end_sec"))
).select(
    "server_id",
    "window_start",
    "window_end",
    round(col("max_net_in"), 2).alias("max_net_in"),
    round(col("max_disk_io"), 2).alias("max_disk_io"),
    "alert"
).orderBy("server_id", "window_start")

total = final_output.count()
if total > 14400:
    final_output = final_output.limit(14400)

# Save output
temp_dir = "temp_output"
if os.path.exists(temp_dir):
    import shutil; shutil.rmtree(temp_dir)
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

final_output.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
if csv_files:
    import shutil
    shutil.copy(os.path.join(temp_dir, csv_files[0]), OUTPUT_FILE)
    shutil.rmtree(temp_dir)

spark.stop()

