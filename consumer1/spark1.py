from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, date_format, min as spark_min, max as spark_max, coalesce, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql import Row


spark = SparkSession.builder.appName("CPU_MEM_Alert_Job").getOrCreate()


CPU_THRESHOLD = 81.73
MEM_THRESHOLD = 78.31
TEAM_NO = "17"


cpu_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", FloatType(), True)
])

mem_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", FloatType(), True)
])


cpu_df = spark.read.csv("cpu_data.csv", header=False, schema=cpu_schema)
mem_df = spark.read.csv("mem_data.csv", header=False, schema=mem_schema)

# Cast cpu_pct and mem_pct from double to float with 2 decimal precision
cpu_df = cpu_df.withColumn("cpu_pct", round(col("cpu_pct").cast(FloatType()), 2))
mem_df = mem_df.withColumn("mem_pct", round(col("mem_pct").cast(FloatType()), 2))


combined_df = cpu_df.join(mem_df, ["ts", "server_id"])

min_ts = combined_df.agg(spark_min("ts")).collect()[0][0]
max_ts = combined_df.agg(spark_max("ts")).collect()[0][0]

window_size_sec = 30
slide_sec = 10

start_seconds = min_ts.second
aligned_second = (start_seconds // slide_sec) * slide_sec
start_time = min_ts.replace(second=aligned_second, microsecond=0)


window_starts = []
curr = start_time
while curr <= max_ts:
    window_starts.append(curr)
    curr += timedelta(seconds=slide_sec)

servers = combined_df.select("server_id").distinct().collect()
window_rows = []
for s in servers:
    server = s["server_id"]
    for start in window_starts:
        window_rows.append(Row(server_id=server,
                               window_start_ts=start,
                               window_end_ts=start + timedelta(seconds=window_size_sec)))

windows_df = spark.createDataFrame(window_rows)


windows_df = windows_df.withColumnRenamed("server_id", "window_server_id")


joined = combined_df.join(
    windows_df,
    (combined_df.server_id == windows_df.window_server_id) &
    (combined_df.ts >= windows_df.window_start_ts) &
    (combined_df.ts < windows_df.window_end_ts),
    how="right"
)


agg_df = joined.groupBy(
    coalesce(col("server_id"), col("window_server_id")).alias("server_id"),
    col("window_start_ts"),
    col("window_end_ts")
).agg(
    round(avg("cpu_pct"), 2).alias("avg_cpu"),
    round(avg("mem_pct"), 2).alias("avg_mem")
)


alerts_df = agg_df.withColumn(
    "alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD), "High CPU + Memory stress")
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD), "CPU spike suspected")
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD), "Memory saturation suspected")
)


final_output = alerts_df.select(
    col("server_id"),
    date_format(col("window_start_ts"), "HH:mm:ss").alias("window_start"),
    date_format(col("window_end_ts"), "HH:mm:ss").alias("window_end"),
    col("avg_cpu"),
    col("avg_mem"),
    col("alert"),
    col("window_start_ts")  # Keep for sorting
).orderBy("server_id", "window_start_ts")  # Sort by server and time


output_filename = f"team_{TEAM_NO}_CPU_MEM.csv"
final_output.drop("window_start_ts").toPandas().to_csv(output_filename, index=False, header=True)

print(f"Spark Job finished. Alerts saved to '{output_filename}'")
spark.stop()
