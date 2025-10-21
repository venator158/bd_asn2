from kafka import KafkaProducer
import json
import os
from pyspark.sql import SparkSession

KAFKA_BROKER_IP = 'broker_ip_here:9092'

#topic names
TOPIC_CPU = 'topic-cpu'
TOPIC_MEM = 'topic-mem'
TOPIC_NET = 'topic-net'
TOPIC_DISK = 'topic-disk'

#Spark init
spark = SparkSession.builder \
    .appName("MetricsProducer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
dataset_path = os.path.join(parent_dir, "team_17", "dataset.csv")

#producer making and config
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_IP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
    batch_size=16384,
    compression_type='gzip'
)
#read using spark and then publish
def read_and_publish_metrics():
    df = spark.read.option("header", "true").csv(dataset_path)
    metrics_data = df.collect()
    
    print(f"{len(metrics_data)} rows are there")
    
    for row in metrics_data:
        metrics = {
            "ts": row.ts,
            "server_id": row.server_id,
            "cpu_pct": float(row.cpu_pct),
            "mem_pct": float(row.mem_pct),
            "net_in": float(row.net_in),
            "net_out": float(row.net_out),
            "disk_io": float(row.disk_io)
        }

        producer.send(TOPIC_CPU, {
            "ts": metrics["ts"],
            "server_id": metrics["server_id"],
            "cpu_pct": metrics["cpu_pct"]
        })

        producer.send(TOPIC_MEM, {
            "ts": metrics["ts"],
            "server_id": metrics["server_id"],
            "mem_pct": metrics["mem_pct"]
        })

        producer.send(TOPIC_NET, {
            "ts": metrics["ts"],
            "server_id": metrics["server_id"],
            "net_in": metrics["net_in"],
            "net_out": metrics["net_out"]
        })

        producer.send(TOPIC_DISK, {
            "ts": metrics["ts"],
            "server_id": metrics["server_id"],
            "disk_io": metrics["disk_io"]
        })

    producer.flush()
    print("Sent pa")

try:
    read_and_publish_metrics()
    
except Exception as e:
    print(f"Error occurred: {e}")

finally:
    producer.close()
    spark.stop()
    print("Done pa")
