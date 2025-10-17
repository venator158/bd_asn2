from kafka import KafkaProducer
import json
import os
from pyspark.sql import SparkSession

# Kafka broker IP (replace with actual)
KAFKA_BROKER_IP = 'broker_ip_here:9092'

# Kafka topics
TOPIC_CPU = 'topic-cpu'
TOPIC_MEM = 'topic-mem'
TOPIC_NET = 'topic-net'
TOPIC_DISK = 'topic-disk'

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MetricsProducer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Get the dataset path - assuming team_17 folder is in the parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
dataset_path = os.path.join(parent_dir, "team_17", "dataset.csv")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_IP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
    batch_size=16384,
    compression_type='gzip'
)

def read_and_publish_metrics():
    # Read the dataset using PySpark
    df = spark.read.option("header", "true").csv(dataset_path)
    
    # Convert to list of dictionaries for processing
    metrics_data = df.collect()
    
    print(f"Processing {len(metrics_data)} metric records...")
    
    for row in metrics_data:
        # Extract metrics from the row
        metrics = {
            "ts": row.ts,
            "server_id": row.server_id,
            "cpu_pct": float(row.cpu_pct),
            "mem_pct": float(row.mem_pct),
            "net_in": float(row.net_in),
            "net_out": float(row.net_out),
            "disk_io": float(row.disk_io)
        }

        # Publish to Kafka topics
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

    # Flush all messages to ensure they are sent
    producer.flush()
    print("All metrics published to Kafka successfully!")

try:
    read_and_publish_metrics()
    
except Exception as e:
    print(f"Error occurred: {e}")

finally:
    producer.close()
    spark.stop()
    print("Producer and Spark session closed.")
