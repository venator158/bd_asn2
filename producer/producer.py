from kafka import KafkaProducer
import json
import time
import psutil  # For system metrics

# Kafka broker IP (replace with actual)
KAFKA_BROKER_IP = 'broker_ip_here:9092'

# Kafka topics
TOPIC_CPU = 'topic-cpu'
TOPIC_MEM = 'topic-mem'
TOPIC_NET = 'topic-net'
TOPIC_DISK = 'topic-disk'

SERVER_ID = 'producer_machine_1'  # Customize server identifier

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_IP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
    batch_size=16384,
    compression_type='gzip'
)

def collect_metrics():
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    cpu_pct = psutil.cpu_percent(interval=1)
    mem_pct = psutil.virtual_memory().percent

    net_io = psutil.net_io_counters()
    net_in = net_io.bytes_recv
    net_out = net_io.bytes_sent

    disk_io = psutil.disk_io_counters()
    disk_activity = disk_io.read_bytes + disk_io.write_bytes

    return {
        "ts": timestamp,
        "server_id": SERVER_ID,
        "cpu_pct": cpu_pct,
        "mem_pct": mem_pct,
        "net_in": net_in,
        "net_out": net_out,
        "disk_io": disk_activity
    }

try:
    while True:
        metrics = collect_metrics()

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

        producer.flush()
        time.sleep(5)  # Adjust collection interval

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()
