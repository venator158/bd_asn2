import json
from kafka import KafkaConsumer
import csv

KAFKA_BROKER_IP = '172.24.174.69:9092'
TOPICS = ['topic-cpu', 'topic-mem']

CPU_CSV_FILE = 'cpu_data.csv'
MEM_CSV_FILE = 'mem_data.csv'

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER_IP],
    auto_offset_reset='earliest',
    group_id='consumer-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Open files once in write mode at the start
f_cpu = open(CPU_CSV_FILE, 'w', newline='')
f_mem = open(MEM_CSV_FILE, 'w', newline='')

cpu_writer = csv.writer(f_cpu)
mem_writer = csv.writer(f_mem)

try:
    for message in consumer:
        data = message.value
        print(f"Received from topic '{message.topic}': {data}")

        if message.topic == 'topic-cpu':
            cpu_writer.writerow([data['ts'], data['server_id'], data['cpu_pct']])
            f_cpu.flush()  # ensures row is written immediately

        elif message.topic == 'topic-mem':
            mem_writer.writerow([data['ts'], data['server_id'], data['mem_pct']])
            f_mem.flush()

except KeyboardInterrupt:
    print("\nStopping Consumer 1")
finally:
    f_cpu.close()
    f_mem.close()
    consumer.close()
    print("Consumer 1 connection closed")


