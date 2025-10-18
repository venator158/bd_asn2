# consumer_2.py 
import json
from kafka import KafkaConsumer
import csv

# === CONFIGURATION (EDIT THIS) ===
# IMPORTANT: Replace with your Kafka Broker's ZeroTier IP address
KAFKA_BROKER_IP = '172.24.174.69:9092' # Use the IP you successfully pinged
TOPICS = ['topic-net', 'topic-disk']
# =================================

NET_CSV_FILE = 'net_data.csv'
DISK_CSV_FILE = 'disk_data.csv'

# --- Initialize Kafka Consumer ---
print(f"Initializing Consumer 2 to listen on topics: {TOPICS}")
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER_IP],
    auto_offset_reset='earliest',
    group_id='consumer-group-2', # Use a unique group_id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# --- Consume and Store Data ---
# We open the files in 'w' (write) mode, which creates/empties them
print("File handles opened. Waiting for messages... (Press Ctrl+C to stop)")

try:
    with open(NET_CSV_FILE, 'w', newline='') as f_net, open(DISK_CSV_FILE, 'w', newline='') as f_disk:
        net_writer = csv.writer(f_net)
        disk_writer = csv.writer(f_disk)

        for message in consumer:
            data = message.value
            print(f"Received from topic '{message.topic}': {data}")

            if message.topic == 'topic-net':
                net_writer.writerow([data['ts'], data['server_id'], data['net_in'], data['net_out']])
                f_net.flush() # Ensure data is written immediately
            
            elif message.topic == 'topic-disk':
                disk_writer.writerow([data['ts'], data['server_id'], data['disk_io']])
                f_disk.flush()

except KeyboardInterrupt:
    print("\nStopping Consumer 2.")
finally:
    consumer.close()
    print("Consumer 2 connection closed.")
