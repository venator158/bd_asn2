from kafka import KafkaConsumer
import json, csv, os

BOOTSTRAP_SERVERS = '172.24.174.69:9092'

def write_to_csv(filename, fieldnames, data):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def main():
    consumer = KafkaConsumer(
        'topic-cpu', 'topic-mem',
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Consumer 1 started. Listening to CPU and MEM topics...")

    for message in consumer:
        data = message.value
        topic = message.topic
        if topic == 'topic-cpu':
            write_to_csv('cpu_data.csv', ['ts','server_id','cpu_pct'], data)
        elif topic == 'topic-mem':
            write_to_csv('mem_data.csv', ['ts','server_id','mem_pct'], data)
        print(f"Data written from {topic}: {data}")

if __name__ == "__main__":
    main()