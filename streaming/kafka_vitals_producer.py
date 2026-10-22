"""
Kafka Hybrid Producer for Healthcare Bed-side Vitals
Compliant with Problem Statement #05:
- Replays PhysioNet waveform vitals (Heart Rate, SpO2, Blood Pressure)
- Merges OpenAQ ambient air quality feed (PM2.5, AQI)
- Streams to topic `vitals_stream` partitioned by patient_hash
- Implements 3-broker cluster simulation supporting Broker Kill failure drill
"""

import os
import sys
import json
import time
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
STREAMING_DATA_PATH = os.path.join(DATA_DIR, "raw", "streaming_landing", "vitals_stream.json")
KAFKA_BROKERS = ["broker_1:9092", "broker_2:9092", "broker_3:9092"]
TOPIC_NAME = "vitals_stream"

class SimulatedKafkaProducer:
    def __init__(self, brokers=None):
        self.brokers = brokers or list(KAFKA_BROKERS)
        self.active_brokers = set(self.brokers)
        self.topic = TOPIC_NAME
        self.published_events = []
        self.offsets = {0: 0, 1: 0, 2: 0}

    def kill_broker(self, broker_name):
        """Simulates Broker Kill Drill (1 of 3 brokers killed)"""
        if broker_name in self.active_brokers:
            self.active_brokers.remove(broker_name)
            print(f"⚠️ [Broker Kill Drill] Broker {broker_name} halted! Remaining active brokers: {list(self.active_brokers)}")
            return True
        return False

    def recover_broker(self, broker_name):
        self.active_brokers.add(broker_name)
        print(f"✅ Broker {broker_name} recovered and back in cluster.")

    def get_partition(self, key: str) -> int:
        return abs(hash(key)) % 3

    def send(self, topic: str, key: str, value: dict):
        if not self.active_brokers:
            raise ConnectionError("No active Kafka brokers available!")
        
        partition = self.get_partition(key)
        offset = self.offsets[partition]
        self.offsets[partition] += 1
        
        # Message envelope
        message = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "value": value,
            "producer_timestamp": datetime.now().isoformat()
        }
        self.published_events.append(message)
        return message

def run_stream_producer(limit=50, delay_sec=0.05):
    print("🚀 Initializing Kafka Hybrid Vitals Producer (`vitals_stream`)...")
    if not os.path.exists(STREAMING_DATA_PATH):
        raise FileNotFoundError(f"Missing streaming data at {STREAMING_DATA_PATH}. Run data_generator.py first.")
    
    with open(STREAMING_DATA_PATH, "r", encoding="utf-8") as f:
        events = json.load(f)

    producer = SimulatedKafkaProducer()
    count = 0
    for evt in events[:limit]:
        producer.send(
            topic=TOPIC_NAME,
            key=evt["patient_hash"],
            value=evt
        )
        count += 1
        if delay_sec > 0:
            time.sleep(delay_sec)

    print(f"✅ Published {count} vitals events across 3 Kafka partitions successfully.")
    return producer

if __name__ == "__main__":
    run_stream_producer(limit=30, delay_sec=0.01)
