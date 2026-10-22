"""
Low-Latency Key-Value Store (HBase Simulation)
Compliant with Problem Statement #05:
- Keyed by `patient_hash`
- Storing real-time bedside vitals and active deterioration alerts
- Sub-10ms point lookup for ICU escalation & nurse response
"""

import os
import sys
import json
import time
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
HBASE_DIR = os.path.join(DATA_DIR, "lakehouse", "hbase")
os.makedirs(HBASE_DIR, exist_ok=True)
HBASE_FILE = os.path.join(HBASE_DIR, "hbase_table_patient_vitals.json")

class HBaseKVStore:
    def __init__(self):
        self.store = {}
        self.load()

    def load(self):
        if os.path.exists(HBASE_FILE):
            try:
                with open(HBASE_FILE, "r", encoding="utf-8") as f:
                    self.store = json.load(f)
            except Exception:
                self.store = {}

    def flush(self):
        with open(HBASE_FILE, "w", encoding="utf-8") as f:
            json.dump(self.store, f, indent=2)

    def put_vitals(self, patient_hash: str, vitals: dict):
        """Upsert into column family 'vitals'"""
        if patient_hash not in self.store:
            self.store[patient_hash] = {"vitals": {}, "alerts": {}}
        self.store[patient_hash]["vitals"] = {
            **vitals,
            "last_updated": datetime.now().isoformat()
        }

    def put_alert(self, patient_hash: str, alert_type: str, severity: str, details: dict):
        """Upsert into column family 'alerts'"""
        if patient_hash not in self.store:
            self.store[patient_hash] = {"vitals": {}, "alerts": {}}
        self.store[patient_hash]["alerts"] = {
            "alert_type": alert_type,
            "severity": severity,
            "details": details,
            "triggered_at": datetime.now().isoformat(),
            "status": "ACTIVE_ESCALATION"
        }

    def get_patient_state(self, patient_hash: str):
        """Sub-10ms point lookup"""
        return self.store.get(patient_hash, None)

    def get_all_active_alerts(self):
        active = []
        for pid, record in self.store.items():
            alert = record.get("alerts")
            if alert and alert.get("status") == "ACTIVE_ESCALATION":
                active.append({
                    "patient_hash": pid,
                    "bed_id": record.get("vitals", {}).get("bed_id", "Unknown"),
                    "vitals": record.get("vitals", {}),
                    "alert": alert
                })
        return active

if __name__ == "__main__":
    kv = HBaseKVStore()
    kv.put_vitals("PAT_DEMO_001", {"bed_id": "ICU-01", "heart_rate": 88, "spo2": 97.5})
    kv.put_alert("PAT_DEMO_001", "HYPOXIA_ALERT", "CRITICAL", {"consecutive_minutes": 3, "min_spo2": 88.0})
    kv.flush()
    print("✅ HBase Low-Latency Store initialized. Active alerts:", len(kv.get_all_active_alerts()))
