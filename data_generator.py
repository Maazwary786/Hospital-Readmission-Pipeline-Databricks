"""
Data Generator for Healthcare Patient Vitals Capstone Pipeline
Compliant with Problem Statement #05 Specification:
- PhysioNet waveform vitals replay simulation
- OpenAQ ambient air quality feed
- Synthetic MIMIC/Synthea patient dataset (Admissions, Diagnoses, Labs, Claims, Doctors)
"""

import os
import sys
import json
import csv
import random
import hashlib
from datetime import datetime, timedelta

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
MYSQL_DUMP_DIR = os.path.join(RAW_DIR, "mysql_ehr")
STREAMING_DIR = os.path.join(RAW_DIR, "streaming_landing")

os.makedirs(MYSQL_DUMP_DIR, exist_ok=True)
os.makedirs(STREAMING_DIR, exist_ok=True)

def hash_patient_id(raw_id: str) -> str:
    """HIPAA-compliant SHA-256 identifier hashing"""
    return hashlib.sha256(raw_id.encode('utf-8')).hexdigest()[:16]

def generate_all_data(num_patients=500, num_admissions=1200, streaming_event_count=300):
    print("🏥 Generating HIPAA Capstone Healthcare Datasets...")
    
    # 1. Doctors
    doctors = [
        {"doctor_id": f"DOC_{i:03d}", "doctor_name": f"Dr. Specialist_{i}", 
         "specialty": random.choice(["Cardiology", "Pulmonology", "Endocrinology", "Critical Care", "Internal Medicine"]),
         "department": random.choice(["ICU", "Step-Down", "General Ward", "Emergency"])}
        for i in range(1, 26)
    ]
    with open(os.path.join(MYSQL_DUMP_DIR, "doctors.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=doctors[0].keys())
        writer.writeheader()
        writer.writerows(doctors)

    # 2. Diagnoses
    diagnoses = [
        {"diagnosis_code": "I21.9", "diagnosis_group": "Circulatory", "description": "Acute myocardial infarction", "base_risk": 0.28},
        {"diagnosis_code": "I50.9", "diagnosis_group": "Circulatory", "description": "Heart failure, unspecified", "base_risk": 0.25},
        {"diagnosis_code": "J44.9", "diagnosis_group": "Respiratory", "description": "Chronic obstructive pulmonary disease", "base_risk": 0.22},
        {"diagnosis_code": "J18.9", "diagnosis_group": "Respiratory", "description": "Pneumonia, unspecified organism", "base_risk": 0.20},
        {"diagnosis_code": "E11.9", "diagnosis_group": "Diabetes", "description": "Type 2 diabetes mellitus", "base_risk": 0.18},
        {"diagnosis_code": "K52.9", "diagnosis_group": "Digestive", "description": "Noninfective gastroenteritis", "base_risk": 0.14},
        {"diagnosis_code": "S06.0", "diagnosis_group": "Injury", "description": "Concussion / Trauma", "base_risk": 0.19},
        {"diagnosis_code": "M54.5", "diagnosis_group": "Musculoskeletal", "description": "Low back pain", "base_risk": 0.11},
    ]
    with open(os.path.join(MYSQL_DUMP_DIR, "diagnoses.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["diagnosis_code", "diagnosis_group", "description", "base_risk"])
        writer.writeheader()
        writer.writerows(diagnoses)

    # 3. Patients (with raw PII that will be masked/hashed for HIPAA)
    patients = []
    base_date = datetime(2025, 1, 1)
    for i in range(1, num_patients + 1):
        raw_ssn = f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
        raw_name = f"Patient_Record_{i}"
        raw_pid = f"PAT_ID_{i:05d}"
        hashed_pid = hash_patient_id(raw_pid)
        
        patients.append({
            "raw_patient_id": raw_pid,
            "patient_hash": hashed_pid,
            "ssn_raw": raw_ssn,
            "name_raw": raw_name,
            "age": random.randint(22, 88),
            "age_group": random.choice(["[20-40)", "[40-60)", "[60-80)", "[80-100)"]),
            "gender": random.choice(["Male", "Female"]),
            "primary_care_physician": random.choice(doctors)["doctor_id"]
        })
    with open(os.path.join(MYSQL_DUMP_DIR, "patients.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=patients[0].keys())
        writer.writeheader()
        writer.writerows(patients)

    # 4. Admissions (Historical & Current Encounters)
    admissions = []
    labs = []
    claims = []
    
    for i in range(1, num_admissions + 1):
        pat = random.choice(patients)
        diag = random.choice(diagnoses)
        doc = random.choice(doctors)
        
        adm_days_offset = random.randint(0, 360)
        adm_date = base_date + timedelta(days=adm_days_offset)
        los = random.choices([2, 3, 4, 5, 6, 7, 10, 14], weights=[20, 25, 20, 15, 10, 5, 3, 2])[0]
        disch_date = adm_date + timedelta(days=los)
        admission_month = adm_date.strftime("%Y-%m")
        
        adm_type = random.choice(["Emergency", "Urgent", "Elective", "Trauma Center"])
        prior_admissions = random.choices([0, 1, 2, 3, 4], weights=[55, 22, 12, 7, 4])[0]
        num_medications = random.randint(3, 28)
        num_lab_procedures = random.randint(10, 85)
        icu_admit = 1 if (diag["diagnosis_group"] in ["Circulatory", "Respiratory"] and random.random() < 0.35) else 0
        
        # Ground truth readmission probability logic
        risk_score = diag["base_risk"] + (0.08 * prior_admissions) + (0.02 * (los > 5)) + (0.05 * (pat["age"] > 65))
        readmit_30d = 1 if random.random() < risk_score else 0

        adm_id = f"ADM_{i:06d}"
        admissions.append({
            "admission_id": adm_id,
            "patient_hash": pat["patient_hash"],
            "doctor_id": doc["doctor_id"],
            "diagnosis_code": diag["diagnosis_code"],
            "diagnosis_group": diag["diagnosis_group"],
            "admission_date": adm_date.strftime("%Y-%m-%d"),
            "discharge_date": disch_date.strftime("%Y-%m-%d"),
            "admission_month": admission_month,
            "admission_type": adm_type,
            "length_of_stay": los,
            "prior_inpatient_admissions": prior_admissions,
            "num_medications": num_medications,
            "num_lab_procedures": num_lab_procedures,
            "icu_stay": icu_admit,
            "readmit_30d": readmit_30d
        })

        # Labs for this admission
        labs.append({
            "lab_id": f"LAB_{i:06d}_A1C",
            "admission_id": adm_id,
            "patient_hash": pat["patient_hash"],
            "test_name": "A1C",
            "result_numeric": round(random.uniform(5.2, 11.8), 1),
            "normal_range": "< 5.7%",
            "flag_abnormal": 1 if random.random() > 0.45 else 0
        })
        labs.append({
            "lab_id": f"LAB_{i:06d}_GLU",
            "admission_id": adm_id,
            "patient_hash": pat["patient_hash"],
            "test_name": "Glucose",
            "result_numeric": round(random.uniform(70, 260), 0),
            "normal_range": "70-99 mg/dL",
            "flag_abnormal": 1 if random.random() > 0.50 else 0
        })

        # Billing Claims
        charges = los * random.uniform(1200, 3500) + (4500 if icu_admit else 0)
        claims.append({
            "claim_id": f"CLM_{i:06d}",
            "admission_id": adm_id,
            "patient_hash": pat["patient_hash"],
            "total_charges": round(charges, 2),
            "insurance_type": random.choice(["Medicare", "Medicaid", "Private Commercial", "Self-Pay"]),
            "claim_status": random.choice(["Paid", "Approved", "In-Review"])
        })

    with open(os.path.join(MYSQL_DUMP_DIR, "admissions.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=admissions[0].keys())
        writer.writeheader()
        writer.writerows(admissions)

    with open(os.path.join(MYSQL_DUMP_DIR, "labs.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=labs[0].keys())
        writer.writeheader()
        writer.writerows(labs)

    with open(os.path.join(MYSQL_DUMP_DIR, "claims.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=claims[0].keys())
        writer.writeheader()
        writer.writerows(claims)

    # 5. Hybrid Bedside Streaming Vitals (PhysioNet Waveform + OpenAQ Feed)
    # Target SLA: Detect SpO2 < 90% for 3 consecutive minutes with <10s alert trigger
    streaming_events = []
    stream_now = datetime.now()
    active_icu_patients = patients[:10]  # Focus monitoring on top 10 ICU beds
    
    # Intentionally inject deterioration episode in Bed-03 & Bed-07 to test <10s SpO2 alert
    for step in range(streaming_event_count):
        evt_time = stream_now + timedelta(seconds=step * 20)
        for idx, p in enumerate(active_icu_patients):
            bed_id = f"ICU-BED-{idx+1:02d}"
            
            # Deterioration injection: Bed-03 undergoes acute hypoxia (SpO2 drops below 90% for >= 3 mins)
            if idx == 2 and step >= 10 and step <= 25:
                spo2 = round(random.uniform(84.0, 88.5), 1)
                hr = random.randint(118, 142)  # Tachycardia
                condition = "HYPOXIA_CRITICAL"
            elif idx == 6 and step >= 15 and step <= 28:
                spo2 = round(random.uniform(86.0, 89.2), 1)
                hr = random.randint(110, 135)
                condition = "OXYGEN_DESATURATION"
            else:
                spo2 = round(random.uniform(94.0, 99.5), 1)
                hr = random.randint(62, 92)
                condition = "NORMAL"

            # OpenAQ Air Quality parameters (PM2.5, AQI) simulated ambient telemetry
            pm25 = round(random.uniform(8.5, 38.0), 1)
            aqi = int(pm25 * 3.1)

            streaming_events.append({
                "event_id": f"EVT_{step:04d}_{idx:02d}",
                "timestamp": evt_time.strftime("%Y-%m-%d %H:%M:%S"),
                "patient_hash": p["patient_hash"],
                "bed_id": bed_id,
                "icu_unit": "Surgical ICU" if idx < 5 else "Medical ICU",
                "heart_rate": hr,
                "spo2": spo2,
                "blood_pressure_sys": random.randint(105, 145),
                "blood_pressure_dia": random.randint(65, 92),
                "respiration_rate": random.randint(12, 26),
                "openaq_pm25": pm25,
                "openaq_aqi": aqi,
                "clinical_condition": condition
            })

    with open(os.path.join(STREAMING_DIR, "vitals_stream.json"), "w", encoding="utf-8") as f:
        json.dump(streaming_events, f, indent=2)

    print(f"✅ Generated {len(patients)} Patients, {len(admissions)} Admissions, {len(labs)} Labs, {len(claims)} Claims.")
    print(f"✅ Generated {len(streaming_events)} Bedside Streaming Vitals Events (PhysioNet + OpenAQ).")
    return len(admissions), len(streaming_events)

if __name__ == "__main__":
    generate_all_data()
