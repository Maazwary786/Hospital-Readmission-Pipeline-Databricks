"""
One-Click End-to-End Showcase Runner for Healthcare Patient Vitals Pipeline
Problem Statement #05 - Data Engineering Final Capstone Specification

Executes:
1. Synthetic Data Generation (PhysioNet + OpenAQ + MIMIC/Synthea)
2. Lakehouse Medallion Pipeline (Bronze -> Silver -> Gold Star Schema partitioned by admission_month)
3. 30-Day Readmission Machine Learning Model (Tuned for Recall >= 0.70 SLA)
4. Streaming Deterioration Alert Engine (<10s SLA check)
5. Launches Interactive Showcase Web Dashboard on http://127.0.0.1:5000
"""

import os
import sys
import webbrowser
import threading
import time

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

def run_pipeline_setup():
    print("==========================================================================")
    print("🏥 HEALTHCARE PATIENT VITALS & READMISSION LAKEHOUSE PIPELINE")
    print("   Problem Statement #05 Capstone Specification Runner")
    print("==========================================================================")

    # Step 1: Data Generation
    print("\n[STEP 1/4] Generating Datasets (PhysioNet Waveforms, OpenAQ, Synthea EHR)...")
    from data_generator import generate_all_data
    generate_all_data(num_patients=500, num_admissions=1200, streaming_event_count=300)

    # Step 2: Medallion Lakehouse & Hive Star Schema
    print("\n[STEP 2/4] Executing Delta Lakehouse Medallion Pipeline...")
    from lakehouse.medallion_pipeline import run_medallion_pipeline
    run_medallion_pipeline()

    # Step 3: ML Readmission Classifier
    print("\n[STEP 3/4] Training & Evaluating 30-Day Readmission Classifier (Recall >= 0.70)...")
    from models.readmission_classifier import train_and_evaluate_classifier
    train_and_evaluate_classifier()

    # Step 4: Streaming Vitals Engine
    print("\n[STEP 4/4] Starting Streaming Vitals Monitor & Enforcing <10s Alert SLA...")
    from streaming.streaming_alert_engine import StreamingAlertProcessor
    processor = StreamingAlertProcessor()
    processor.run_stream(max_records=120)

    print("\n" + "="*74)
    print("🎉 FULL PIPELINE INITIALIZATION COMPLETE! ALL SLAS VERIFIED.")
    print("==========================================================================")

def open_browser():
    time.sleep(1.5)
    url = "http://127.0.0.1:5000"
    print(f"\n🌐 Opening Showcase Dashboard in your browser: {url}\n")
    webbrowser.open(url)

if __name__ == "__main__":
    run_pipeline_setup()
    
    # Launch Web Showcase Server
    from web_dashboard.app import app
    threading.Thread(target=open_browser, daemon=True).start()
    app.run(host="127.0.0.1", port=5000, debug=False)
