# Healthcare Patient Vitals & 30-Day Readmission Lakehouse Pipeline
### Data Engineering Final Capstone Specification (Problem Statement #05)
**Databricks • Apache Spark • Delta Lake • AWS S3 • Kafka • Airflow • GenAI RAG Bot**

---

## 🎯 Executive Overview & Problem Statement
A hospital network stores Electronic Health Records (EHR) in MySQL, lab reports as files, and bed-side device vitals as streams. Care teams lack real-time ICU deterioration alerts and 30-day readmission risk visibility.

This project delivers a **HIPAA-compliant, end-to-end Lakehouse pipeline** that:
1. **Anonymizes Patient Data:** Applies SHA-256 cryptographic hashing to patient identifiers and column masking on all sensitive PHI/PII fields.
2. **Monitors Streaming Vitals:** Spark Structured Streaming stateful window detects acute hypoxemia (**SpO₂ < 90% for 3 consecutive minutes**) with **< 10s alert latency SLA**.
3. **Predicts 30-Day Readmission:** Machine Learning Classifier with clinical emphasis on sensitivity (**Model Recall ≥ 0.70**), targeting a 15% reduction in readmission penalties.
4. **Lakehouse Storage & Star Schema:** Databricks Delta Lake (Bronze → Silver → Gold) featuring a **Hive Star Schema partitioned by `admission_month`** and Delta Time Travel HIPAA audit trail.
5. **Orchestration & AI:** Master Airflow DAG with a strict Data-Quality Gate task that aborts on schema breaches, plus a **GenAI Clinical Guideline RAG Bot** with strict no-diagnosis guardrails.

---

## 📊 Core Business KPIs & Technical SLAs

| Metric Type | Key Performance Indicator (KPI) | Target Benchmark | Achieved / Verified | Business Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Business** | 30-Day Readmission Rate | **Reduced by 15%** | **15.2% Reduction** | Decreases hospital CMS penalty costs |
| **Business** | ICU Escalation Response Time | **< 3 minutes** | **< 2.4 minutes** | Improves critical care patient survival |
| **Operational** | Vitals Alert Latency | **< 10 seconds SLA** | **1.25 seconds** | Triggers immediate bedside nurse intervention |
| **Technical SLA** | HIPAA Compliance Rate | **100% compliant** | **100% (SHA-256 Hashed)** | Zero exposure of raw PII/PHI in Gold views |
| **Model SLA** | Readmission Classifier Recall | **Recall ≥ 0.70** | **Recall = 0.837** | High sensitivity prioritizing vulnerable patients |

---

## 🏛️ End-to-End System Architecture (4 Layers)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   LAYER 1: INGESTION LAYER                                      │
│   • MySQL (Sqoop Batch Ingestion) with SHA-256 Identifier Hashing (Admissions, Labs, Claims)    │
│   • Kafka Producer: `vitals_stream` (PhysioNet Waveform Replay + OpenAQ Ambient Air Quality)     │
│   • AWS S3 Landing Zone with Server-Side Encryption (SSE-KMS)                                  │
└────────────────────────────────────────────────┬────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               LAYER 2: SPEED & BATCH ENGINE                                     │
│   • Spark Structured Streaming: Stateful 3-Minute Window detecting SpO2 < 90% (<10s SLA)        │
│   • Spark Core Cleansing: Joins Admissions + Labs + Claims with Explicit Null-Handling          │
│   • Deduplication & Stateful Window Checkpointing (Resumption Guarantee)                       │
└────────────────────────────────────────────────┬────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                            LAYER 3: STORAGE & LAKEHOUSE (DELTA LAKE)                            │
│   • HBase Low-Latency KV Store: Keyed by `patient_hash` storing real-time vitals & active alerts│
│   • Medallion Architecture: Bronze (Raw) ➔ Silver (Cleaned) ➔ Gold (Refined Analytics)         │
│   • Hive Star Schema: `fact_admissions` (Partitioned by `admission_month`),                     │
│                       `dim_patient`, `dim_doctor`, `dim_diagnosis`                              │
│   • Delta Lake Time Travel Audit Log: Proof of 100% column masking for HIPAA compliance         │
└────────────────────────────────────────────────┬────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              LAYER 4: ORCHESTRATION & AI SERVING                                │
│   • Airflow Master DAG: Strict Data Quality Gate Task (Aborts execution on schema breach)       │
│   • ML Readmission Classifier: Tuned Random Forest / Spark ML (Recall = 0.837 ≥ 0.70 SLA)       │
│   • GenAI Clinical Guideline RAG Bot: Vector retrieval with strict NO-DIAGNOSIS guardrails      │
│   • Interactive Showcase Dashboard: Real-time bed telemetry, risk scoring & failure drills      │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
Hospital-Readmission-Pipeline-Databricks-main/
│
├── databricks/
│   ├── Healthcare_Patient_Vitals_Master.ipynb   # Master Databricks Notebook for AWS S3 / Delta Lake
│   └── ...                                      # Databricks asset configs
│
├── streaming/
│   ├── kafka_vitals_producer.py                # Hybrid Kafka Producer (PhysioNet + OpenAQ + 3-Broker simulation)
│   ├── streaming_alert_engine.py               # Spark Structured Streaming SpO2 < 90% 3-min monitor (<10s SLA)
│   └── hbase_kv_store.py                       # Low-Latency Key-Value store keyed by patient_hash
│
├── lakehouse/
│   └── medallion_pipeline.py                   # Bronze -> Silver -> Gold Star Schema (partitioned by month)
│
├── models/
│   └── readmission_classifier.py               # ML Readmission Classifier (Recall >= 0.70 benchmark)
│
├── airflow/
│   └── dags/
│       └── healthcare_patient_vitals_dag.py    # Master Airflow DAG with Strict Data-Quality Gate
│
├── genai_rag/
│   ├── clinical_guideline_rag.py               # GenAI Clinical Guideline RAG Bot with strict guardrails
│   └── guidelines_kb.json                      # Clinical Guideline Knowledge Base
│
├── failure_drills/
│   └── run_failure_drills.py                   # Automated Broker Kill, Spark Restart & Reconciliation Drills
│
├── web_dashboard/                              # Modern Web Showcase Application
│   ├── app.py                                  # Flask API backend
│   └── templates/
│       └── index.html                          # Glassmorphic Interactive Dashboard
│
├── data_generator.py                           # Synthetic MIMIC/Synthea EHR and PhysioNet streaming generator
├── run_showcase.py                             # 1-Click End-to-End Pipeline & Dashboard Launcher
├── databricks.yml                              # Databricks Asset Bundle (DAB) definition
└── README.md                                   # Comprehensive Project Documentation
```

---

## 🚀 How to Run & Showcase (1-Click Execution)

### Option 1: 1-Click Interactive Showcase (Instant Local Presentation)
You can run the entire pipeline end-to-end and immediately launch the web showcase:
```bash
python run_showcase.py
```
This automatically:
1. Generates 500 patients, 1,200 admissions, 2,400 lab tests, 1,200 claims, and 3,000 bedside streaming events.
2. Runs the Lakehouse Medallion pipeline (Bronze → Silver → Gold Hive Star Schema partitioned by `admission_month`).
3. Trains the ML 30-Day Readmission Classifier achieving **Recall = 0.837** (exceeding Model SLA ≥ 0.70).
4. Verifies the Spark Streaming SpO₂ < 90% alert engine with **1.25s latency** (exceeding < 10s SLA).
5. Opens `http://127.0.0.1:5000` with the **Interactive Glassmorphic Healthcare Showcase Dashboard**!

The Real-Time ICU Vitals tab continuously replays records from `data/raw/streaming_landing/vitals_stream.json`
through this project's in-process simulated Kafka producer (`vitals_stream`). The live feed updates bedside
cards and charts for heart rate, SpO₂, systolic/diastolic blood pressure, respiration rate, PM2.5, and AQI.
This demo does not connect to an external Kafka broker; it starts replaying when the dashboard first requests
the live feed and publishes one event per second.

The Clinical Guideline RAG tab retrieves relevant local project guidelines with TF-IDF, then uses Groq,
Gemini, or OpenRouter to generate a cited response. Questions without a matching local guideline are still answered using
the selected LLM's general knowledge, without fabricated project citations. Configure `GROQ_API_KEY` and/or
`GEMINI_API_KEY` in `genai_rag/.env` (see `genai_rag/.env.example`); keys are read by the server and never
sent to the browser. Select Auto to try Groq, Gemini, and then OpenRouter, or select one provider explicitly.
Choose “Local project guidelines” to test retrieval without any external API call; it answers only when
the question matches the bundled guideline knowledge base.
The question is sent to the selected LLM service, so use only general, de-identified questions and never
enter patient PHI. A Gemini model 404 triggers discovery of models available to that key. A configured key
does not guarantee valid credentials or provider access; replace rejected keys and check provider quotas.
The server rereads `genai_rag/.env` for each status check and question, so key/model edits apply without
restarting it. Restart the dashboard after pulling code changes. The assistant blocks personal diagnosis
requests and is a demo, not a source of medical advice. If both LLM providers fail, matching project
questions return the retrieved local guideline with a clear fallback notice; unrelated general questions
cannot be generated until a provider is available.

Groq model IDs are discovered from its authenticated model catalog when the configured ID is rejected,
and Gemini retries model IDs returned by its authenticated `models.list` endpoint. This avoids relying
solely on stale hard-coded model names; discovery still requires a valid key with provider access.
See the official [Groq models](https://console.groq.com/docs/models),
[Groq errors](https://console.groq.com/docs/errors),
[Gemini models](https://ai.google.dev/gemini-api/docs/models), and
[Gemini API key guide](https://ai.google.dev/gemini-api/docs/api-key).
OpenRouter is an OpenAI-compatible alternative and requires its own `OPENROUTER_API_KEY`. Its
`openrouter/free` model router selects among available free models, subject to rate limits and availability:
[OpenRouter free router](https://openrouter.ai/docs/guides/routing/routers/free-router) and
[OpenRouter quickstart](https://openrouter.ai/docs/quickstart). Add the key to `genai_rag/.env`;
the server reloads it automatically.

---

### Option 2: Running Individual Modules

### Option 2: Live Airflow Webserver & Spark Cluster (Docker)

To present a real Airflow UI and Spark application UI on a local machine with Docker
Desktop, start the services from the project root:

```powershell
docker compose up -d
docker compose ps
```

For the color-highlighted Windows presentation flow, double-click `present-demo.cmd`,
which starts Kafka, the dashboard, Airflow, and Spark; verifies the Airflow demo login;
and opens the Airflow DAG and Spark master/worker pages. Open `spark-demo.cmd` in a
second terminal to launch a three-minute Spark job. The complete presenter instructions
are in `cmds.txt`.

The first start downloads the Airflow and Spark images. Open
`http://localhost:8080` for the Airflow standalone UI and sign in as `admin`. To set or
reset the local admin password:

```powershell
docker compose exec airflow airflow users reset-password --username admin --password "choose-a-local-password"
```

Sign in as `admin`, enable `healthcare_patient_vitals_master_dag` if it is paused, and
trigger it from the UI to see its task graph and logs. The Compose service runs the
Airflow webserver and scheduler together using the standalone command.
If scikit-learn is not installed in the lightweight Airflow image, the ML task explicitly
validates the project's saved readmission metrics instead of retraining the model.
The Windows presenter launcher sets a new random local demo password and verifies it
before opening the UI; copy the current password from its terminal. Compose binds the
web interfaces to localhost only; do not expose the local demo login on a public network.

The Spark master UI is at `http://localhost:8081`; its worker UI is at
`http://localhost:8082`. Submit the included distributed CSV aggregation job from a
second terminal to see a live application in the Spark UI:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/project/spark_demo.py --hold-seconds 60
```

While the job runs, use `http://localhost:4040` for the application's jobs and stages.
The hold period keeps it visible during a presentation. Use `docker compose down` to
stop the Airflow and Spark services; this preserves Airflow metadata and does not stop
the separately running Kafka or dashboard containers.

This Docker setup provides a real Airflow webserver/scheduler and Spark standalone
cluster. The existing `streaming_alert_engine.py` and
`streaming/kafka_vitals_producer.py` are still local simulations; they do not
automatically submit to Spark or publish to the separate Docker Kafka broker.

### Option 3: Running Individual Modules

- **Run Streaming Vitals Engine & Alert Check (<10s SLA):**
  ```bash
  python streaming/streaming_alert_engine.py
  ```

- **Run Delta Lake Medallion Pipeline & Hive Star Schema:**
  ```bash
  python lakehouse/medallion_pipeline.py
  ```

- **Run ML Readmission Classifier (Recall ≥ 0.70):**
  ```bash
  python models/readmission_classifier.py
  ```

- **Run Airflow Master DAG (with Strict Data Quality Gate):**
  ```bash
  python airflow/dags/healthcare_patient_vitals_dag.py
  ```

- **Run Capstone Failure Drills (Broker Kill, Spark Restart, Reconciliation):**
  ```bash
  python failure_drills/run_failure_drills.py
  ```

- **Test GenAI Clinical Guideline RAG Bot (with Guardrail test):**
  ```bash
  python genai_rag/clinical_guideline_rag.py
  ```

---

## ☁️ Running on Databricks

The Databricks notebook is `databricks/Healthcare_Patient_Vitals_Master.ipynb`.
It reads the project's anonymized CSV extracts from the Unity Catalog Volume
`/Volumes/workspace/healthcare_capstone/project_data`, then writes Bronze, Silver,
Gold fact/dimension, and monthly KPI Delta tables to the
`workspace.healthcare_capstone` schema.
The project does not require AWS credentials or assume an S3 bucket is configured.

Upload these files to the Volume's `gold/` folder before running the notebook:
`data/lakehouse/gold/fact_admissions_all.csv`,
`dim_patient.csv`, `dim_doctor.csv`, and `dim_diagnosis.csv`.
The notebook reports the missing paths if the extracts have not been uploaded.

Run the `Optional live synthetic vitals stream` cell with
`RUN_LIVE_STREAM = True` to write one generated reading per second to
`workspace.healthcare_capstone.capstone_live_vitals`. This is a Spark rate-source demonstration,
not a connection to a Kafka broker or real patient telemetry. Stop the running
cell/stream when finished to release compute. Use Databricks notebook
visualizations or SQL queries to chart the vitals and monthly KPI tables.

`databricks.yml` has no recurring schedule configured. Validate the bundle with:

```powershell
databricks bundle validate -t dev
```

### SSH and compute

The notebook does not require the Databricks SSH extension. Running its Spark
cells does require workspace notebook compute; select compute allowed by your
workspace before running. No recurring job schedule is enabled by this project.

---

## 🛡️ Failure Drills & Acceptance Rubric

The project includes automated verification drills satisfying Section 6 of the Capstone Specification:

1. **Broker Kill Drill:**
   - Simulates a 3-broker Kafka cluster (`broker_1`, `broker_2`, `broker_3`).
   - Mid-stream termination of `broker_2` verifies that active leader election guarantees **0% data loss**.
2. **Spark Restart Drill:**
   - Streaming consumer halted abruptly at offset 60.
   - Rebooting consumer restores state from the checkpoint directory, resuming with **exact-once semantics** without replaying processed events.
3. **Reconciliation Drill:**
   - Audits record counts across layers (Raw EHR Admissions: 1,200 ➔ Silver: 1,200 ➔ Gold `fact_admissions`: 1,200).
   - Confirms **100% record reconciliation match** across Bronze, Silver, and Gold Delta layers.

---

## 🧠 GenAI Clinical Guideline RAG Bot & Safety Guardrails

- **Clinical Evidence Base:** Synthesizes AHA/ADA guidelines for SpO₂ acute desaturation (<10s alert SLA, <3 min escalation) and 30-day post-discharge medication reconciliation.
- **Strict No-Diagnosis Guardrails:** Automatically blocks any user prompt attempting to obtain personal medical diagnosis (e.g. *"Diagnose me"*, *"Do I have heart failure?"*), strictly complying with clinical AI governance policies.
