# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Vitals and Readmission Lakehouse
# MAGIC
# MAGIC This notebook loads the project's anonymized CSV outputs from a Unity Catalog
# MAGIC Volume, validates and models admissions data with Spark, trains a baseline
# MAGIC readmission classifier, and includes an optional synthetic live-vitals stream.
# MAGIC
# MAGIC **No AWS credentials, external Kafka broker, or external LLM key is required.**
# MAGIC The streaming demo uses Spark's rate source; it is synthetic and is not a
# MAGIC connection to the dashboard's in-process Kafka simulator.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

CATALOG = "workspace"
SCHEMA = "healthcare_capstone"
VOLUME = "project_data"
INPUT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
OUTPUT_SCHEMA = f"{CATALOG}.{SCHEMA}"
CHECKPOINT_ROOT = f"{INPUT_ROOT}/checkpoints"

print(f"Input data: {INPUT_ROOT}")
print(f"Managed tables: {OUTPUT_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load project data
# MAGIC
# MAGIC The uploaded extracts contain pseudonymized patient hashes and masked SSNs.
# MAGIC Do not upload raw patient identifiers or secrets to this Volume.

# COMMAND ----------

fact_path = f"{INPUT_ROOT}/gold/fact_admissions_all.csv"
patient_path = f"{INPUT_ROOT}/gold/dim_patient.csv"
doctor_path = f"{INPUT_ROOT}/gold/dim_doctor.csv"
diagnosis_path = f"{INPUT_ROOT}/gold/dim_diagnosis.csv"

required_paths = [fact_path, patient_path, doctor_path, diagnosis_path]
required_files = {path.rsplit("/", 1)[-1] for path in required_paths}
available_files = {item.name.rstrip("/") for item in dbutils.fs.ls(f"{INPUT_ROOT}/gold")}
missing_paths = sorted(required_files.difference(available_files))
if missing_paths:
    raise FileNotFoundError(
        f"Required project CSVs are missing from {INPUT_ROOT}/gold: " + ", ".join(missing_paths)
    )

def read_project_csv(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

source_fact = read_project_csv(fact_path)
dim_patient_source = read_project_csv(patient_path)
dim_doctor_source = read_project_csv(doctor_path)
dim_diagnosis_source = read_project_csv(diagnosis_path)

display(source_fact.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Bronze ingestion and data-quality checks

# COMMAND ----------

required_columns = {
    "admission_id",
    "patient_hash",
    "doctor_id",
    "diagnosis_code",
    "admission_month",
    "length_of_stay",
    "prior_inpatient_admissions",
    "num_medications",
    "num_lab_procedures",
    "num_abnormal_labs",
    "avg_a1c_result",
    "total_charges",
    "icu_stay",
    "readmit_30d",
}
missing_columns = required_columns.difference(source_fact.columns)
if missing_columns:
    raise ValueError(f"Admissions extract is missing required columns: {sorted(missing_columns)}")

bronze_fact = source_fact
bronze_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_bronze_admissions"
)

bronze_counts = spark.createDataFrame(
    [("admissions", bronze_fact.count())],
    ["dataset", "row_count"],
)
display(bronze_counts)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver cleansing and enrichment
# MAGIC
# MAGIC Duplicate admission IDs and rows missing model-critical values are excluded.

# COMMAND ----------

silver_fact = (
    bronze_fact
    .dropDuplicates(["admission_id"])
    .withColumn("length_of_stay", F.col("length_of_stay").cast(IntegerType()))
    .withColumn("prior_inpatient_admissions", F.col("prior_inpatient_admissions").cast(IntegerType()))
    .withColumn("num_medications", F.col("num_medications").cast(IntegerType()))
    .withColumn("num_lab_procedures", F.col("num_lab_procedures").cast(IntegerType()))
    .withColumn("num_abnormal_labs", F.col("num_abnormal_labs").cast(IntegerType()))
    .withColumn("avg_a1c_result", F.col("avg_a1c_result").cast(DoubleType()))
    .withColumn("total_charges", F.col("total_charges").cast(DoubleType()))
    .withColumn("icu_stay", F.col("icu_stay").cast(IntegerType()))
    .withColumn("readmit_30d", F.col("readmit_30d").cast(IntegerType()))
    .filter(
        F.col("admission_id").isNotNull()
        & F.col("patient_hash").isNotNull()
        & F.col("readmit_30d").isin(0, 1)
    )
)

silver_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_silver_admissions"
)

silver_patient = dim_patient_source.dropDuplicates(["patient_hash"])
silver_doctor = dim_doctor_source.dropDuplicates(["doctor_id"])
silver_diagnosis = dim_diagnosis_source.dropDuplicates(["diagnosis_code"])

print(f"Bronze rows: {bronze_fact.count():,}; silver rows: {silver_fact.count():,}")
display(silver_fact.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold star schema and readmission KPIs

# COMMAND ----------

gold_fact = (
    silver_fact
    .join(silver_patient, on="patient_hash", how="left")
    .join(silver_doctor, on="doctor_id", how="left")
    .join(silver_diagnosis, on="diagnosis_code", how="left")
)

gold_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_gold_fact_admissions"
)
silver_patient.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_dim_patient"
)
silver_doctor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_dim_doctor"
)
silver_diagnosis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_dim_diagnosis"
)

readmission_kpis = (
    gold_fact
    .groupBy("admission_month")
    .agg(
        F.count("*").alias("admissions"),
        F.round(F.avg("readmit_30d") * 100, 2).alias("readmission_rate_pct"),
        F.round(F.avg("length_of_stay"), 2).alias("avg_length_of_stay"),
        F.round(F.avg("total_charges"), 2).alias("avg_total_charges"),
    )
    .orderBy("admission_month")
)
readmission_kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{OUTPUT_SCHEMA}.capstone_gold_readmission_kpis"
)
display(readmission_kpis)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Baseline Spark ML readmission model
# MAGIC
# MAGIC This is a project demonstration, not a clinically validated decision system.
# MAGIC The held-out recall is measured from the test split; no target is asserted.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

feature_columns = [
    "length_of_stay",
    "prior_inpatient_admissions",
    "num_medications",
    "num_lab_procedures",
    "num_abnormal_labs",
    "avg_a1c_result",
    "total_charges",
    "icu_stay",
]
model_data = gold_fact.select(
    *[F.coalesce(F.col(name), F.lit(0)).alias(name) for name in feature_columns],
    F.col("readmit_30d").alias("label"),
)

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
classifier = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=50,
    maxDepth=6,
    seed=42,
)
model_pipeline = Pipeline(stages=[assembler, classifier])
train_data, test_data = model_data.randomSplit([0.8, 0.2], seed=42)
if train_data.isEmpty() or test_data.isEmpty():
    raise ValueError("The project dataset must contain enough rows for a train/test split.")

readmission_model = model_pipeline.fit(train_data)
test_predictions = readmission_model.transform(test_data)
positive_recall = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="recallByLabel",
    metricLabel=1.0,
).evaluate(test_predictions)
print(f"Held-out readmission recall (positive class): {positive_recall:.3f}")
display(test_predictions.select("label", "prediction", "probability").limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Optional live synthetic vitals stream
# MAGIC
# MAGIC Change `RUN_LIVE_STREAM` to `True` and run this cell to start a continuous
# MAGIC Spark rate-source demo. It writes synthetic readings to a Delta table and
# MAGIC displays the updating stream. Stop the cell/stream when finished to release
# MAGIC compute. This does not connect to Kafka or use real patient data.

# COMMAND ----------

RUN_LIVE_STREAM = False
VITALS_TABLE = f"{OUTPUT_SCHEMA}.capstone_live_vitals"

rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
live_vitals = (
    rate_stream
    .withColumn(
        "patient_hash",
        F.sha2(F.concat(F.lit("DEMO_PATIENT_"), (F.col("value") % 20).cast("string")), 256),
    )
    .withColumn("heart_rate", (F.lit(55) + F.pmod(F.col("value") * 7, F.lit(70))).cast("int"))
    .withColumn("spo2", F.round(F.lit(88.0) + F.pmod(F.col("value") * 3, F.lit(13)) * 0.5, 1))
    .withColumn("respiratory_rate", (F.lit(12) + F.pmod(F.col("value") * 5, F.lit(18))).cast("int"))
    .withColumn("systolic_bp", (F.lit(100) + F.pmod(F.col("value") * 11, F.lit(60))).cast("int"))
    .select(
        F.col("timestamp").alias("event_time"),
        "patient_hash",
        "heart_rate",
        "spo2",
        "respiratory_rate",
        "systolic_bp",
    )
)

if RUN_LIVE_STREAM:
    live_query = (
        live_vitals.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/live_vitals")
        .toTable(VITALS_TABLE)
    )
    print(f"Streaming to {VITALS_TABLE}; stop the query when done.")
    display(spark.readStream.table(VITALS_TABLE))
else:
    demo_vitals = (
        spark.range(20)
        .withColumn("event_time", F.current_timestamp())
        .withColumn(
            "patient_hash",
            F.sha2(F.concat(F.lit("DEMO_PATIENT_"), F.col("id").cast("string")), 256),
        )
        .withColumn("heart_rate", (F.lit(55) + F.pmod(F.col("id") * 7, F.lit(70))).cast("int"))
        .withColumn("spo2", F.round(F.lit(88.0) + F.pmod(F.col("id") * 3, F.lit(13)) * 0.5, 1))
        .withColumn("respiratory_rate", (F.lit(12) + F.pmod(F.col("id") * 5, F.lit(18))).cast("int"))
        .withColumn("systolic_bp", (F.lit(100) + F.pmod(F.col("id") * 11, F.lit(60))).cast("int"))
        .drop("id")
    )
    print("Live mode is off. Set RUN_LIVE_STREAM = True to enable the continuous demo.")
    display(demo_vitals)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Dashboard queries
# MAGIC
# MAGIC Use the Databricks SQL editor or notebook visualizations to chart the tables:
# MAGIC `workspace.healthcare_capstone.capstone_gold_readmission_kpis` and
# MAGIC `workspace.healthcare_capstone.capstone_live_vitals`. The live table is populated only while
# MAGIC the optional stream is running.
