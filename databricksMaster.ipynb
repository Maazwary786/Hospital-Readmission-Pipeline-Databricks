{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "6939a747-a290-4279-87d7-0535d15692a8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Healthcare Vitals and Readmission Lakehouse\n",
    "\n",
    "This notebook loads the project's anonymized CSV outputs from a Unity Catalog\n",
    "Volume, validates and models admissions data with Spark, trains a baseline\n",
    "readmission classifier, and includes an optional synthetic live-vitals stream.\n",
    "\n",
    "\n",
    "\n",
    "The streaming demo uses Spark's rate source; it is synthetic and is not a\n",
    "connection to the dashboard's in-process Kafka simulator.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603100772,
     "inputWidgets": {},
     "nuid": "b1a9f23d-f820-479a-b5bf-d13115aba3c1",
     "showTitle": false,
     "startTime": 1792603100335,
     "submitTime": 1792603098131,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Input data: /Volumes/workspace/healthcare_capstone/project_data\nManaged tables: workspace.healthcare_capstone\n"
     ]
    }
   ],
   "source": [
    "|from pyspark.sql import functions as F\n",
    "from pyspark.sql.types import DoubleType, IntegerType\n",
    "\n",
    "CATALOG = \"workspace\"\n",
    "SCHEMA = \"healthcare_capstone\"\n",
    "VOLUME = \"project_data\"\n",
    "INPUT_ROOT = f\"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}\"\n",
    "OUTPUT_SCHEMA = f\"{CATALOG}.{SCHEMA}\"\n",
    "CHECKPOINT_ROOT = f\"{INPUT_ROOT}/checkpoints\"\n",
    "\n",
    "print(f\"Input data: {INPUT_ROOT}\")\n",
    "print(f\"Managed tables: {OUTPUT_SCHEMA}\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b8af76ca-b634-45c6-8d8d-af8a90944253",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 1. Load project data\n",
    "\n",
    "The uploaded extracts contain pseudonymized patient hashes and masked SSNs.\n",
    "Do not upload raw patient identifiers or secrets to this Volume.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603143832,
     "inputWidgets": {},
     "nuid": "25e9e24d-baac-4c7d-af5d-bd202d09b6b9",
     "showTitle": false,
     "startTime": 1792603100877,
     "submitTime": 1792603098147,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>admission_id</th><th>patient_hash</th><th>doctor_id</th><th>diagnosis_code</th><th>admission_date</th><th>discharge_date</th><th>admission_month</th><th>admission_type</th><th>length_of_stay</th><th>prior_inpatient_admissions</th><th>num_medications</th><th>num_lab_procedures</th><th>num_abnormal_labs</th><th>avg_a1c_result</th><th>total_charges</th><th>insurance_type</th><th>icu_stay</th><th>readmit_30d</th></tr></thead><tbody><tr><td>ADM_000001</td><td>98283da5f25d4bc1</td><td>DOC_015</td><td>J18.9</td><td>2025-10-31</td><td>2025-11-02</td><td>2025-10-01</td><td>Urgent</td><td>2</td><td>3</td><td>17</td><td>38</td><td>1</td><td>78.4</td><td>8483.78</td><td>Medicaid</td><td>1</td><td>1</td></tr><tr><td>ADM_000002</td><td>4145d5e8212f75fa</td><td>DOC_004</td><td>K52.9</td><td>2025-03-30</td><td>2025-04-04</td><td>2025-03-01</td><td>Emergency</td><td>5</td><td>2</td><td>12</td><td>23</td><td>0</td><td>58.55</td><td>16700.16</td><td>Private Commercial</td><td>0</td><td>0</td></tr><tr><td>ADM_000003</td><td>53202b4e86f99985</td><td>DOC_015</td><td>K52.9</td><td>2025-08-28</td><td>2025-09-04</td><td>2025-08-01</td><td>Urgent</td><td>7</td><td>0</td><td>25</td><td>38</td><td>1</td><td>81.1</td><td>17309.79</td><td>Self-Pay</td><td>0</td><td>1</td></tr><tr><td>ADM_000004</td><td>3fabb75529936c8c</td><td>DOC_013</td><td>S06.0</td><td>2025-10-09</td><td>2025-10-12</td><td>2025-10-01</td><td>Emergency</td><td>3</td><td>0</td><td>25</td><td>34</td><td>1</td><td>55.95</td><td>5591.45</td><td>Private Commercial</td><td>0</td><td>0</td></tr><tr><td>ADM_000005</td><td>12586f15394869f9</td><td>DOC_022</td><td>J44.9</td><td>2025-09-10</td><td>2025-09-13</td><td>2025-09-01</td><td>Emergency</td><td>3</td><td>2</td><td>18</td><td>36</td><td>2</td><td>62.55</td><td>8471.48</td><td>Self-Pay</td><td>1</td><td>1</td></tr><tr><td>ADM_000006</td><td>5df6155c1797a810</td><td>DOC_014</td><td>S06.0</td><td>2025-09-13</td><td>2025-09-15</td><td>2025-09-01</td><td>Trauma Center</td><td>2</td><td>0</td><td>23</td><td>49</td><td>0</td><td>100.15</td><td>4676.67</td><td>Medicaid</td><td>0</td><td>0</td></tr><tr><td>ADM_000007</td><td>7ab239d8e7b35338</td><td>DOC_014</td><td>S06.0</td><td>2025-12-22</td><td>2025-12-24</td><td>2025-12-01</td><td>Trauma Center</td><td>2</td><td>0</td><td>7</td><td>11</td><td>2</td><td>115.75</td><td>5965.29</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000008</td><td>f8ec53d8f48f506c</td><td>DOC_005</td><td>J18.9</td><td>2025-03-04</td><td>2025-03-08</td><td>2025-03-01</td><td>Urgent</td><td>4</td><td>1</td><td>19</td><td>43</td><td>1</td><td>81.85</td><td>7621.65</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000009</td><td>d94c12df8a0b2e6b</td><td>DOC_009</td><td>M54.5</td><td>2025-02-06</td><td>2025-02-10</td><td>2025-02-01</td><td>Urgent</td><td>4</td><td>2</td><td>8</td><td>12</td><td>2</td><td>94.9</td><td>7198.21</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000010</td><td>5fe22b3ac56d671c</td><td>DOC_025</td><td>M54.5</td><td>2025-04-24</td><td>2025-04-27</td><td>2025-04-01</td><td>Elective</td><td>3</td><td>1</td><td>24</td><td>11</td><td>2</td><td>110.6</td><td>9699.92</td><td>Medicare</td><td>0</td><td>0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "ADM_000001",
         "98283da5f25d4bc1",
         "DOC_015",
         "J18.9",
         "2025-10-31",
         "2025-11-02",
         "2025-10-01",
         "Urgent",
         2,
         3,
         17,
         38,
         1,
         78.4,
         8483.78,
         "Medicaid",
         1,
         1
        ],
        [
         "ADM_000002",
         "4145d5e8212f75fa",
         "DOC_004",
         "K52.9",
         "2025-03-30",
         "2025-04-04",
         "2025-03-01",
         "Emergency",
         5,
         2,
         12,
         23,
         0,
         58.55,
         16700.16,
         "Private Commercial",
         0,
         0
        ],
        [
         "ADM_000003",
         "53202b4e86f99985",
         "DOC_015",
         "K52.9",
         "2025-08-28",
         "2025-09-04",
         "2025-08-01",
         "Urgent",
         7,
         0,
         25,
         38,
         1,
         81.1,
         17309.79,
         "Self-Pay",
         0,
         1
        ],
        [
         "ADM_000004",
         "3fabb75529936c8c",
         "DOC_013",
         "S06.0",
         "2025-10-09",
         "2025-10-12",
         "2025-10-01",
         "Emergency",
         3,
         0,
         25,
         34,
         1,
         55.95,
         5591.45,
         "Private Commercial",
         0,
         0
        ],
        [
         "ADM_000005",
         "12586f15394869f9",
         "DOC_022",
         "J44.9",
         "2025-09-10",
         "2025-09-13",
         "2025-09-01",
         "Emergency",
         3,
         2,
         18,
         36,
         2,
         62.55,
         8471.48,
         "Self-Pay",
         1,
         1
        ],
        [
         "ADM_000006",
         "5df6155c1797a810",
         "DOC_014",
         "S06.0",
         "2025-09-13",
         "2025-09-15",
         "2025-09-01",
         "Trauma Center",
         2,
         0,
         23,
         49,
         0,
         100.15,
         4676.67,
         "Medicaid",
         0,
         0
        ],
        [
         "ADM_000007",
         "7ab239d8e7b35338",
         "DOC_014",
         "S06.0",
         "2025-12-22",
         "2025-12-24",
         "2025-12-01",
         "Trauma Center",
         2,
         0,
         7,
         11,
         2,
         115.75,
         5965.29,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000008",
         "f8ec53d8f48f506c",
         "DOC_005",
         "J18.9",
         "2025-03-04",
         "2025-03-08",
         "2025-03-01",
         "Urgent",
         4,
         1,
         19,
         43,
         1,
         81.85,
         7621.65,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000009",
         "d94c12df8a0b2e6b",
         "DOC_009",
         "M54.5",
         "2025-02-06",
         "2025-02-10",
         "2025-02-01",
         "Urgent",
         4,
         2,
         8,
         12,
         2,
         94.9,
         7198.21,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000010",
         "5fe22b3ac56d671c",
         "DOC_025",
         "M54.5",
         "2025-04-24",
         "2025-04-27",
         "2025-04-01",
         "Elective",
         3,
         1,
         24,
         11,
         2,
         110.6,
         9699.92,
         "Medicare",
         0,
         0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "admission_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "patient_hash",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "doctor_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "diagnosis_code",
         "type": "\"string\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "admission_date",
         "type": "\"date\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "discharge_date",
         "type": "\"date\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M\"}",
         "name": "admission_month",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "admission_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "length_of_stay",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "prior_inpatient_admissions",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_medications",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_lab_procedures",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_abnormal_labs",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "avg_a1c_result",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "total_charges",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "insurance_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "icu_stay",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "readmit_30d",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "fact_path = f\"{INPUT_ROOT}/gold/fact_admissions_all.csv\"\n",
    "patient_path = f\"{INPUT_ROOT}/gold/dim_patient.csv\"\n",
    "doctor_path = f\"{INPUT_ROOT}/gold/dim_doctor.csv\"\n",
    "diagnosis_path = f\"{INPUT_ROOT}/gold/dim_diagnosis.csv\"\n",
    "\n",
    "required_paths = [fact_path, patient_path, doctor_path, diagnosis_path]\n",
    "required_files = {path.rsplit(\"/\", 1)[-1] for path in required_paths}\n",
    "available_files = {item.name.rstrip(\"/\") for item in dbutils.fs.ls(f\"{INPUT_ROOT}/gold\")}\n",
    "missing_paths = sorted(required_files.difference(available_files))\n",
    "if missing_paths:\n",
    "    raise FileNotFoundError(\n",
    "        f\"Required project CSVs are missing from {INPUT_ROOT}/gold: \" + \", \".join(missing_paths)\n",
    "    )\n",
    "\n",
    "def read_project_csv(path):\n",
    "    return (\n",
    "        spark.read\n",
    "        .option(\"header\", True)\n",
    "        .option(\"inferSchema\", True)\n",
    "        .csv(path)\n",
    "    )\n",
    "\n",
    "source_fact = read_project_csv(fact_path)\n",
    "dim_patient_source = read_project_csv(patient_path)\n",
    "dim_doctor_source = read_project_csv(doctor_path)\n",
    "dim_diagnosis_source = read_project_csv(diagnosis_path)\n",
    "\n",
    "display(source_fact.limit(10))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "6c64be66-2e3c-4860-bd3f-5dd848e7bc71",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 2. Bronze ingestion and data-quality checks\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603156722,
     "inputWidgets": {},
     "nuid": "36113b31-be0f-41d5-90ba-b5d9beb70b86",
     "showTitle": false,
     "startTime": 1792603143932,
     "submitTime": 1792603098163,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>dataset</th><th>row_count</th></tr></thead><tbody><tr><td>admissions</td><td>1200</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "admissions",
         1200
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "dataset",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "row_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "required_columns = {\n",
    "    \"admission_id\",\n",
    "    \"patient_hash\",\n",
    "    \"doctor_id\",\n",
    "    \"diagnosis_code\",\n",
    "    \"admission_month\",\n",
    "    \"length_of_stay\",\n",
    "    \"prior_inpatient_admissions\",\n",
    "    \"num_medications\",\n",
    "    \"num_lab_procedures\",\n",
    "    \"num_abnormal_labs\",\n",
    "    \"avg_a1c_result\",\n",
    "    \"total_charges\",\n",
    "    \"icu_stay\",\n",
    "    \"readmit_30d\",\n",
    "}\n",
    "missing_columns = required_columns.difference(source_fact.columns)\n",
    "if missing_columns:\n",
    "    raise ValueError(f\"Admissions extract is missing required columns: {sorted(missing_columns)}\")\n",
    "\n",
    "bronze_fact = source_fact\n",
    "bronze_fact.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_bronze_admissions\"\n",
    ")\n",
    "\n",
    "bronze_counts = spark.createDataFrame(\n",
    "    [(\"admissions\", bronze_fact.count())],\n",
    "    [\"dataset\", \"row_count\"],\n",
    ")\n",
    "display(bronze_counts)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "3cc61750-cd0c-4e63-84bb-b9799286a1d4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 3. Silver cleansing and enrichment\n",
    "\n",
    "Duplicate admission IDs and rows missing model-critical values are excluded.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603165627,
     "inputWidgets": {},
     "nuid": "381b05fb-4022-46d3-b4d8-58ec412b0fdd",
     "showTitle": false,
     "startTime": 1792603156748,
     "submitTime": 1792603098177,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Bronze rows: 1,200; silver rows: 1,200\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>admission_id</th><th>patient_hash</th><th>doctor_id</th><th>diagnosis_code</th><th>admission_date</th><th>discharge_date</th><th>admission_month</th><th>admission_type</th><th>length_of_stay</th><th>prior_inpatient_admissions</th><th>num_medications</th><th>num_lab_procedures</th><th>num_abnormal_labs</th><th>avg_a1c_result</th><th>total_charges</th><th>insurance_type</th><th>icu_stay</th><th>readmit_30d</th></tr></thead><tbody><tr><td>ADM_000013</td><td>9fef0090d016f524</td><td>DOC_008</td><td>E11.9</td><td>2025-06-24</td><td>2025-06-30</td><td>2025-06-01</td><td>Urgent</td><td>6</td><td>0</td><td>15</td><td>22</td><td>1</td><td>66.75</td><td>19352.83</td><td>Private Commercial</td><td>0</td><td>0</td></tr><tr><td>ADM_000014</td><td>8be17597f368be8e</td><td>DOC_008</td><td>I21.9</td><td>2025-11-27</td><td>2025-11-30</td><td>2025-11-01</td><td>Elective</td><td>3</td><td>0</td><td>9</td><td>74</td><td>0</td><td>52.3</td><td>8102.22</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000076</td><td>a11888eef15ba196</td><td>DOC_012</td><td>I21.9</td><td>2025-11-02</td><td>2025-11-05</td><td>2025-11-01</td><td>Elective</td><td>3</td><td>1</td><td>27</td><td>52</td><td>2</td><td>131.9</td><td>8242.4</td><td>Private Commercial</td><td>0</td><td>0</td></tr><tr><td>ADM_000099</td><td>4793b95e4a1e4622</td><td>DOC_001</td><td>J18.9</td><td>2025-04-03</td><td>2025-04-08</td><td>2025-04-01</td><td>Urgent</td><td>5</td><td>0</td><td>27</td><td>43</td><td>1</td><td>40.55</td><td>13026.69</td><td>Self-Pay</td><td>1</td><td>0</td></tr><tr><td>ADM_000107</td><td>813be5a086a1763f</td><td>DOC_003</td><td>I50.9</td><td>2025-02-26</td><td>2025-02-28</td><td>2025-02-01</td><td>Urgent</td><td>2</td><td>0</td><td>7</td><td>20</td><td>1</td><td>100.85</td><td>4523.01</td><td>Private Commercial</td><td>0</td><td>0</td></tr><tr><td>ADM_000120</td><td>82cb95a86d390485</td><td>DOC_002</td><td>E11.9</td><td>2025-02-21</td><td>2025-02-27</td><td>2025-02-01</td><td>Urgent</td><td>6</td><td>0</td><td>23</td><td>58</td><td>1</td><td>104.85</td><td>9403.76</td><td>Self-Pay</td><td>0</td><td>1</td></tr><tr><td>ADM_000129</td><td>3350ccecb6e5a91d</td><td>DOC_008</td><td>S06.0</td><td>2025-11-23</td><td>2025-11-27</td><td>2025-11-01</td><td>Emergency</td><td>4</td><td>0</td><td>22</td><td>81</td><td>0</td><td>92.0</td><td>8188.33</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000153</td><td>acc2176b07698cb7</td><td>DOC_008</td><td>J44.9</td><td>2025-10-24</td><td>2025-11-03</td><td>2025-10-01</td><td>Trauma Center</td><td>10</td><td>4</td><td>21</td><td>17</td><td>1</td><td>60.25</td><td>25953.99</td><td>Self-Pay</td><td>1</td><td>1</td></tr><tr><td>ADM_000200</td><td>4c5ca55a57e2134c</td><td>DOC_003</td><td>E11.9</td><td>2025-12-16</td><td>2025-12-20</td><td>2025-12-01</td><td>Emergency</td><td>4</td><td>0</td><td>18</td><td>26</td><td>0</td><td>104.2</td><td>10439.49</td><td>Self-Pay</td><td>0</td><td>0</td></tr><tr><td>ADM_000212</td><td>14f6c9d295477c74</td><td>DOC_004</td><td>J44.9</td><td>2025-11-08</td><td>2025-11-11</td><td>2025-11-01</td><td>Trauma Center</td><td>3</td><td>0</td><td>25</td><td>48</td><td>2</td><td>74.8</td><td>8364.8</td><td>Self-Pay</td><td>0</td><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "ADM_000013",
         "9fef0090d016f524",
         "DOC_008",
         "E11.9",
         "2025-06-24",
         "2025-06-30",
         "2025-06-01",
         "Urgent",
         6,
         0,
         15,
         22,
         1,
         66.75,
         19352.83,
         "Private Commercial",
         0,
         0
        ],
        [
         "ADM_000014",
         "8be17597f368be8e",
         "DOC_008",
         "I21.9",
         "2025-11-27",
         "2025-11-30",
         "2025-11-01",
         "Elective",
         3,
         0,
         9,
         74,
         0,
         52.3,
         8102.22,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000076",
         "a11888eef15ba196",
         "DOC_012",
         "I21.9",
         "2025-11-02",
         "2025-11-05",
         "2025-11-01",
         "Elective",
         3,
         1,
         27,
         52,
         2,
         131.9,
         8242.4,
         "Private Commercial",
         0,
         0
        ],
        [
         "ADM_000099",
         "4793b95e4a1e4622",
         "DOC_001",
         "J18.9",
         "2025-04-03",
         "2025-04-08",
         "2025-04-01",
         "Urgent",
         5,
         0,
         27,
         43,
         1,
         40.55,
         13026.69,
         "Self-Pay",
         1,
         0
        ],
        [
         "ADM_000107",
         "813be5a086a1763f",
         "DOC_003",
         "I50.9",
         "2025-02-26",
         "2025-02-28",
         "2025-02-01",
         "Urgent",
         2,
         0,
         7,
         20,
         1,
         100.85,
         4523.01,
         "Private Commercial",
         0,
         0
        ],
        [
         "ADM_000120",
         "82cb95a86d390485",
         "DOC_002",
         "E11.9",
         "2025-02-21",
         "2025-02-27",
         "2025-02-01",
         "Urgent",
         6,
         0,
         23,
         58,
         1,
         104.85,
         9403.76,
         "Self-Pay",
         0,
         1
        ],
        [
         "ADM_000129",
         "3350ccecb6e5a91d",
         "DOC_008",
         "S06.0",
         "2025-11-23",
         "2025-11-27",
         "2025-11-01",
         "Emergency",
         4,
         0,
         22,
         81,
         0,
         92.0,
         8188.33,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000153",
         "acc2176b07698cb7",
         "DOC_008",
         "J44.9",
         "2025-10-24",
         "2025-11-03",
         "2025-10-01",
         "Trauma Center",
         10,
         4,
         21,
         17,
         1,
         60.25,
         25953.99,
         "Self-Pay",
         1,
         1
        ],
        [
         "ADM_000200",
         "4c5ca55a57e2134c",
         "DOC_003",
         "E11.9",
         "2025-12-16",
         "2025-12-20",
         "2025-12-01",
         "Emergency",
         4,
         0,
         18,
         26,
         0,
         104.2,
         10439.49,
         "Self-Pay",
         0,
         0
        ],
        [
         "ADM_000212",
         "14f6c9d295477c74",
         "DOC_004",
         "J44.9",
         "2025-11-08",
         "2025-11-11",
         "2025-11-01",
         "Trauma Center",
         3,
         0,
         25,
         48,
         2,
         74.8,
         8364.8,
         "Self-Pay",
         0,
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "admission_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "patient_hash",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "doctor_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "diagnosis_code",
         "type": "\"string\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "admission_date",
         "type": "\"date\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M-d\"}",
         "name": "discharge_date",
         "type": "\"date\""
        },
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M\"}",
         "name": "admission_month",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "admission_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "length_of_stay",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "prior_inpatient_admissions",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_medications",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_lab_procedures",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "num_abnormal_labs",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "avg_a1c_result",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "total_charges",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "insurance_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "icu_stay",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "readmit_30d",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "silver_fact = (\n",
    "    bronze_fact\n",
    "    .dropDuplicates([\"admission_id\"])\n",
    "    .withColumn(\"length_of_stay\", F.col(\"length_of_stay\").cast(IntegerType()))\n",
    "    .withColumn(\"prior_inpatient_admissions\", F.col(\"prior_inpatient_admissions\").cast(IntegerType()))\n",
    "    .withColumn(\"num_medications\", F.col(\"num_medications\").cast(IntegerType()))\n",
    "    .withColumn(\"num_lab_procedures\", F.col(\"num_lab_procedures\").cast(IntegerType()))\n",
    "    .withColumn(\"num_abnormal_labs\", F.col(\"num_abnormal_labs\").cast(IntegerType()))\n",
    "    .withColumn(\"avg_a1c_result\", F.col(\"avg_a1c_result\").cast(DoubleType()))\n",
    "    .withColumn(\"total_charges\", F.col(\"total_charges\").cast(DoubleType()))\n",
    "    .withColumn(\"icu_stay\", F.col(\"icu_stay\").cast(IntegerType()))\n",
    "    .withColumn(\"readmit_30d\", F.col(\"readmit_30d\").cast(IntegerType()))\n",
    "    .filter(\n",
    "        F.col(\"admission_id\").isNotNull()\n",
    "        & F.col(\"patient_hash\").isNotNull()\n",
    "        & F.col(\"readmit_30d\").isin(0, 1)\n",
    "    )\n",
    ")\n",
    "\n",
    "silver_fact.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_silver_admissions\"\n",
    ")\n",
    "\n",
    "silver_patient = dim_patient_source.dropDuplicates([\"patient_hash\"])\n",
    "silver_doctor = dim_doctor_source.dropDuplicates([\"doctor_id\"])\n",
    "silver_diagnosis = dim_diagnosis_source.dropDuplicates([\"diagnosis_code\"])\n",
    "\n",
    "print(f\"Bronze rows: {bronze_fact.count():,}; silver rows: {silver_fact.count():,}\")\n",
    "display(silver_fact.limit(10))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "055865e0-f984-458b-bc84-c3e82f9faf95",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 4. Gold star schema and readmission KPIs\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603190559,
     "inputWidgets": {},
     "nuid": "63dcd5b6-4471-4515-832a-47bec0171f08",
     "showTitle": false,
     "startTime": 1792603165657,
     "submitTime": 1792603098193,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>admission_month</th><th>admissions</th><th>readmission_rate_pct</th><th>avg_length_of_stay</th><th>avg_total_charges</th></tr></thead><tbody><tr><td>2025-01-01</td><td>107</td><td>20.56</td><td>4.15</td><td>10700.0</td></tr><tr><td>2025-02-01</td><td>87</td><td>24.14</td><td>3.8</td><td>9521.23</td></tr><tr><td>2025-03-01</td><td>102</td><td>24.51</td><td>4.34</td><td>11069.81</td></tr><tr><td>2025-04-01</td><td>92</td><td>27.17</td><td>4.2</td><td>10369.42</td></tr><tr><td>2025-05-01</td><td>97</td><td>32.99</td><td>3.99</td><td>10102.4</td></tr><tr><td>2025-06-01</td><td>91</td><td>28.57</td><td>4.33</td><td>10583.08</td></tr><tr><td>2025-07-01</td><td>99</td><td>27.27</td><td>4.19</td><td>10755.99</td></tr><tr><td>2025-08-01</td><td>105</td><td>35.24</td><td>4.01</td><td>10135.2</td></tr><tr><td>2025-09-01</td><td>108</td><td>28.7</td><td>4.24</td><td>11180.46</td></tr><tr><td>2025-10-01</td><td>117</td><td>32.48</td><td>4.25</td><td>10580.54</td></tr><tr><td>2025-11-01</td><td>95</td><td>23.16</td><td>4.54</td><td>11187.95</td></tr><tr><td>2025-12-01</td><td>100</td><td>32.0</td><td>4.17</td><td>10256.14</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "2025-01-01",
         107,
         20.56,
         4.15,
         10700.0
        ],
        [
         "2025-02-01",
         87,
         24.14,
         3.8,
         9521.23
        ],
        [
         "2025-03-01",
         102,
         24.51,
         4.34,
         11069.81
        ],
        [
         "2025-04-01",
         92,
         27.17,
         4.2,
         10369.42
        ],
        [
         "2025-05-01",
         97,
         32.99,
         3.99,
         10102.4
        ],
        [
         "2025-06-01",
         91,
         28.57,
         4.33,
         10583.08
        ],
        [
         "2025-07-01",
         99,
         27.27,
         4.19,
         10755.99
        ],
        [
         "2025-08-01",
         105,
         35.24,
         4.01,
         10135.2
        ],
        [
         "2025-09-01",
         108,
         28.7,
         4.24,
         11180.46
        ],
        [
         "2025-10-01",
         117,
         32.48,
         4.25,
         10580.54
        ],
        [
         "2025-11-01",
         95,
         23.16,
         4.54,
         11187.95
        ],
        [
         "2025-12-01",
         100,
         32.0,
         4.17,
         10256.14
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{\"__detected_date_formats\": \"yyyy-M\"}",
         "name": "admission_month",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "admissions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "readmission_rate_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "avg_length_of_stay",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "avg_total_charges",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "gold_fact = (\n",
    "    silver_fact\n",
    "    .join(silver_patient, on=\"patient_hash\", how=\"left\")\n",
    "    .join(silver_doctor, on=\"doctor_id\", how=\"left\")\n",
    "    .join(silver_diagnosis, on=\"diagnosis_code\", how=\"left\")\n",
    ")\n",
    "\n",
    "gold_fact.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_gold_fact_admissions\"\n",
    ")\n",
    "silver_patient.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_dim_patient\"\n",
    ")\n",
    "silver_doctor.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_dim_doctor\"\n",
    ")\n",
    "silver_diagnosis.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_dim_diagnosis\"\n",
    ")\n",
    "\n",
    "readmission_kpis = (\n",
    "    gold_fact\n",
    "    .groupBy(\"admission_month\")\n",
    "    .agg(\n",
    "        F.count(\"*\").alias(\"admissions\"),\n",
    "        F.round(F.avg(\"readmit_30d\") * 100, 2).alias(\"readmission_rate_pct\"),\n",
    "        F.round(F.avg(\"length_of_stay\"), 2).alias(\"avg_length_of_stay\"),\n",
    "        F.round(F.avg(\"total_charges\"), 2).alias(\"avg_total_charges\"),\n",
    "    )\n",
    "    .orderBy(\"admission_month\")\n",
    ")\n",
    "readmission_kpis.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(\n",
    "    f\"{OUTPUT_SCHEMA}.capstone_gold_readmission_kpis\"\n",
    ")\n",
    "display(readmission_kpis)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "701f0f04-5d23-4b8b-a6ac-8c850991f176",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 5. Baseline Spark ML readmission model\n",
    "\n",
    "This is a project demonstration, not a clinically validated decision system.\n",
    "The held-out recall is measured from the test split; no target is asserted.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603208679,
     "inputWidgets": {},
     "nuid": "42494fc3-1f7a-48d4-ab12-e94198c08452",
     "showTitle": false,
     "startTime": 1792603190591,
     "submitTime": 1792603098214,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Held-out readmission recall (positive class): 0.101\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>label</th><th>prediction</th><th>probability</th></tr></thead><tbody><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7075845148661907\",\"0.2924154851338093\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7392087899498696\",\"0.26079121005013034\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8488423989392168\",\"0.15115760106078324\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8701863479934936\",\"0.1298136520065064\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8688478965024906\",\"0.13115210349750941\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8551351175662195\",\"0.1448648824337804\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7960978316663627\",\"0.2039021683336372\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.6838443101652028\",\"0.31615568983479725\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7758927627693366\",\"0.22410723723066342\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.820143983912674\",\"0.17985601608732607\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8013664603849595\",\"0.19863353961504046\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8970850298326001\",\"0.1029149701674\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7612806490615821\",\"0.23871935093841798\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8133863113040701\",\"0.18661368869592998\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8395601454341094\",\"0.16043985456589052\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8383078651996131\",\"0.16169213480038694\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8362644414102745\",\"0.16373555858972552\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7445494364110268\",\"0.25545056358897317\"]}</td></tr><tr><td>1</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8279405602004561\",\"0.17205943979954377\"]}</td></tr><tr><td>0</td><td>0.0</td><td>{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8099969482595155\",\"0.19000305174048457\"]}</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7075845148661907\",\"0.2924154851338093\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7392087899498696\",\"0.26079121005013034\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8488423989392168\",\"0.15115760106078324\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8701863479934936\",\"0.1298136520065064\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8688478965024906\",\"0.13115210349750941\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8551351175662195\",\"0.1448648824337804\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7960978316663627\",\"0.2039021683336372\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.6838443101652028\",\"0.31615568983479725\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7758927627693366\",\"0.22410723723066342\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.820143983912674\",\"0.17985601608732607\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8013664603849595\",\"0.19863353961504046\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8970850298326001\",\"0.1029149701674\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7612806490615821\",\"0.23871935093841798\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8133863113040701\",\"0.18661368869592998\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8395601454341094\",\"0.16043985456589052\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8383078651996131\",\"0.16169213480038694\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8362644414102745\",\"0.16373555858972552\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.7445494364110268\",\"0.25545056358897317\"]}"
        ],
        [
         1,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8279405602004561\",\"0.17205943979954377\"]}"
        ],
        [
         0,
         0.0,
         "{\"type\":\"1\",\"size\":null,\"indices\":null,\"values\":[\"0.8099969482595155\",\"0.19000305174048457\"]}"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "label",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"ml_attr\": {\"num_vals\": 2, \"type\": \"nominal\"}}",
         "name": "prediction",
         "type": "\"double\""
        },
        {
         "metadata": "{\"ml_attr\": {\"num_attrs\": 2}}",
         "name": "probability",
         "type": "{\"class\":\"org.apache.spark.ml.linalg.VectorUDT\",\"pyClass\":\"pyspark.ml.linalg.VectorUDT\",\"sqlType\":{\"fields\":[{\"metadata\":{},\"name\":\"type\",\"nullable\":false,\"type\":\"byte\"},{\"metadata\":{},\"name\":\"size\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"indices\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"integer\",\"type\":\"array\"}},{\"metadata\":{},\"name\":\"values\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"double\",\"type\":\"array\"}}],\"type\":\"struct\"},\"type\":\"udt\"}"
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "from pyspark.ml import Pipeline\n",
    "from pyspark.ml.classification import RandomForestClassifier\n",
    "from pyspark.ml.evaluation import MulticlassClassificationEvaluator\n",
    "from pyspark.ml.feature import VectorAssembler\n",
    "\n",
    "feature_columns = [\n",
    "    \"length_of_stay\",\n",
    "    \"prior_inpatient_admissions\",\n",
    "    \"num_medications\",\n",
    "    \"num_lab_procedures\",\n",
    "    \"num_abnormal_labs\",\n",
    "    \"avg_a1c_result\",\n",
    "    \"total_charges\",\n",
    "    \"icu_stay\",\n",
    "]\n",
    "model_data = gold_fact.select(\n",
    "    *[F.coalesce(F.col(name), F.lit(0)).alias(name) for name in feature_columns],\n",
    "    F.col(\"readmit_30d\").alias(\"label\"),\n",
    ")\n",
    "\n",
    "assembler = VectorAssembler(inputCols=feature_columns, outputCol=\"features\")\n",
    "classifier = RandomForestClassifier(\n",
    "    labelCol=\"label\",\n",
    "    featuresCol=\"features\",\n",
    "    numTrees=50,\n",
    "    maxDepth=6,\n",
    "    seed=42,\n",
    ")\n",
    "model_pipeline = Pipeline(stages=[assembler, classifier])\n",
    "train_data, test_data = model_data.randomSplit([0.8, 0.2], seed=42)\n",
    "if train_data.isEmpty() or test_data.isEmpty():\n",
    "    raise ValueError(\"The project dataset must contain enough rows for a train/test split.\")\n",
    "\n",
    "readmission_model = model_pipeline.fit(train_data)\n",
    "test_predictions = readmission_model.transform(test_data)\n",
    "positive_recall = MulticlassClassificationEvaluator(\n",
    "    labelCol=\"label\",\n",
    "    predictionCol=\"prediction\",\n",
    "    metricName=\"recallByLabel\",\n",
    "    metricLabel=1.0,\n",
    ").evaluate(test_predictions)\n",
    "print(f\"Held-out readmission recall (positive class): {positive_recall:.3f}\")\n",
    "display(test_predictions.select(\"label\", \"prediction\", \"probability\").limit(20))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "edd2abae-55dc-4e3d-87fd-b51fd556b6c8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 6. Optional live synthetic vitals stream\n",
    "\n",
    "Change `RUN_LIVE_STREAM` to `True` and run this cell to start a continuous\n",
    "Spark rate-source demo. It writes synthetic readings to a Delta table and\n",
    "displays the updating stream. Stop the cell/stream when finished to release\n",
    "compute. This does not connect to Kafka or use real patient data.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792603210691,
     "inputWidgets": {},
     "nuid": "d82f8550-9c74-4215-8d89-4c1dac922e63",
     "showTitle": false,
     "startTime": 1792603208730,
     "submitTime": 1792603098235,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Live mode is off. Set RUN_LIVE_STREAM = True to enable the continuous demo.\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_time</th><th>patient_hash</th><th>heart_rate</th><th>spo2</th><th>respiratory_rate</th><th>systolic_bp</th></tr></thead><tbody><tr><td>2026-10-21T17:20:09.263Z</td><td>701365665b04cb9951546a898ae2678e05da3cee6ec6905b722b3b089e8304b4</td><td>55</td><td>88.0</td><td>12</td><td>100</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>51acfb7d7ae3e46e8804531df4aa2aba1d8db14947c7de39a86e3e1d7ebf244a</td><td>62</td><td>89.5</td><td>17</td><td>111</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>590ca7d63adb9e6295e56143b47451c4270be07028b425c4056a730e83625a54</td><td>69</td><td>91.0</td><td>22</td><td>122</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>cb40736e7bd172be2cd21361d4c3d3f04fc69a5d9c32479466eef3566711c714</td><td>76</td><td>92.5</td><td>27</td><td>133</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>64257e295995a6eb28af917fcd2c9ca2f453d5bb36167ac6ac61a219d29f1a53</td><td>83</td><td>94.0</td><td>14</td><td>144</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>ce3c398ba81b3bbb02eed1bf75699d0f40828d1efc7e24192b05e3a14ea10e0c</td><td>90</td><td>89.0</td><td>19</td><td>155</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>292f46df559f83bcb1609e5c7569c541fb5845b717906d66da63c31b14883d03</td><td>97</td><td>90.5</td><td>24</td><td>106</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>7afd9a93e8a2b565b4acb157418ca8bb557c553361506b0b3f59c68a2772b1a8</td><td>104</td><td>92.0</td><td>29</td><td>117</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>51ce17b01634bdb1e3a466eab54b45512ca2fe9a51fd94334daefc0eea915ad8</td><td>111</td><td>93.5</td><td>16</td><td>128</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>3f1fd3e9deb77bf4617987106646eccbc225558d2a6372cd66d46814ff7bee51</td><td>118</td><td>88.5</td><td>21</td><td>139</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>3d1be5f2e6e9b254341f28f69228ed5a54cae175da373bb22fb475f20e88523a</td><td>55</td><td>90.0</td><td>26</td><td>150</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>1b190d9db3c740297c35d7cadf76cbf448f9a8dd0d8e517863c647f02f4fd18d</td><td>62</td><td>91.5</td><td>13</td><td>101</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>c5b03c232bc61bceba95a6cd41301e2231bfe3e21ec79d763f52e8ab4cea5ca4</td><td>69</td><td>93.0</td><td>18</td><td>112</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>f7165b58f6eef929049cafcd544b981c86f33bdd15b68ed48659fe0fc85d064e</td><td>76</td><td>88.0</td><td>23</td><td>123</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>1ee980b3ef326a6382e55105cd28af4d1ef3695b99153267d73270dbc8aaf818</td><td>83</td><td>89.5</td><td>28</td><td>134</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>d2dd66879d615423f9e572d3aa99740034ef34bcc566f6039d96956a2c5542c7</td><td>90</td><td>91.0</td><td>15</td><td>145</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>d615ad15f7a0a9e213f667ab902cc8f9c039ef16874c16b7e9a8cc408d8b6bb9</td><td>97</td><td>92.5</td><td>20</td><td>156</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>500881cdb1f71ce9ecb3afcb0f4fcdf48a401931c8f647e7ce7b499ccd564197</td><td>104</td><td>94.0</td><td>25</td><td>107</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>4ccdfa1b22205a3b9ec3165b4e1d9ff9c80677e715484506c153a4b53de69583</td><td>111</td><td>89.0</td><td>12</td><td>118</td></tr><tr><td>2026-10-21T17:20:09.263Z</td><td>58f338280c5108514b457556d083f4e7954fe492bc45582684323c64b41894f4</td><td>118</td><td>90.5</td><td>17</td><td>129</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "2026-10-21T17:20:09.263Z",
         "701365665b04cb9951546a898ae2678e05da3cee6ec6905b722b3b089e8304b4",
         55,
         88.0,
         12,
         100
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "51acfb7d7ae3e46e8804531df4aa2aba1d8db14947c7de39a86e3e1d7ebf244a",
         62,
         89.5,
         17,
         111
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "590ca7d63adb9e6295e56143b47451c4270be07028b425c4056a730e83625a54",
         69,
         91.0,
         22,
         122
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "cb40736e7bd172be2cd21361d4c3d3f04fc69a5d9c32479466eef3566711c714",
         76,
         92.5,
         27,
         133
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "64257e295995a6eb28af917fcd2c9ca2f453d5bb36167ac6ac61a219d29f1a53",
         83,
         94.0,
         14,
         144
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "ce3c398ba81b3bbb02eed1bf75699d0f40828d1efc7e24192b05e3a14ea10e0c",
         90,
         89.0,
         19,
         155
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "292f46df559f83bcb1609e5c7569c541fb5845b717906d66da63c31b14883d03",
         97,
         90.5,
         24,
         106
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "7afd9a93e8a2b565b4acb157418ca8bb557c553361506b0b3f59c68a2772b1a8",
         104,
         92.0,
         29,
         117
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "51ce17b01634bdb1e3a466eab54b45512ca2fe9a51fd94334daefc0eea915ad8",
         111,
         93.5,
         16,
         128
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "3f1fd3e9deb77bf4617987106646eccbc225558d2a6372cd66d46814ff7bee51",
         118,
         88.5,
         21,
         139
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "3d1be5f2e6e9b254341f28f69228ed5a54cae175da373bb22fb475f20e88523a",
         55,
         90.0,
         26,
         150
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "1b190d9db3c740297c35d7cadf76cbf448f9a8dd0d8e517863c647f02f4fd18d",
         62,
         91.5,
         13,
         101
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "c5b03c232bc61bceba95a6cd41301e2231bfe3e21ec79d763f52e8ab4cea5ca4",
         69,
         93.0,
         18,
         112
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "f7165b58f6eef929049cafcd544b981c86f33bdd15b68ed48659fe0fc85d064e",
         76,
         88.0,
         23,
         123
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "1ee980b3ef326a6382e55105cd28af4d1ef3695b99153267d73270dbc8aaf818",
         83,
         89.5,
         28,
         134
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "d2dd66879d615423f9e572d3aa99740034ef34bcc566f6039d96956a2c5542c7",
         90,
         91.0,
         15,
         145
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "d615ad15f7a0a9e213f667ab902cc8f9c039ef16874c16b7e9a8cc408d8b6bb9",
         97,
         92.5,
         20,
         156
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "500881cdb1f71ce9ecb3afcb0f4fcdf48a401931c8f647e7ce7b499ccd564197",
         104,
         94.0,
         25,
         107
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "4ccdfa1b22205a3b9ec3165b4e1d9ff9c80677e715484506c153a4b53de69583",
         111,
         89.0,
         12,
         118
        ],
        [
         "2026-10-21T17:20:09.263Z",
         "58f338280c5108514b457556d083f4e7954fe492bc45582684323c64b41894f4",
         118,
         90.5,
         17,
         129
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "event_time",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "patient_hash",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "heart_rate",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "spo2",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "respiratory_rate",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "systolic_bp",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "RUN_LIVE_STREAM = False\n",
    "VITALS_TABLE = f\"{OUTPUT_SCHEMA}.capstone_live_vitals\"\n",
    "\n",
    "rate_stream = spark.readStream.format(\"rate\").option(\"rowsPerSecond\", 1).load()\n",
    "live_vitals = (\n",
    "    rate_stream\n",
    "    .withColumn(\n",
    "        \"patient_hash\",\n",
    "        F.sha2(F.concat(F.lit(\"DEMO_PATIENT_\"), (F.col(\"value\") % 20).cast(\"string\")), 256),\n",
    "    )\n",
    "    .withColumn(\"heart_rate\", (F.lit(55) + F.pmod(F.col(\"value\") * 7, F.lit(70))).cast(\"int\"))\n",
    "    .withColumn(\"spo2\", F.round(F.lit(88.0) + F.pmod(F.col(\"value\") * 3, F.lit(13)) * 0.5, 1))\n",
    "    .withColumn(\"respiratory_rate\", (F.lit(12) + F.pmod(F.col(\"value\") * 5, F.lit(18))).cast(\"int\"))\n",
    "    .withColumn(\"systolic_bp\", (F.lit(100) + F.pmod(F.col(\"value\") * 11, F.lit(60))).cast(\"int\"))\n",
    "    .select(\n",
    "        F.col(\"timestamp\").alias(\"event_time\"),\n",
    "        \"patient_hash\",\n",
    "        \"heart_rate\",\n",
    "        \"spo2\",\n",
    "        \"respiratory_rate\",\n",
    "        \"systolic_bp\",\n",
    "    )\n",
    ")\n",
    "\n",
    "if RUN_LIVE_STREAM:\n",
    "    live_query = (\n",
    "        live_vitals.writeStream\n",
    "        .format(\"delta\")\n",
    "        .outputMode(\"append\")\n",
    "        .option(\"checkpointLocation\", f\"{CHECKPOINT_ROOT}/live_vitals\")\n",
    "        .toTable(VITALS_TABLE)\n",
    "    )\n",
    "    print(f\"Streaming to {VITALS_TABLE}; stop the query when done.\")\n",
    "    display(spark.readStream.table(VITALS_TABLE))\n",
    "else:\n",
    "    demo_vitals = (\n",
    "        spark.range(20)\n",
    "        .withColumn(\"event_time\", F.current_timestamp())\n",
    "        .withColumn(\n",
    "            \"patient_hash\",\n",
    "            F.sha2(F.concat(F.lit(\"DEMO_PATIENT_\"), F.col(\"id\").cast(\"string\")), 256),\n",
    "        )\n",
    "        .withColumn(\"heart_rate\", (F.lit(55) + F.pmod(F.col(\"id\") * 7, F.lit(70))).cast(\"int\"))\n",
    "        .withColumn(\"spo2\", F.round(F.lit(88.0) + F.pmod(F.col(\"id\") * 3, F.lit(13)) * 0.5, 1))\n",
    "        .withColumn(\"respiratory_rate\", (F.lit(12) + F.pmod(F.col(\"id\") * 5, F.lit(18))).cast(\"int\"))\n",
    "        .withColumn(\"systolic_bp\", (F.lit(100) + F.pmod(F.col(\"id\") * 11, F.lit(60))).cast(\"int\"))\n",
    "        .drop(\"id\")\n",
    "    )\n",
    "    print(\"Live mode is off. Set RUN_LIVE_STREAM = True to enable the continuous demo.\")\n",
    "    display(demo_vitals)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "2321e545-b707-47a1-b15e-b28306a9255b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 7. Dashboard queries\n",
    "\n",
    "Use the Databricks SQL editor or notebook visualizations to chart the tables:\n",
    "`workspace.healthcare_capstone.capstone_gold_readmission_kpis` and\n",
    "`workspace.healthcare_capstone.capstone_live_vitals`. The live table is populated only while\n",
    "the optional stream is running.\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "6"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "Healthcare_Patient_Vitals_Master",
   "widgets": {}
  },
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}