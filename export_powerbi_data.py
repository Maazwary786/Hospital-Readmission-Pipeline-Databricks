import os
import sys
import pandas as pd

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "power_bi_export")
os.makedirs(EXPORT_DIR, exist_ok=True)

# 1. Load Gold Fact & ML Inferences
df_fact = pd.read_csv(os.path.join(EXPORT_DIR, "fact_admissions_all.csv"))
df_inf = pd.read_csv(os.path.join(EXPORT_DIR, "gold_readmission_inferences.csv"))

# Merge ML scores into fact
df_combined = df_fact.merge(df_inf[["admission_id", "readmission_risk_score", "risk_tier", "predicted_readmit"]], on="admission_id", how="left")

# Save combined Master Analytics table for Power BI (Super easy: 1 file with everything!)
df_combined.to_csv(os.path.join(EXPORT_DIR, "healthcare_powerbi_master.csv"), index=False)

# 2. Pre-calculated KPI Aggregations (Ready-to-chart)
kpi_utilization = df_combined.groupby("prior_inpatient_admissions").agg(
    total_patients=("admission_id", "count"),
    readmitted_count=("readmit_30d", "sum"),
    avg_length_of_stay=("length_of_stay", "mean"),
    avg_total_charges=("total_charges", "mean")
).reset_index()
kpi_utilization["readmission_rate_pct"] = (kpi_utilization["readmitted_count"] / kpi_utilization["total_patients"] * 100).round(1)
kpi_utilization.to_csv(os.path.join(EXPORT_DIR, "kpi_readmission_by_utilization.csv"), index=False)

kpi_diagnosis = df_combined.groupby("diagnosis_code").agg(
    total_patients=("admission_id", "count"),
    readmitted_count=("readmit_30d", "sum"),
    avg_los=("length_of_stay", "mean"),
    icu_stay_count=("icu_stay", "sum")
).reset_index()
kpi_diagnosis["readmission_rate_pct"] = (kpi_diagnosis["readmitted_count"] / kpi_diagnosis["total_patients"] * 100).round(1)
kpi_diagnosis.to_csv(os.path.join(EXPORT_DIR, "kpi_los_by_diagnosis.csv"), index=False)

print("✅ Power BI Export Dataset Ready:")
print(f"   • healthcare_powerbi_master.csv ({len(df_combined)} rows with Bed Occupancy, LOS, Readmission Risk & ML scores)")
print(f"   • kpi_readmission_by_utilization.csv")
print(f"   • kpi_los_by_diagnosis.csv")
