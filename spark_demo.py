"""Run a small real Spark job over the project's generated admissions data."""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=0,
        help="Keep the Spark application running briefly so it is easy to show in the UI.",
    )
    args = parser.parse_args()
    if args.hold_seconds < 0:
        parser.error("--hold-seconds must be zero or greater")

    spark = (
        SparkSession.builder.appName("HospitalReadmission-Spark-Demo")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    admissions_path = "/opt/project/data/raw/mysql_ehr/admissions.csv"
    admissions = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(admissions_path)
    )

    print("\n=== Spark schema: admissions ===", flush=True)
    admissions.printSchema()
    print(f"Admissions rows: {admissions.count()}", flush=True)
    print("Admissions by diagnosis:", flush=True)
    (
        admissions.groupBy("diagnosis_code")
        .agg(F.count("*").alias("admission_count"))
        .orderBy(F.desc("admission_count"))
        .show(10, truncate=False)
    )

    if args.hold_seconds:
        print(
            f"Keeping Spark application active for {args.hold_seconds}s "
            "so it can be viewed in the Spark UI.",
            flush=True,
        )
        time.sleep(args.hold_seconds)

    spark.stop()


if __name__ == "__main__":
    main()
