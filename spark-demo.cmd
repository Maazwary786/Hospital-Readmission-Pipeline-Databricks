@echo off
setlocal
cd /d "%~dp0"
color 0B
title Hospital Readmission - Live Spark Job
echo Submitting the project's admissions aggregation to the Spark cluster...
echo Keep this terminal open during the presentation.
echo Spark application UI: http://localhost:4040
echo Spark master UI:      http://localhost:8081
echo.
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/project/spark_demo.py --hold-seconds 180
if errorlevel 1 (
    color 0C
    echo.
    echo Spark job failed. Check that Docker Desktop and the Spark services are running.
) else (
    color 0A
    echo.
    echo Spark job completed successfully.
)
echo.
pause
