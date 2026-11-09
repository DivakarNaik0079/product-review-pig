#!/usr/bin/env bash
# =============================================================================
# SCRIPT: mapreduce/execution.bash
# DESCRIPTION:
#   Compiles MapReduce Java source files into amazon_jobs.jar
#   and executes GameStats, MovingAverage, and TopReviewer on Hadoop cluster / WSL.
# =============================================================================

set -e

echo "=== 1. Setting Hadoop Classpath ==="
export HADOOP_CLASSPATH=$(hadoop classpath)

echo "=== 2. Compiling Java Source Files ==="
javac -classpath "${HADOOP_CLASSPATH}" -d . src/GameStats.java src/MovingAverage.java src/TopReviewer.java

echo "=== 3. Packaging into amazon_jobs.jar ==="
jar -cvf amazon_jobs.jar *.class

echo "=== 4. Setting Up HDFS Directories ==="
hdfs dfs -mkdir -p /amazon/
hdfs dfs -put -f ../data/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/

echo "=== 5. Executing Job 1: GameStats ==="
hdfs dfs -rm -r -f /amazon/output_stats
hadoop jar amazon_jobs.jar GameStats /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_stats
hdfs dfs -cat /amazon/output_stats/part*

echo "=== 6. Executing Job 2: MovingAverage ==="
hdfs dfs -rm -r -f /amazon/output_avg
hadoop jar amazon_jobs.jar MovingAverage /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_avg
hdfs dfs -cat /amazon/output_avg/part*

echo "=== 7. Executing Job 3: TopReviewer ==="
hdfs dfs -rm -r -f /amazon/output_best
hadoop jar amazon_jobs.jar TopReviewer /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_best
hdfs dfs -cat /amazon/output_best/part*

echo "=== MapReduce Jobs Execution Complete ==="
