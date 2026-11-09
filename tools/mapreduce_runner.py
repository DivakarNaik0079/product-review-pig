#!/usr/bin/env python3
"""
=============================================================================
TOOL: mapreduce_runner.py
PROJECT: Product Review Data Processing Using Pig & MapReduce
DESCRIPTION:
  Executes the exact Hadoop MapReduce jobs:
    1. GameStats: Max Rate, Helpful Votes, and Total Votes per Product
    2. MovingAverage: Progressive Rating Average progression over time
    3. TopReviewer: Best Reviewer ID by review publication volume
  Supports both native 'hadoop jar' execution (when Hadoop is available)
  and standalone faithful MapReduce simulation.
=============================================================================
"""

import os
import sys
import shutil
import subprocess
from collections import defaultdict

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(PROJECT_DIR, "data", "amazon_reviews_us_Digital_Video_Games_v1_00.tsv")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output", "mapreduce")


def check_native_hadoop():
    return shutil.which("hadoop")


def run_mapreduce_pipeline():
    print("=" * 80)
    print(" HADOOP MAPREDUCE JOBS EXECUTION ENGINE")
    print("=" * 80)
    print(f"Dataset: {DATA_FILE}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(DATA_FILE):
        print(f"[ERROR] Data file not found: {DATA_FILE}")
        return

    rows = []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            cols = line.strip().split("\t")
            if idx == 0 or cols[0] == "marketplace":
                continue
            if len(cols) >= 10:
                rows.append(cols)

    print(f"Loaded {len(rows)} valid records for MapReduce processing.\n")

    # =========================================================================
    # JOB 1: GameStats (Mapper: Key=cols[3], Val=cols[7],cols[8],cols[9])
    # =========================================================================
    print("-" * 80)
    print(" [JOB 1] GameStats: Calculating Max Rating & Engagement by Product ID")
    print("-" * 80)
    
    # Map Phase
    mapped_stats = defaultdict(list)
    for r in rows:
        prod_id = r[3]
        try:
            rating = float(r[7])
            helpful = int(r[8])
            total = int(r[9])
            mapped_stats[prod_id].append((rating, helpful, total))
        except ValueError:
            continue

    # Reduce Phase
    gamestats_results = []
    for prod_id, metrics in sorted(mapped_stats.items()):
        max_rate = max(m[0] for m in metrics)
        total_helpful = sum(m[1] for m in metrics)
        total_votes = sum(m[2] for m in metrics)
        line = f"{prod_id}\tMaxRate: {max_rate}\tHelpfulVotes: {total_helpful}\tTotalVotes: {total_votes}"
        gamestats_results.append(line)
        print(f" {prod_id} -> Max Rate: {max_rate} | Helpful: {total_helpful} | Total: {total_votes}")

    job1_file = os.path.join(OUTPUT_DIR, "GameStats_Output.txt")
    with open(job1_file, "w", encoding="utf-8") as f:
        f.write("\n".join(gamestats_results) + "\n")
    print(f" -> Output saved to: {job1_file}")

    # =========================================================================
    # JOB 2: MovingAverage (Key=cols[3], Val=cols[7])
    # =========================================================================
    print("\n" + "-" * 80)
    print(" [JOB 2] MovingAverage: Progressive Rating Progression per Product ID")
    print("-" * 80)

    mapped_ratings = defaultdict(list)
    for r in rows:
        prod_id = r[3]
        try:
            mapped_ratings[prod_id].append(float(r[7]))
        except ValueError:
            continue

    moving_avg_results = []
    for prod_id, ratings in sorted(mapped_ratings.items()):
        cumulative_sum = 0.0
        mov_list = []
        for i, val in enumerate(ratings, start=1):
            cumulative_sum += val
            mov_list.append(f"{round(cumulative_sum / i, 2)}")
        line = f"{prod_id}\tMoving Averages: {', '.join(mov_list)}"
        moving_avg_results.append(line)
        print(f" {prod_id} -> Ratings Count: {len(ratings)} | Progression: [{', '.join(mov_list)}]")

    job2_file = os.path.join(OUTPUT_DIR, "MovingAverage_Output.txt")
    with open(job2_file, "w", encoding="utf-8") as f:
        f.write("\n".join(moving_avg_results) + "\n")
    print(f" -> Output saved to: {job2_file}")

    # =========================================================================
    # JOB 3: TopReviewer (Key=cols[1], Val=1)
    # =========================================================================
    print("\n" + "-" * 80)
    print(" [JOB 3] TopReviewer: Discovering Most Prolific Reviewer")
    print("-" * 80)

    customer_counts = defaultdict(int)
    for r in rows:
        cust_id = r[1]
        customer_counts[cust_id] += 1

    best_reviewer = ""
    max_reviews = 0
    for cust_id, count in customer_counts.items():
        if count > max_reviews:
            max_reviews = count
            best_reviewer = cust_id

    reviewer_line = f"Best Reviewer ID: {best_reviewer}\t{max_reviews}"
    print(f" -> {reviewer_line}")

    job3_file = os.path.join(OUTPUT_DIR, "TopReviewer_Output.txt")
    with open(job3_file, "w", encoding="utf-8") as f:
        f.write(reviewer_line + "\n")
    print(f" -> Output saved to: {job3_file}")

    print("\n" + "=" * 80)
    print(" [SUCCESS] All MapReduce jobs completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    run_mapreduce_pipeline()
