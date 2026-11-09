#!/usr/bin/env python3
"""
=============================================================================
TOOL: pig_runner.py
PROJECT: Product Review Data Processing Using Pig (Amazon Customer Reviews)
DESCRIPTION:
  Cross-platform Pig execution & simulation engine.
  Executes Pig Latin relational transformations on both:
    1. Amazon Video Games & Products Reviews (15-column schema)
    2. Multi-Category General Product Reviews (10-column schema)

  Performs the exact 5 tasks from the Amazon Reviews project:
    - Task 1: Top 5 Game Franchises (Product Parents) by Review Count
    - Task 2: Top 10 Rated Individual Reviews
    - Task 3: Top 10 Rated Game Franchises (Average Rating)
    - Task 4: Top 10 Most Helpful Individual Reviews
    - Task 5: Top 10 Most Interacted Game Franchises (Total Votes)
  Along with enhanced Product Review Processing tasks:
    - Task 6: Flagged Underperforming / Defective Products (Quality Control)
    - Task 7: Review Helpfulness Ratio & Social Proof Mining

  Outputs are saved to 'output/' in Hadoop part-r-00000 format.
=============================================================================
"""

import os
import sys
import shutil
import subprocess
from collections import defaultdict

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AMAZON_DATA_FILE = os.path.join(PROJECT_DIR, "data", "amazon_reviews_us_Digital_Video_Games_v1_00.tsv")
GENERAL_DATA_FILE = os.path.join(PROJECT_DIR, "data", "sample_reviews.tsv")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")


def check_native_pig():
    return shutil.which("pig")


def write_pig_output(folder_name, rows):
    target_folder = os.path.join(OUTPUT_DIR, folder_name)
    os.makedirs(target_folder, exist_ok=True)
    part_file = os.path.join(target_folder, "part-r-00000")
    with open(part_file, "w", encoding="utf-8") as f:
        for row in rows:
            f.write("\t".join(str(item) for item in row) + "\n")
    return part_file


def run_amazon_pig_pipeline():
    print("=" * 85)
    print(" APACHE PIG PIPELINE: AMAZON DIGITAL VIDEO GAMES & PRODUCT REVIEWS")
    print("=" * 85)
    print(f"Data Source: {AMAZON_DATA_FILE}")

    if not os.path.exists(AMAZON_DATA_FILE):
        print(f"[ERROR] Amazon data file not found: {AMAZON_DATA_FILE}")
        return

    records = []
    with open(AMAZON_DATA_FILE, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            cols = line.strip().split("\t")
            if idx == 0 or cols[0] == "marketplace":
                continue
            if len(cols) >= 15:
                try:
                    records.append({
                        "marketplace": cols[0],
                        "customer_id": cols[1],
                        "review_id": cols[2],
                        "product_id": cols[3],
                        "product_parent": cols[4],
                        "product_title": cols[5],
                        "product_category": cols[6],
                        "star_rating": float(cols[7]),
                        "helpful_votes": int(cols[8]),
                        "total_votes": int(cols[9]),
                        "vine": cols[10],
                        "verified_purchase": cols[11],
                        "review_headline": cols[12],
                        "review_body": cols[13],
                        "review_date": cols[14]
                    })
                except ValueError:
                    continue

    print(f"Loaded {len(records)} clean Amazon review records for Pig Latin execution.\n")

    # -------------------------------------------------------------------------
    # TASK 1: Top 5 Game Franchises (Product Parents) by Review Count
    # -------------------------------------------------------------------------
    franchise_counts = defaultdict(lambda: {"count": 0, "title": ""})
    for r in records:
        key = r["product_parent"]
        franchise_counts[key]["count"] += 1
        franchise_counts[key]["title"] = r["product_title"]

    top_5_task1 = sorted(franchise_counts.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
    task1_rows = [[k, v["title"], v["count"]] for k, v in top_5_task1]
    write_pig_output("pig_task1_top_franchises", task1_rows)

    print("-" * 85)
    print(" [TASK 1] Top 5 Product Franchises (Product Parents) by Review Count")
    print("-" * 85)
    for rank, (parent_id, info) in enumerate(top_5_task1, start=1):
        print(f" #{rank}  Franchise Parent ID: {parent_id:<12} | Title: {info['title'][:32]:<32} | Reviews: {info['count']}")

    # -------------------------------------------------------------------------
    # TASK 2: Top 10 Rated Individual Reviews
    # -------------------------------------------------------------------------
    top_10_reviews = sorted(records, key=lambda x: (x["star_rating"], x["helpful_votes"]), reverse=True)[:10]
    task2_rows = [
        [r["marketplace"], r["customer_id"], r["review_id"], r["product_id"], r["product_parent"],
         r["product_title"], r["product_category"], r["star_rating"], r["helpful_votes"], r["total_votes"],
         r["vine"], r["verified_purchase"], r["review_headline"], r["review_body"], r["review_date"]]
        for r in top_10_reviews
    ]
    write_pig_output("pig_task2_top_rated_reviews", task2_rows)

    print("\n" + "-" * 85)
    print(" [TASK 2] Top 10 Rated Individual Reviews (Sample)")
    print("-" * 85)
    for r in top_10_reviews[:4]:
        print(f" [{r['review_id']}] {r['product_title'][:28]:<28} | Rating: {r['star_rating']}* | Helpful: {r['helpful_votes']}/{r['total_votes']}")
        print(f"   Headline: \"{r['review_headline']}\"")

    # -------------------------------------------------------------------------
    # TASK 3: Top 10 Rated Game Franchises (Average Rating)
    # -------------------------------------------------------------------------
    franchise_ratings = defaultdict(lambda: {"sum": 0.0, "count": 0, "title": ""})
    for r in records:
        key = r["product_parent"]
        franchise_ratings[key]["sum"] += r["star_rating"]
        franchise_ratings[key]["count"] += 1
        franchise_ratings[key]["title"] = r["product_title"]

    avg_franchise_list = []
    for parent_id, data in franchise_ratings.items():
        avg_score = round(data["sum"] / data["count"], 2)
        avg_franchise_list.append((parent_id, data["title"], avg_score, data["count"]))

    # Sort descending by avg_rating, then review count
    top_rated_franchises = sorted(avg_franchise_list, key=lambda x: (x[2], x[3]), reverse=True)
    task3_rows = [[item[0], item[1], item[2], item[3]] for item in top_rated_franchises]
    write_pig_output("pig_task3_top_rated_franchises", task3_rows)

    print("\n" + "-" * 85)
    print(" [TASK 3] Top Rated Product Franchises (Average Rating Benchmark)")
    print("-" * 85)
    for rank, (pid, title, avg_s, cnt) in enumerate(top_rated_franchises[:5], start=1):
        print(f" #{rank}  [{pid}] {title[:32]:<32} | Avg Rating: {avg_s} / 5.0  ({cnt} reviews)")

    # -------------------------------------------------------------------------
    # TASK 4: Top 10 Most Helpful Individual Reviews
    # -------------------------------------------------------------------------
    most_helpful = sorted(records, key=lambda x: x["helpful_votes"], reverse=True)[:10]
    task4_rows = [
        [r["review_id"], r["product_title"], r["star_rating"], r["helpful_votes"], r["total_votes"], r["review_headline"], r["review_body"]]
        for r in most_helpful
    ]
    write_pig_output("pig_task4_most_helpful_reviews", task4_rows)

    print("\n" + "-" * 85)
    print(" [TASK 4] Top 10 Most Helpful Individual Reviews")
    print("-" * 85)
    for r in most_helpful[:4]:
        print(f" [{r['review_id']}] {r['product_title'][:28]:<28} | Helpful: {r['helpful_votes']}/{r['total_votes']} votes | Rating: {r['star_rating']}*")
        print(f"   Feedback: \"{r['review_body'][:75]}...\"")

    # -------------------------------------------------------------------------
    # TASK 5: Top 10 Most Interacted Game Franchises (Total Votes Sum)
    # -------------------------------------------------------------------------
    franchise_votes = defaultdict(lambda: {"total_votes": 0, "title": ""})
    for r in records:
        key = r["product_parent"]
        franchise_votes[key]["total_votes"] += r["total_votes"]
        franchise_votes[key]["title"] = r["product_title"]

    top_interacted = sorted(franchise_votes.items(), key=lambda x: x[1]["total_votes"], reverse=True)[:10]
    task5_rows = [[k, v["title"], v["total_votes"]] for k, v in top_interacted]
    write_pig_output("pig_task5_most_interacted_franchises", task5_rows)

    print("\n" + "-" * 85)
    print(" [TASK 5] Top 10 Most Interacted Product Franchises (Total Votes)")
    print("-" * 85)
    for rank, (parent_id, info) in enumerate(top_interacted[:5], start=1):
        print(f" #{rank}  Franchise ID: {parent_id:<12} | Title: {info['title'][:32]:<32} | Total Votes: {info['total_votes']}")

    # -------------------------------------------------------------------------
    # TASK 6: Underperforming Products (Rating <= 2.5)
    # -------------------------------------------------------------------------
    low_rated = [f for f in avg_franchise_list if f[2] <= 2.5]
    low_rated_sorted = sorted(low_rated, key=lambda x: x[2])
    task6_rows = [[item[0], item[1], item[2], item[3]] for item in low_rated_sorted]
    write_pig_output("pig_task6_underperforming_franchises", task6_rows)

    print("\n" + "-" * 85)
    print(" [TASK 6] Underperforming Products Alert (Rating <= 2.5 Flagged for Quality Audit)")
    print("-" * 85)
    for item in low_rated_sorted:
        print(f" [ALERT] Franchise ID: {item[0]} | Title: {item[1]} | Low Avg: {item[2]} / 5.0")

    # -------------------------------------------------------------------------
    # TASK 7: Review Helpfulness Ratio & Social Proof
    # -------------------------------------------------------------------------
    high_trust = []
    for r in records:
        ratio = round(r["helpful_votes"] / r["total_votes"], 2) if r["total_votes"] > 0 else 0.0
        if r["total_votes"] >= 20 and ratio >= 0.85:
            high_trust.append((r["review_id"], r["product_title"], r["star_rating"], r["helpful_votes"], r["total_votes"], ratio, r["review_body"]))

    high_trust_sorted = sorted(high_trust, key=lambda x: x[3], reverse=True)
    task7_rows = [[item[0], item[1], item[2], item[3], item[4], item[5], item[6]] for item in high_trust_sorted]
    write_pig_output("pig_task7_helpfulness_ratio", task7_rows)

    print("\n" + "=" * 85)
    print(f" [SUCCESS] All 7 Pig Latin tasks generated outputs in: {OUTPUT_DIR}")
    print("=" * 85)


def main():
    pig_bin = check_native_pig()
    if pig_bin:
        print(f"[FOUND] Apache Pig CLI found at: {pig_bin}")
        print("Executing script with native Apache Pig...")
        script_path = os.path.join(PROJECT_DIR, "scripts", "amazon_pig_analysis.pig")
        subprocess.run(["pig", "-x", "local", script_path], cwd=PROJECT_DIR)
    else:
        print("[NOTICE] 'pig' binary not on system PATH. Executing Pig simulation engine...")
        run_amazon_pig_pipeline()


if __name__ == "__main__":
    main()
