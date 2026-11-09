# 📦 Product Review Data Processing Using Apache Pig & Hadoop MapReduce
### Big Data Analytics of Amazon Digital Video Game & Product Reviews

[![Hadoop](https://img.shields.io/badge/Engine-Apache%20Hadoop%203.x-yellow.svg)](https://hadoop.apache.org/)
[![Apache Pig](https://img.shields.io/badge/Language-Pig%20Latin-orange.svg)](https://pig.apache.org/)
[![MapReduce](https://img.shields.io/badge/Design%20Pattern-Java%20MapReduce-red.svg)](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
[![Platform](https://img.shields.io/badge/Platform-WSL%20%7C%20Linux%20%7C%20Windows-blue.svg)]()
[![Status](https://img.shields.io/badge/Status-Completed%20%26%20Verified-brightgreen.svg)]()

> Cloned from and enhanced based on: [Big-Data-Analysis-of-Amazon-Digital-Video-Game-Reviews-Using-Hadoop-MapReduce-and-Apache-Pig](https://github.com/Shadman-Al-Muksit/Big-Data-Analysis-of-Amazon-Digital-Video-Game-Reviews-Using-Hadoop-MapReduce-and-Apache-Pig.git)

---

## 🎯 Project Overview

This project implements an enterprise-grade Big Data pipeline for processing and extracting insights from the **Amazon Customer Reviews Big Dataset** using **Apache Pig (Pig Latin)** and **Hadoop MapReduce (Java)**.

It processes large-scale semi-structured tab-delimited review logs to analyze product quality, customer sentiment trends, review helpfulness metrics, and identify top-performing vs underperforming product lines.

---

## 📁 Repository Structure

```text
product-review-pig/
├── data/
│   ├── amazon_reviews_us_Digital_Video_Games_v1_00.tsv # 15-column Amazon Review dataset
│   ├── sample_reviews.tsv                             # Multi-category e-commerce TSV dataset
│   └── sample_reviews_noheader.tsv                    # Headerless dataset for strict HDFS loading
├── scripts/
│   ├── amazon_pig_analysis.pig                        # Master Pig script covering all 7 analytical tasks
│   ├── 01_data_cleaning.pig                           # Data cleansing & verification filter
│   ├── 02_product_summary.pig                         # Product-level review stats & aggregations
│   ├── 03_top_and_bottom_products.pig                 # Leaderboards & quality control alerts
│   ├── 04_category_analysis.pig                       # Category benchmarks
│   ├── 05_helpfulness_analysis.pig                    # Review helpfulness ratio calculations
│   └── run_complete_pipeline.pig                      # Master general pipeline script
├── mapreduce/
│   ├── src/
│   │   ├── GameStats.java                             # MR Job 1: Max Rating, Helpful & Total Votes by Product
│   │   ├── MovingAverage.java                         # MR Job 2: Moving Average Rating over time
│   │   └── TopReviewer.java                           # MR Job 3: Best Reviewer by published review count
│   └── execution.bash                                 # Bash script for compiling & running on Hadoop / WSL
├── outputs/                                           # Final consolidated output text reports
│   ├── GameStats_Output.txt
│   ├── MovingAverage_Output.txt
│   ├── TopReviewer_Output.txt
│   ├── Pig_Task1_TopFranchises.txt
│   ├── Pig_Task2_TopRatedReviews.txt
│   ├── Pig_Task3_TopRatedFranchises.txt
│   ├── Pig_Task4_MostHelpful.txt
│   ├── Pig_Task5_MostInteracted.txt
│   ├── Pig_Task6_Underperforming.txt
│   └── Pig_Task7_HelpfulnessRatio.txt
├── tools/
│   ├── pig_runner.py                                  # Cross-platform Pig engine (Native or Simulation)
│   └── mapreduce_runner.py                            # Standalone MapReduce execution engine
├── docs/
│   ├── PROJECT_REPORT.md                              # Comprehensive technical & academic project report
│   └── VIVA_QUESTIONS.md                              # 20+ Viva & technical interview Q&A
├── reference_repo/                                    # Cloned original upstream repository
└── README.md                                          # Master documentation
```

---

## 📋 Amazon Review Schema (15 TSV Columns)

| Field Name | Pig Data Type | Description |
| :--- | :--- | :--- |
| `marketplace` | `chararray` | 2-letter country code (`US`) |
| `customer_id` | `chararray` | Random identifier tracking author reviews |
| `review_id` | `chararray` | Unique review ID (e.g. `R1WFOQ3N9BO65I`) |
| `product_id` | `chararray` | Unique Amazon Standard ID (e.g. `B00F4CEHNK`) |
| `product_parent` | `chararray` | Franchise/parent identifier aggregating variants |
| `product_title` | `chararray` | Descriptive title of the game/product |
| `product_category`| `chararray`| Broad category (`Digital_Video_Games`) |
| `star_rating` | `float` | Customer rating score (1.0 to 5.0) |
| `helpful_votes` | `int` | Count of helpful votes |
| `total_votes` | `int` | Total customer engagement votes |
| `vine` | `chararray` | Amazon Vine voice reviewer program (`Y`/`N`) |
| `verified_purchase`| `chararray`| Verified customer purchase confirmation (`Y`/`N`) |
| `review_headline`| `chararray` | Short title / headline of the review |
| `review_body` | `chararray` | Full customer review text |
| `review_date` | `chararray` | Date review was submitted (`YYYY-MM-DD`) |

---

## 🔬 Analytical Tasks Implemented

### 🐷 Apache Pig Tasks (`amazon_pig_analysis.pig`)
1. **Task 1: Top 5 Product Franchises by Review Count** (`GROUP BY product_parent`, `COUNT`, `TOP(5, 2, ...)`).
2. **Task 2: Top 10 Rated Individual Reviews** (`TOP(10, 7, clean_data)`).
3. **Task 3: Top 10 Highest Rated Franchises** (`AVG(star_rating)`, volume threshold).
4. **Task 4: Top 10 Most Helpful Reviews** (`TOP(10, 8, clean_data)`).
5. **Task 5: Top 10 Most Interacted Franchises** (`SUM(total_votes)`).
6. **Task 6: Underperforming / Low Rated Products** (Identifies products $\le 2.5$ stars needing quality recall).
7. **Task 7: Review Helpfulness Ratio & Social Proof** ($\frac{\text{helpful\_votes}}{\text{total\_votes}} \ge 0.85$ and $\ge 20$ votes).

### ☕ Hadoop MapReduce Jobs (Java)
1. **`GameStats.java`**: Calculates Maximum Rating, Total Helpful Votes, and Total Votes per Product ID.
2. **`MovingAverage.java`**: Computes sequential progressive average rating evolution over time per Product ID.
3. **`TopReviewer.java`**: Employs an In-Reducer tracking pattern to output the single most active customer reviewer.

---

## 🚀 Execution Guide

### Option 1: Instant Local Execution (Windows / Mac / Linux without Hadoop)
Run the built-in execution engines:

```powershell
cd "C:\Users\jagad\.gemini\antigravity\scratch\product-review-pig"

# Run Pig pipeline
python tools/pig_runner.py

# Run MapReduce jobs
python tools/mapreduce_runner.py
```

### Option 2: Running Apache Pig on Hadoop / WSL / Linux Cluster
```bash
# Ingest data to HDFS
hdfs dfs -mkdir -p /amazon/
hdfs dfs -put -f data/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/

# Run in Local Mode
pig -x local scripts/amazon_pig_analysis.pig

# Or run in MapReduce Cluster Mode
pig -x mapreduce scripts/amazon_pig_analysis.pig
```

### Option 3: Compiling & Running Java MapReduce Jobs
```bash
cd mapreduce
bash execution.bash
```
*Or manually:*
```bash
export HADOOP_CLASSPATH=$(hadoop classpath)
javac -classpath ${HADOOP_CLASSPATH} -d . src/GameStats.java src/MovingAverage.java src/TopReviewer.java
jar -cvf amazon_jobs.jar *.class
hadoop jar amazon_jobs.jar GameStats /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_stats
hadoop jar amazon_jobs.jar MovingAverage /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_avg
hadoop jar amazon_jobs.jar TopReviewer /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_best
```

---

## 📊 Sample Execution Outputs

### Pig Task 1: Top Franchises by Review Volume
```text
341969535	Xbox Live Gift Card	4
384246568	Playstation Network Card	4
402222185	FIFA 24 Ultimate Edition	4
232803743	Cyber Warrior RPG	4
695277014	Civilization VI Digital Deluxe	3
```

### Pig Task 3: Top Rated Franchises (Average Rating)
```text
395682204	Minecraft Java & Bedrock	5.0	3
341969535	Xbox Live Gift Card	4.75	4
384246568	Playstation Network Card	4.75	4
695277014	Civilization VI Digital Deluxe	4.67	3
955209542	Grand Theft Auto V Online	4.67	3
```

### Pig Task 6: Underperforming Flagged Products
```text
232803743	Cyber Warrior RPG	1.25	4
402222185	FIFA 24 Ultimate Edition	2.5	4
```

### MapReduce: GameStats Output
```text
B001KC01WO	MaxRate: 5.0	HelpfulVotes: 81	TotalVotes: 88
B002LIT9EC	MaxRate: 5.0	HelpfulVotes: 88	TotalVotes: 95
B003LIT9FC	MaxRate: 5.0	HelpfulVotes: 146	TotalVotes: 153
B004RMK4BC	MaxRate: 5.0	HelpfulVotes: 90	TotalVotes: 97
B008139R8A	MaxRate: 3.0	HelpfulVotes: 94	TotalVotes: 112
B009140S9B	MaxRate: 2.0	HelpfulVotes: 155	TotalVotes: 169
B00F4CEHNK	MaxRate: 5.0	HelpfulVotes: 70	TotalVotes: 74
```

### MapReduce: Moving Average Output
```text
B001KC01WO	Moving Averages: 5.0, 4.5, 4.67
B002LIT9EC	Moving Averages: 5.0, 5.0, 4.67
B003LIT9FC	Moving Averages: 5.0, 5.0, 5.0
B004RMK4BC	Moving Averages: 5.0, 4.5, 4.67, 4.75
B008139R8A	Moving Averages: 3.0, 2.5, 2.33, 2.5
B009140S9B	Moving Averages: 1.0, 1.0, 1.33, 1.25
B00F4CEHNK	Moving Averages: 5.0, 5.0, 5.0, 4.75
```

### MapReduce: Top Reviewer Output
```text
Best Reviewer ID: 21925455	7
```

---

## 📚 Technical Documentation & Viva Resources

- 📄 **[Comprehensive Project Report](docs/PROJECT_REPORT.md)**: Academic report detailing system architecture, MapReduce vs Pig paradigms, schema definitions, and analytical conclusions.
- 🎓 **[Viva & Technical Interview Guide](docs/VIVA_QUESTIONS.md)**: 20+ comprehensive interview questions covering MapReduce internals, Pig Latin UDFs, `TOP()` function, and Big Data design patterns.
