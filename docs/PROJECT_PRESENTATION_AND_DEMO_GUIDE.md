# 🎯 Master Project Presentation & Demonstration Guide
## Big Data Analysis of Amazon Product Reviews Using Apache Pig & Hadoop MapReduce
**Author / Presenter:** Divakar Naik  
**GitHub Repository:** [https://github.com/DivakarNaik0079/product-review-pig](https://github.com/DivakarNaik0079/product-review-pig)  
**Live Demo:** [https://divakarnaik0079.github.io/product-review-pig/](https://divakarnaik0079.github.io/product-review-pig/)

---

## 📑 Table of Contents
1. [The 60-Second Elevator Pitch](#1-the-60-second-elevator-pitch)
2. [Presentation Structure (5-Minute & 10-Minute Formats)](#2-presentation-structure)
3. [Step-by-Step Live Demo Walkthrough Script](#3-step-by-step-live-demo-walkthrough-script)
4. [Explaining the Code: Pig Latin & MapReduce](#4-explaining-the-code)
5. [Key Insights & Business Value](#5-key-insights--business-value)
6. [Defense Strategies for Tough Evaluators](#6-defense-strategies-for-tough-evaluators)

---

## 1. The 60-Second Elevator Pitch
> *"Good morning/afternoon, Respected Evaluators.  
> My project is **Big Data Analysis of Amazon Product Reviews Using Apache Pig and Hadoop MapReduce**.  
> In modern e-commerce, customer reviews are generated at massive scale. Raw review data is noisy, semi-structured, and voluminous, making traditional SQL databases slow and expensive.  
> In this project, I built a dual-engine Big Data analytics pipeline:  
> 1. First, I used **Apache Pig** and Pig Latin to execute high-level dataflow transformations: filtering corrupted records, ranking top-tier product franchises, calculating review helpfulness ratios, and flagging defective products with low star ratings.  
> 2. Second, I implemented custom **Hadoop MapReduce jobs in Java** to handle specialized analytical patterns: computing chronological moving rating averages and using In-Reducer tracking to find the most prolific reviewer.  
> I have also deployed a live, interactive web dashboard showcasing all the analytics, real-time charts, and dataset inspections. Let me walk you through the live demonstration."*

---

## 2. Presentation Structure

### 5-Minute Fast Track (Ideal for standard college vivas):
| Time | Topic | What to Show / Say |
| :--- | :--- | :--- |
| **0:00 - 1:00** | Problem Statement & Architecture | Introduce Amazon review dataset (15 columns) & show the hybrid architecture diagram. |
| **1:00 - 3:00** | Live Dashboard Demo | Open the live demo link. Show the KPI cards, the 3 interactive charts, and the Underperforming Products alert. |
| **3:00 - 4:15** | Pig Tasks & MapReduce Jobs | Switch to "Pig Tasks" tab and "MapReduce Jobs" tab. Explain `TOP` UDF, `FLATTEN`, and `MovingAverage`. |
| **4:15 - 5:00** | Business Conclusion & Q&A | Highlight how this flags defective products (Cyber Warrior RPG 1.25★) and summarizes top-rated items. |

### 10-Minute Deep Dive (Ideal for final project defense / job interviews):
- **Min 0–2:** Introduction, Big Data 5 V's, and why Java MapReduce + Apache Pig were chosen over traditional RDBMS.
- **Min 2–4:** Data Schema walkthrough (15 fields) and data cleansing pipeline.
- **Min 4–7:** Live Demonstration of all 4 tabs on the web dashboard.
- **Min 7–9:** Code walkthrough of `amazon_pig_analysis.pig`, `MovingAverage.java`, and `TopReviewer.java`.
- **Min 9–10:** Scalability discussion (how the pipeline scales to 1 Terabyte) and conclusion.

---

## 3. Step-by-Step Live Demo Walkthrough Script

Open your live web demo: **[https://divakarnaik0079.github.io/product-review-pig/](https://divakarnaik0079.github.io/product-review-pig/)**

### Screen 1: Dashboard Overview Tab
- **Action:** Point to the 4 Top KPI Cards.
- **What to Say:**  
  *"As you can see on the overview dashboard, the pipeline processed 25 verified customer review records across 7 distinct product franchises. The top-rated franchise is Minecraft with a solid 5.0 out of 5 stars. Crucially, the system flagged 2 underperforming products requiring quality recalls, and discovered customer ID 21925455 as the most active reviewer."*
- **Action:** Scroll to the Charts.
- **What to Say:**  
  *"Here we have three interactive visual charts:  
  1. **Review Volume Chart (Pig Task 1):** Shows which digital video game franchises have the highest customer interaction.  
  2. **Average Rating Benchmark (Pig Task 3 & 6):** Green bars represent high-satisfaction titles (Minecraft, Xbox Live Card, Civ VI), while red bars visually highlight the problematic games.  
  3. **Moving Rating Average (MapReduce Job 2):** Notice the cyan line for PSN Card remaining stable around 4.75, while the red line for Cyber Warrior RPG rapidly plummets from initial ratings down to 1.25 as crash complaints accumulate."*

### Screen 2: Underperforming Alert Banner
- **Action:** Hover over the red alert box.
- **What to Say:**  
  *"This is our automated quality alert filter. It uses Pig Latin to isolate franchises where average star rating drops below 2.5. Here, Cyber Warrior RPG scored only 1.25 because reviews reported game crashes and corrupt saves, giving product managers an instant early-warning signal."*

### Screen 3: Apache Pig Tasks Tab
- **Action:** Click on the **Pig Tasks** navigation button.
- **What to Say:**  
  *"In this tab, we see the implementation of all 7 Pig Latin tasks.  
  - For example, in **Task 1**, we group by `product_parent` and use Pig's built-in `TOP` UDF to extract the top 5 franchises.  
  - In **Task 4**, we extract the most helpful reviews based on customer upvotes.  
  - In **Task 7**, we calculated the Helpfulness Ratio: helpful votes divided by total votes, filtering for reviews with at least 85% credibility and at least 20 total votes."*

### Screen 4: MapReduce Jobs Tab
- **Action:** Click on the **MapReduce Jobs** navigation button.
- **What to Say:**  
  *"Here are the three custom Java MapReduce jobs:  
  1. **GameStats.java:** Uses a custom reducer to compute max rating and total engagement.  
  2. **MovingAverage.java:** Tracks sequential rating progression over time.  
  3. **TopReviewer.java:** Implements an advanced MapReduce design pattern using an In-Reducer `cleanup()` method to find the top reviewer without emitting unnecessary intermediate pairs."*

### Screen 5: Dataset Explorer Tab
- **Action:** Click on **Dataset Explorer** and type `crashes` or `Minecraft` into the live search box.
- **What to Say:**  
  *"The dataset explorer allows instant inspection of the raw 15-column Amazon TSV dataset, including review headlines, bodies, verified purchase indicators, and star ratings."*

---

## 4. Explaining the Code: Pig Latin & MapReduce

### Explaining Pig Script (`scripts/amazon_pig_analysis.pig`)

#### 1. Ingestion & Schema-on-Read:
```pig
raw_data = LOAD 'data/amazon_reviews_us_Digital_Video_Games_v1_00.tsv' USING PigStorage('\t') AS (
    marketplace: chararray, customer_id: chararray, review_id: chararray, ...
);
```
**Talking Point:** *"Pig handles semi-structured data via schema-on-read. It uses `PigStorage('\t')` to parse tab-separated columns."*

#### 2. Data Cleaning:
```pig
clean_data = FILTER raw_data BY (star_rating IS NOT NULL) AND (marketplace != 'marketplace');
```
**Talking Point:** *"We filter out the CSV/TSV header row and null values so they don't break numeric aggregations."*

#### 3. Top-N Ranking with `TOP()` & `FLATTEN()`:
```pig
top_5_parent = FOREACH (GROUP parent_counts ALL) GENERATE FLATTEN(TOP(5, 2, parent_counts));
```
**Talking Point:** *"Instead of a slow global ORDER BY, we use Pig's built-in `TOP()` UDF inside a `GROUP ALL`, and `FLATTEN` the resulting bag into flat tuples."*

#### 4. Helpfulness Ratio Calculation:
```pig
(total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpfulness_ratio
```
**Talking Point:** *"We cast integers to floats and use a ternary conditional guard to avoid division-by-zero crashes."*

---

### Explaining MapReduce Java Source Code

#### 1. `MovingAverage.java`
- **Mapper:** Emits `(Product_ID, Star_Rating)`.
- **Reducer:** Iterates over ratings sequentially:
  $$\text{MovingAvg}_n = \frac{1}{n} \sum_{i=1}^n \text{rating}_i$$
- **Talking Point:** *"This allows us to see if a product started well and degraded over time, or improved after patches."*

#### 2. `TopReviewer.java`
- **Pattern:** In-Reducer State Tracking.
- **Talking Point:** *"Instead of emitting every customer count and running a secondary sort, the Reducer keeps state variables `bestReviewer` and `maxReviews`, and emits only the top customer once inside the `cleanup()` lifecycle method."*

---

## 5. Key Insights & Business Value
1. **Defect Detection**: Identifies faulty game releases (`Cyber Warrior RPG` at 1.25★) within hours of release.
2. **Social Proof Curation**: Surfaces trustworthy 5-star reviews with $>95\%$ helpfulness ratios for marketing and product landing pages.
3. **Franchise Aggregation**: Grouping by `product_parent` rather than `product_id` prevents fragmented stats between Standard, Deluxe, and Collector editions.

---

## 6. Defense Strategies for Tough Evaluators

| If Evaluator Asks... | Best Answer Strategy |
| :--- | :--- |
| *"Why didn't you just use MySQL or PostgreSQL?"* | *"RDBMS works well for gigabytes, but struggles with horizontal scaling across commodity servers when data reaches terabytes. Hadoop HDFS distributes storage across nodes, and Pig/MapReduce parallelizes computation across all CPU cores."* |
| *"Why Pig instead of Hive?"* | *"Hive is declarative (SQL) and best for ad-hoc business reporting. Pig is a procedural dataflow language that allows step-by-step pipeline transformations, ETL preprocessing, and complex data structures like bags and tuples."* |
| *"Why Pig instead of Apache Spark?"* | *"Spark is faster due to in-memory processing, but requires high RAM infrastructure. Pig on MapReduce or Tez is extremely robust for heavy batch processing with constrained memory where disk spilling is acceptable."* |
| *"Did you run this on real Hadoop?"* | *"Yes, the repository includes full Hadoop bash scripts (`mapreduce/execution.bash` and `scripts/amazon_pig_analysis.pig`) ready for HDFS and YARN. I also built standalone Python runners so the pipeline can be demonstrated portably on any laptop without needing a cluster."* |
