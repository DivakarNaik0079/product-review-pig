/*
 * =============================================================================
 * SCRIPT: amazon_pig_analysis.pig
 * PROJECT: Product Review Data Processing Using Pig (Amazon Customer Reviews)
 * DESCRIPTION:
 *   Comprehensive big data analysis pipeline for Amazon Customer Reviews.
 *   Executes 7 distinct analytical tasks:
 *     Task 1: Top 5 Product Franchises (Product Parents) by Review Count
 *     Task 2: Top 10 Rated Individual Reviews
 *     Task 3: Top 10 Highest Rated Game Franchises (Average Rating)
 *     Task 4: Top 10 Most Helpful Individual Reviews
 *     Task 5: Top 10 Most Interacted Game Franchises (Total Votes)
 *     Task 6: Flagged Underperforming / Low Rated Products (Quality Control)
 *     Task 7: Verified Purchase & Helpfulness Ratio Metric
 *
 * EXECUTION:
 *   Local Mode:     pig -x local scripts/amazon_pig_analysis.pig
 *   MapReduce/HDFS: pig -x mapreduce scripts/amazon_pig_analysis.pig
 * =============================================================================
 */

-- Step 1: Safely remove previous output directories
rmf output/pig_task1_top_franchises;
rmf output/pig_task2_top_rated_reviews;
rmf output/pig_task3_top_rated_franchises;
rmf output/pig_task4_most_helpful_reviews;
rmf output/pig_task5_most_interacted_franchises;
rmf output/pig_task6_underperforming_franchises;
rmf output/pig_task7_helpfulness_ratio;

-- Step 2: Ingest raw Amazon Reviews TSV dataset with official 15-column schema
raw_data = LOAD 'data/amazon_reviews_us_Digital_Video_Games_v1_00.tsv' USING PigStorage('\t') AS (
    marketplace:        chararray,
    customer_id:        chararray,
    review_id:          chararray,
    product_id:         chararray,
    product_parent:     chararray,
    product_title:      chararray,
    product_category:   chararray,
    star_rating:        float,
    helpful_votes:      int,
    total_votes:        int,
    vine:               chararray,
    verified_purchase:  chararray,
    review_headline:    chararray,
    review_body:        chararray,
    review_date:        chararray
);

-- Step 3: Data Cleaning & Hygiene
-- Filter out the TSV header row and records with null ratings or non-standard values
clean_data = FILTER raw_data BY 
    (star_rating IS NOT NULL) AND 
    (total_votes IS NOT NULL) AND 
    (marketplace != 'marketplace');

/* -----------------------------------------------------------------------------
 * TASK 1: Top 5 Game Franchises (Product Parents) by Review Count
 * ----------------------------------------------------------------------------- */
grp_parent = GROUP clean_data BY (product_parent, product_title);
parent_counts = FOREACH grp_parent GENERATE 
    group.product_parent AS product_parent, 
    group.product_title AS product_title, 
    COUNT(clean_data) AS total_reviews;

grp_all_1 = GROUP parent_counts ALL;
top_5_parent = FOREACH grp_all_1 GENERATE FLATTEN(TOP(5, 2, parent_counts));
STORE top_5_parent INTO 'output/pig_task1_top_franchises' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 2: Top 10 Rated Individual Reviews
 * ----------------------------------------------------------------------------- */
grp_all_2 = GROUP clean_data ALL;
top_10_rated = FOREACH grp_all_2 GENERATE FLATTEN(TOP(10, 7, clean_data));
STORE top_10_rated INTO 'output/pig_task2_top_rated_reviews' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 3: Top 10 Rated Game Franchises (Highest Average Rating)
 * ----------------------------------------------------------------------------- */
avg_rate = FOREACH grp_parent GENERATE 
    group.product_parent AS product_parent,
    group.product_title AS product_title,
    ROUND(AVG(clean_data.star_rating) * 100.0) / 100.0 AS avg_rating,
    COUNT(clean_data) AS review_count;

-- Filter for statistical relevance (at least 2 reviews)
qualified_franchises = FILTER avg_rate BY review_count >= 2;
grp_all_3 = GROUP qualified_franchises ALL;
top_10_avg_rate = FOREACH grp_all_3 GENERATE FLATTEN(TOP(10, 2, qualified_franchises));
STORE top_10_avg_rate INTO 'output/pig_task3_top_rated_franchises' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 4: Top 10 Most Helpful Individual Reviews
 * ----------------------------------------------------------------------------- */
grp_all_4 = GROUP clean_data ALL;
top_10_helpful = FOREACH grp_all_4 GENERATE FLATTEN(TOP(10, 8, clean_data));
STORE top_10_helpful INTO 'output/pig_task4_most_helpful_reviews' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 5: Top 10 Most Interacted Game Franchises (Total Votes Sum)
 * ----------------------------------------------------------------------------- */
sum_votes = FOREACH grp_parent GENERATE 
    group.product_parent AS product_parent,
    group.product_title AS product_title,
    SUM(clean_data.total_votes) AS total_franchise_votes;

grp_all_5 = GROUP sum_votes ALL;
top_10_sum_votes = FOREACH grp_all_5 GENERATE FLATTEN(TOP(10, 2, sum_votes));
STORE top_10_sum_votes INTO 'output/pig_task5_most_interacted_franchises' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 6: Underperforming / Critical Alert Products (avg_rating <= 2.5)
 * ----------------------------------------------------------------------------- */
underperforming = FILTER avg_rate BY avg_rating <= 2.5;
sorted_underperforming = ORDER underperforming BY avg_rating ASC;
STORE sorted_underperforming INTO 'output/pig_task6_underperforming_franchises' USING PigStorage('\t');

/* -----------------------------------------------------------------------------
 * TASK 7: Helpfulness Ratio Metric Calculation
 * ----------------------------------------------------------------------------- */
reviews_with_helpfulness = FOREACH clean_data GENERATE
    review_id,
    product_title,
    star_rating,
    helpful_votes,
    total_votes,
    (total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpfulness_ratio,
    review_body;

high_trust_reviews = FILTER reviews_with_helpfulness BY total_votes >= 20 AND helpfulness_ratio >= 0.85f;
STORE high_trust_reviews INTO 'output/pig_task7_helpfulness_ratio' USING PigStorage('\t');
