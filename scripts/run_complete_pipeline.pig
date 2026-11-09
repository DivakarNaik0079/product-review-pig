/*
 * =============================================================================
 * SCRIPT: run_complete_pipeline.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Master pipeline script that performs all processing stages in one run:
 *   1. Data Ingestion & Sanitization
 *   2. Product-Level Metrics Aggregation
 *   3. Top and Bottom Product Leaderboards
 *   4. Category Level Distribution Analysis
 *   5. Review Helpfulness & Impact Scoring
 *
 * EXECUTION:
 *   Local Mode:     pig -x local scripts/run_complete_pipeline.pig
 *   Hadoop Cluster: pig scripts/run_complete_pipeline.pig
 * =============================================================================
 */

-- [STAGE 1] Ingestion
raw_data = LOAD 'data/sample_reviews.tsv' USING PigStorage('\t') AS (
    review_id:      chararray,
    product_id:     chararray,
    product_name:   chararray,
    category:       chararray,
    rating:         int,
    helpful_votes:  int,
    total_votes:    int,
    review_date:    chararray,
    verified:       chararray,
    review_text:    chararray
);

-- Filter headers and invalid ratings
clean_reviews = FILTER raw_data BY 
    review_id != 'review_id' AND 
    rating >= 1 AND rating <= 5;

-- [STAGE 2] Product Summary
grouped_products = GROUP clean_reviews BY (product_id, product_name, category);

product_summary = FOREACH grouped_products GENERATE
    group.product_id AS product_id,
    group.product_name AS product_name,
    group.category AS category,
    COUNT(clean_reviews) AS review_count,
    ROUND(AVG(clean_reviews.rating) * 10.0) / 10.0 AS avg_rating,
    SUM(clean_reviews.helpful_votes) AS total_helpful_votes;

-- [STAGE 3] Top & Bottom Ranked Products
qualified_products = FILTER product_summary BY review_count >= 3;

sorted_products_desc = ORDER qualified_products BY avg_rating DESC, review_count DESC;
top_5_products = LIMIT sorted_products_desc 5;

low_rated_products = FILTER qualified_products BY avg_rating <= 2.5;
sorted_products_asc = ORDER low_rated_products BY avg_rating ASC;

-- [STAGE 4] Category Aggregation
grouped_categories = GROUP clean_reviews BY category;

category_summary = FOREACH grouped_categories GENERATE
    group AS category,
    COUNT(clean_reviews) AS total_reviews,
    ROUND(AVG(clean_reviews.rating) * 10.0) / 10.0 AS avg_rating,
    SUM(clean_reviews.helpful_votes) AS total_helpful_votes;

sorted_categories = ORDER category_summary BY avg_rating DESC;

-- [STAGE 5] Helpfulness Metric
reviews_with_helpfulness = FOREACH clean_reviews GENERATE
    review_id,
    product_name,
    rating,
    helpful_votes,
    total_votes,
    (total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpfulness_ratio,
    review_text;

high_impact_reviews = FILTER reviews_with_helpfulness BY total_votes >= 15 AND helpfulness_ratio >= 0.85f;
sorted_high_impact = ORDER high_impact_reviews BY helpful_votes DESC;

-- [STAGE 6] Storing Pipeline Results
rmf output/all_product_summary;
STORE sorted_products_desc INTO 'output/all_product_summary' USING PigStorage('\t');

rmf output/top_5_products;
STORE top_5_products INTO 'output/top_5_products' USING PigStorage('\t');

rmf output/underperforming_products;
STORE sorted_products_asc INTO 'output/underperforming_products' USING PigStorage('\t');

rmf output/category_summary;
STORE sorted_categories INTO 'output/category_summary' USING PigStorage('\t');

rmf output/high_impact_reviews;
STORE sorted_high_impact INTO 'output/high_impact_reviews' USING PigStorage('\t');
