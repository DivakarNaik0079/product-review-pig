/*
 * =============================================================================
 * SCRIPT: 02_product_summary.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Aggregates reviews at product level to calculate total reviews,
 *   average rating, and total helpful engagement votes per product.
 * =============================================================================
 */

-- 1. Ingestion
raw_reviews = LOAD 'data/sample_reviews.tsv' USING PigStorage('\t') AS (
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

-- 2. Data Cleaning
clean_reviews = FILTER raw_reviews BY 
    review_id != 'review_id' AND 
    rating >= 1 AND rating <= 5;

-- 3. Group by Product Composite Key
grouped_by_product = GROUP clean_reviews BY (product_id, product_name, category);

-- 4. Calculate Aggregate Metrics
product_metrics = FOREACH grouped_by_product GENERATE
    group.product_id AS product_id,
    group.product_name AS product_name,
    group.category AS category,
    COUNT(clean_reviews) AS total_reviews,
    ROUND(AVG(clean_reviews.rating) * 10.0) / 10.0 AS avg_rating,
    SUM(clean_reviews.helpful_votes) AS total_helpful_votes;

-- 5. Sort by Average Rating (Highest to Lowest)
sorted_products = ORDER product_metrics BY avg_rating DESC, total_reviews DESC;

-- 6. Store Output
rmf output/product_summary;
STORE sorted_products INTO 'output/product_summary' USING PigStorage('\t');

-- For interactive inspection:
-- DUMP sorted_products;
