/*
 * =============================================================================
 * SCRIPT: 03_top_and_bottom_products.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Extracts:
 *   1) Top 5 highest rated products with a minimum review volume threshold
 *   2) Bottom flagged products with low customer satisfaction ratings
 * =============================================================================
 */

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

clean_reviews = FILTER raw_reviews BY 
    review_id != 'review_id' AND 
    rating >= 1 AND rating <= 5;

-- Group by product to aggregate ratings
grouped_product = GROUP clean_reviews BY (product_id, product_name, category);

product_stats = FOREACH grouped_product GENERATE
    group.product_id AS product_id,
    group.product_name AS product_name,
    group.category AS category,
    COUNT(clean_reviews) AS review_count,
    ROUND(AVG(clean_reviews.rating) * 10.0) / 10.0 AS avg_rating;

-- Enforce statistical threshold: At least 3 reviews to qualify
trusted_products = FILTER product_stats BY review_count >= 3;

-- 1. TOP PERFORMERS
ranked_desc = ORDER trusted_products BY avg_rating DESC, review_count DESC;
top_5_products = LIMIT ranked_desc 5;

-- 2. LOWEST RATED / CRITICAL ISSUES (Rating <= 2.5)
low_rated_products = FILTER trusted_products BY avg_rating <= 2.5;
ranked_asc = ORDER low_rated_products BY avg_rating ASC, review_count DESC;

-- Store Outputs
rmf output/top_products;
STORE top_5_products INTO 'output/top_products' USING PigStorage('\t');

rmf output/underperforming_products;
STORE ranked_asc INTO 'output/underperforming_products' USING PigStorage('\t');
