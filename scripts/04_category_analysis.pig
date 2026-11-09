/*
 * =============================================================================
 * SCRIPT: 04_category_analysis.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Performs category-level aggregations: total review volume,
 *   average category rating, and overall customer satisfaction benchmark.
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

-- Group by Category
grouped_category = GROUP clean_reviews BY category;

-- Aggregate metrics per category
category_insights = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(clean_reviews) AS total_reviews,
    ROUND(AVG(clean_reviews.rating) * 10.0) / 10.0 AS avg_rating,
    SUM(clean_reviews.helpful_votes) AS total_helpful_votes;

-- Sort categories by average rating descending
sorted_categories = ORDER category_insights BY avg_rating DESC, total_reviews DESC;

rmf output/category_analysis;
STORE sorted_categories INTO 'output/category_analysis' USING PigStorage('\t');

-- DUMP sorted_categories;
