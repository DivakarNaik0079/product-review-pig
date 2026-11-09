/*
 * =============================================================================
 * SCRIPT: 05_helpfulness_analysis.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Calculates review helpfulness ratio (helpful_votes / total_votes)
 *   and extracts most influential positive & critical reviews.
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

-- Compute helpfulness ratio with guard against division by zero
reviews_with_ratio = FOREACH clean_reviews GENERATE
    review_id,
    product_id,
    product_name,
    category,
    rating,
    helpful_votes,
    total_votes,
    (total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpful_ratio,
    review_text;

-- Filter high-impact trustworthy reviews (at least 15 votes & 85% helpfulness score)
high_impact_reviews = FILTER reviews_with_ratio BY 
    total_votes >= 15 AND 
    helpful_ratio >= 0.85f;

-- Sort by highest engagement
sorted_impact = ORDER high_impact_reviews BY helpful_votes DESC;

-- Segment 1: High Impact Critical Reviews (Alerts for engineering / product team)
critical_alerts = FILTER sorted_impact BY rating <= 2;

-- Segment 2: High Impact Praise (Best for marketing & featured quotes)
stellar_praise = FILTER sorted_impact BY rating >= 4;

rmf output/high_impact_critical;
STORE critical_alerts INTO 'output/high_impact_critical' USING PigStorage('\t');

rmf output/high_impact_praise;
STORE stellar_praise INTO 'output/high_impact_praise' USING PigStorage('\t');
