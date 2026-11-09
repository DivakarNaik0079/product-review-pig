/*
 * =============================================================================
 * SCRIPT: 01_data_cleaning.pig
 * PROJECT: Product Review Data Processing Using Pig
 * DESCRIPTION:
 *   Loads raw review data, strips header row, filters out malformed ratings,
 *   and isolates verified customer purchases for trustworthy analytics.
 * USAGE:
 *   Local Mode:     pig -x local scripts/01_data_cleaning.pig
 *   MapReduce Mode: pig scripts/01_data_cleaning.pig
 * =============================================================================
 */

-- Step 1: Load raw TSV dataset with explicit schema
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

-- Step 2: Filter out header row (if present) and remove invalid ratings
clean_reviews = FILTER raw_reviews BY 
    review_id != 'review_id' AND 
    rating >= 1 AND rating <= 5;

-- Step 3: Segment into Verified Customer Purchases
verified_reviews = FILTER clean_reviews BY verified == 'Y';

-- Step 4: Project selected columns for clean intermediate view
projected_clean = FOREACH verified_reviews GENERATE
    review_id,
    product_id,
    product_name,
    category,
    rating,
    review_date;

-- Step 5: Output sample to console / storage
-- In interactive Grunt shell: DUMP projected_clean;
rmf output/cleaned_reviews;
STORE projected_clean INTO 'output/cleaned_reviews' USING PigStorage('\t');

DESCRIBE projected_clean;
