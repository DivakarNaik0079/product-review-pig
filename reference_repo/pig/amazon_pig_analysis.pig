/* 1. Safely remove old output folders */
rmf /amazon/pig_task1;
rmf /amazon/pig_task2;
rmf /amazon/pig_task3;
rmf /amazon/pig_task4;
rmf /amazon/pig_task5;

/* 2. Load and Filter the raw data */
raw_data = LOAD '/amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray, product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:float, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:chararray);

-- Filter out the header row and any corrupted/null data
clean_data = FILTER raw_data BY (star_rating IS NOT NULL) AND (total_votes IS NOT NULL) AND (marketplace != 'marketplace');

/* -------------------------------------------------------- */
/* TASK 1: Top 5 Game Franchises (Product Parents) by Review Count */
/* -------------------------------------------------------- */
grp_parent = GROUP clean_data BY product_parent;
parent_counts = FOREACH grp_parent GENERATE group AS product_parent, COUNT(clean_data) AS total;
grp_all_1 = GROUP parent_counts ALL;
top_5_parent = FOREACH grp_all_1 GENERATE FLATTEN(TOP(5, 1, parent_counts));
STORE top_5_parent INTO '/amazon/pig_task1' USING PigStorage('\t');
EXEC;

/* -------------------------------------------------------- */
/* TASK 2: Top 10 Rated Individual Reviews */
/* -------------------------------------------------------- */
-- Sort by star_rating (column index 7)
grp_all_2 = GROUP clean_data ALL;
top_10_rated = FOREACH grp_all_2 GENERATE FLATTEN(TOP(10, 7, clean_data));
STORE top_10_rated INTO '/amazon/pig_task2' USING PigStorage('\t');
EXEC;

/* -------------------------------------------------------- */
/* TASK 3: Top 10 Rated Game Franchises */
/* -------------------------------------------------------- */
avg_rate = FOREACH grp_parent GENERATE group AS product_parent, AVG(clean_data.star_rating) AS avg_rating;
grp_all_3 = GROUP avg_rate ALL;
top_10_avg_rate = FOREACH grp_all_3 GENERATE FLATTEN(TOP(10, 1, avg_rate));
STORE top_10_avg_rate INTO '/amazon/pig_task3' USING PigStorage('\t');
EXEC;

/* -------------------------------------------------------- */
/* TASK 4: Top 10 Most Helpful Individual Reviews */
/* -------------------------------------------------------- */
-- Sort by helpful_votes (column index 8)
grp_all_4 = GROUP clean_data ALL;
top_10_helpful = FOREACH grp_all_4 GENERATE FLATTEN(TOP(10, 8, clean_data));
STORE top_10_helpful INTO '/amazon/pig_task4' USING PigStorage('\t');
EXEC;

/* -------------------------------------------------------- */
/* TASK 5: Top 10 Most Interacted Game Franchises (Total Votes) */
/* -------------------------------------------------------- */
sum_votes = FOREACH grp_parent GENERATE group AS product_parent, SUM(clean_data.total_votes) AS total_franchise_votes;
grp_all_5 = GROUP sum_votes ALL;
top_10_sum_votes = FOREACH grp_all_5 GENERATE FLATTEN(TOP(10, 1, sum_votes));
STORE top_10_sum_votes INTO '/amazon/pig_task5' USING PigStorage('\t');
EXEC;
