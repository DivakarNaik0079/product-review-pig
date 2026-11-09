# Big-Data-Analysis-of-Amazon-Digital-Video-Game-Reviews-Using-Hadoop-MapReduce-and-Apache-Pig
This repository contains a complete big data analytics pipeline built for processing the Amazon US Customer Reviews dataset. It utilizes Apache Hadoop (HDFS and custom Java MapReduce jobs) and Apache Pig script-based data flows executed within a WSL (Linux) environment to extract product insights, customer trends, and review statistics.
Hadoop, HDFS, MapReduce, Pig, Java, WSL(Linux commands)

Project Overview
University project for Big Data Analytics course.

Implemented Apache Hadoop big data framework to analyze e-commerce data with the help of HDFS, map-reduce design patterns, and Pig.

Implemented Data flow language Apache Pig built on top of Hadoop to execute Pig Scripts for Big Data Analysis.

Problem Statement :-
Analyze the Amazon Customer Reviews Big dataset using Hadoop and Pig based on different column fields to provide various comprehensive insights regarding product quality and customer engagement.

Summary :-
The dataset is available on the following URL: https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset?select=amazon_reviews_us_Digital_Video_Games_v1_00.tsv

We record the following information of an Amazon review in order; they are divided by '\t' in the data file:

marketplace : 2-letter country code.

customer_id : Random identifier that can be used to track reviews written by the same author.

review_id : The unique ID of the review.

product_id : The unique Product ID the review pertains to.

product_parent : Random identifier that can be used to aggregate reviews for the same product franchise.

product_title : Title of the product.

product_category : Broad product category (Digital_Video_Games).

star_rating : The 1-5 star rating of the review.

helpful_votes : Number of helpful votes.

total_votes : Number of total votes the review received.

vine : Review was written as part of the Vine program.  

verified_purchase : The review is on a verified purchase.  

review_headline : The title of the review.  

review_body : The review text.  

review_date : The date the review was written.  

Following MapReduce programs and its different design patterns are implemented:  

GameStats: Calculate Max Rating, Total Helpful Votes, and Total Votes by Product ID.  

MovingAverage: Calculate Moving Rating Average by Product ID.

TopReviewer: Find the Best Reviewer based on the total number of reviews published.

Following Pig analysis is performed on the data-set :

Calculate top 5 Game Franchises (Product Parents) by Review Count.

Calculate top 10 Rated Individual Reviews.

Calculate top 10 Rated Game Franchises.

Calculate top 10 Most Helpful Individual Reviews.

Calculate top 10 Most Interacted Game Franchises (Total Votes).

Execution Instructions

1. Start Hadoop and Load Data

Bash
start-all.sh
jps
hdfs dfs -mkdir /amazon/
hdfs dfs -put amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/

2. Compile and Run MapReduce Jobs

Bash
export HADOOP_CLASSPATH=$(hadoop classpath)
javac -classpath ${HADOOP_CLASSPATH} -d . GameStats.java MovingAverage.java TopReviewer.java
jar -cvf amazon_jobs.jar *.class

hadoop jar amazon_jobs.jar GameStats /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_stats
hadoop jar amazon_jobs.jar MovingAverage /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_avg
hadoop jar amazon_jobs.jar TopReviewer /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_best

3. Run Apache Pig Script

Bash
pig -x mapreduce amazon_pig_analysis.pig
