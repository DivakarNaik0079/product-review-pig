export HADOOP_CLASSPATH=$(hadoop classpath) 
javac -classpath ${HADOOP_CLASSPATH} -d . GameStats.java MovingAverage.java TopReviewer.java 
-cvf amazon_jobs.jar *.class
hadoop jar amazon_jobs.jar GameStats /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_stats 
hadoop jar amazon_jobs.jar MovingAverage /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_avg
hadoop jar amazon_jobs.jar TopReviewer /amazon/amazon_reviews_us_Digital_Video_Games_v1_00.tsv /amazon/output_best
