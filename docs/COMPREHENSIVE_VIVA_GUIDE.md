# 🎓 Ultimate Big Data Viva & Interview Guide
## Apache Pig, Hadoop MapReduce & Product Review Analytics
**Student / Candidate:** Divakar Naik  
**Project:** Product Review Data Processing Using Apache Pig & Hadoop MapReduce  
**Repository:** [https://github.com/DivakarNaik0079/product-review-pig](https://github.com/DivakarNaik0079/product-review-pig)

---

## 📑 Table of Contents
- [Category A: Big Data & Hadoop Fundamentals (Q1 - Q7)](#category-a-big-data--hadoop-fundamentals)
- [Category B: Apache Pig & Pig Latin Core (Q8 - Q16)](#category-b-apache-pig--pig-latin-core)
- [Category C: Project-Specific Pig Transformations (Q17 - Q24)](#category-c-project-specific-pig-transformations)
- [Category D: Java MapReduce Design Patterns (Q25 - Q31)](#category-d-java-mapreduce-design-patterns)
- [Category E: Advanced, Architecture & Scalability Questions (Q32 - Q38)](#category-e-advanced-architecture--scalability-questions)

---

## Category A: Big Data & Hadoop Fundamentals

### Q1: What are the 5 V's of Big Data, and how does customer review data fit into them?
**Answer:**  
The 5 V's are:
1. **Volume**: E-commerce platforms like Amazon receive millions of reviews daily (terabytes to petabytes).
2. **Velocity**: Reviews arrive continuously in real-time as users make purchases.
3. **Variety**: Data contains structured ratings (1-5), categorical fields, dates, and unstructured free-text review bodies.
4. **Veracity**: Reviews contain noise, bots, sarcasm, and fake reviews, requiring validation (e.g. `verified_purchase == 'Y'`).
5. **Value**: Extracting sentiment, quality issues, and customer trends generates actionable business insights.

### Q2: What are the core components of Apache Hadoop 3.x?
**Answer:**  
1. **HDFS (Hadoop Distributed File System)**: Distributed, fault-tolerant file storage that splits files into blocks (default 128 MB) replicated across DataNodes (default replication factor = 3).
2. **YARN (Yet Another Resource Negotiator)**: Cluster resource management and job scheduling framework (ResourceManager & NodeManagers).
3. **MapReduce Engine**: Distributed data processing model (Map, Shuffle-and-Sort, Reduce).
4. **Hadoop Common**: Core utilities, RPC, and OS-level libraries.

### Q3: What is the role of NameNode and DataNode in HDFS?
**Answer:**  
- **NameNode (Master)**: Manages file system namespace, directory tree, and tracks which DataNodes hold the blocks for each file. Stores metadata in RAM and persists to `fsimage` and `edits` log.
- **DataNode (Slave)**: Stores actual block replicas on local disks, serves read/write requests, and sends periodic heartbeats and block reports to the NameNode.

### Q4: Explain the MapReduce execution flow step-by-step.
**Answer:**  
1. **InputFormat & RecordReader**: Splits input file into InputSplits; converts splits into raw `(Key, Value)` records.
2. **Map Phase**: User-defined `map()` processes each input pair and emits intermediate `(K2, V2)`.
3. **Partitioner**: Determines which Reducer receives which intermediate key using hash partitioning:  
   $$\text{partition} = (\text{key.hashCode()} \ \& \ \text{Integer.MAX\_VALUE}) \pmod{\text{numReduceTasks}}$$
4. **Shuffle & Sort**: Hadoop transfers intermediate data across the network to reducers and sorts records by key.
5. **Reduce Phase**: User-defined `reduce()` aggregates values for each key and writes to HDFS via `OutputFormat`.

### Q5: What is the difference between a Block and an InputSplit in Hadoop?
**Answer:**  
- **Block**: Physical representation of data stored on disk in HDFS (typically 128MB).
- **InputSplit**: Logical representation of data assigned to a single Mapper. It does not contain data, but rather file path, byte offset, and length. Usually, 1 InputSplit = 1 HDFS Block.

### Q6: Why do we write `hadoop classpath` when compiling Java MapReduce jobs?
**Answer:**  
Because MapReduce programs depend on Hadoop core libraries (such as `org.apache.hadoop.conf.Configuration`, `org.apache.hadoop.io.*`, and `org.apache.hadoop.mapreduce.*`). Running `export HADOOP_CLASSPATH=$(hadoop classpath)` adds all the required JAR dependencies to the Java compiler path.

### Q7: What is the role of Speculative Execution in Hadoop?
**Answer:**  
If a task runs significantly slower than expected due to a degraded worker node (straggler), the YARN ApplicationMaster launches a duplicate backup task on another healthy node. Whichever task finishes first commits its output, and the other is killed.

---

## Category B: Apache Pig & Pig Latin Core

### Q8: What is Apache Pig, and who created it?
**Answer:**  
Apache Pig is a high-level dataflow platform and execution framework for processing large datasets on Hadoop. It was originally developed at Yahoo! in 2006 to simplify programming MapReduce jobs, and later donated to the Apache Software Foundation.

### Q9: What are the primary data models in Apache Pig?
**Answer:**  
- **Atom / Scalar**: A single atomic value (`int`, `long`, `float`, `double`, `chararray`, `bytearray`, `boolean`, `datetime`).
- **Tuple**: An ordered sequence of fields, denoted by parentheses: `('B00F4CEHNK', 'Xbox Live Gift Card', 5.0)`.
- **Bag**: An unordered collection of tuples, denoted by curly braces: `{('B001', 5), ('B002', 4)}`. Bags can contain duplicate tuples.
- **Map**: Key-value pairs where key must be a `chararray` and value can be any data type, denoted by brackets: `['title'#'Minecraft', 'rating'#5]`.

### Q10: What is "Lazy Evaluation" in Pig Latin?
**Answer:**  
In Pig Latin, relational transformation statements (`LOAD`, `FILTER`, `FOREACH`, `GROUP`) are not executed immediately when declared. Instead, Pig parses them and constructs a Directed Acyclic Graph (DAG) representing the logical plan. Computation is only executed when an action statement like `DUMP` or `STORE` is encountered.  
**Benefit**: Allows the Pig Optimizer to perform optimizations like filter pushdown, early column projection, and combining map/reduce stages.

### Q11: Explain the execution pipeline inside Apache Pig.
**Answer:**  
1. **Pig Latin Script** $\rightarrow$
2. **Parser**: Validates syntax, schemas, and creates an abstract syntax tree.
3. **Logical Optimizer**: Generates logical plan; optimizes with filter pushdown and projection pruning.
4. **Physical Compiler**: Translates logical operators into physical operators.
5. **Execution Engine (MapReduce/Tez)**: Converts physical plan into a series of Hadoop MapReduce or Tez jobs that execute on HDFS.

### Q12: What is the difference between `DUMP` and `STORE`?
**Answer:**  
- `DUMP`: Emits results directly to stdout / terminal screen. Used for debugging small datasets.
- `STORE`: Writes the output permanently to an HDFS directory or local filesystem using a storage function (e.g. `PigStorage('\t')`).

### Q13: What does `DESCRIBE`, `EXPLAIN`, and `ILLUSTRATE` do in Pig?
**Answer:**  
- `DESCRIBE <relation>`: Displays the schema (field names and types) of a relation.
- `EXPLAIN <relation>`: Prints the logical, physical, and MapReduce execution plans.
- `ILLUSTRATE <relation>`: Generates a step-by-step sample data execution walkthrough to verify transformation correctness.

### Q14: What is the difference between Pig and Hive?
**Answer:**  
| Feature | Apache Pig | Apache Hive |
| :--- | :--- | :--- |
| **Language** | Pig Latin (Procedural dataflow) | HiveQL (Declarative SQL-like) |
| **Philosophy** | "How to do" (Data pipeline steps) | "What to get" (Query result) |
| **Schema** | Schema-on-read (schema is optional) | Schema-on-write / Metastore required |
| **Target Audience** | Data engineers, programmers | Data analysts, SQL developers, BI teams |
| **Best Used For** | ETL pipelines, text mining, complex data structures | Data warehousing, structured reporting |

### Q15: In what execution modes can Pig run?
**Answer:**  
1. **Local Mode (`pig -x local`)**: Runs inside a single JVM on the local filesystem. Useful for testing without a cluster.
2. **MapReduce Mode (`pig` or `pig -x mapreduce`)**: Runs on a live Hadoop cluster using HDFS and YARN.
3. **Tez Mode (`pig -x tez`)**: Runs DAGs on Apache Tez for faster execution than standard MapReduce.

### Q16: What is a UDF in Apache Pig?
**Answer:**  
A User Defined Function (UDF) allows developers to write custom processing logic in Java, Python, JavaScript, or Ruby when Pig's built-in operators are insufficient. Pig supports:
- **Eval Functions**: Take one or more values and return a value (e.g. `TOP`).
- **Filter Functions**: Return a boolean condition (`true`/`false`).
- **Load/Store Functions**: Read/write specialized file formats (e.g. JSON, Parquet).

---

## Category C: Project-Specific Pig Transformations

### Q17: What dataset was used in this project?
**Answer:**  
The **Amazon US Customer Reviews Dataset** (Digital Video Games category) from Kaggle/AWS, adhering to the standard 15-column tab-separated format.

### Q18: What is the difference between `product_id` and `product_parent`? Why did you group by `product_parent`?
**Answer:**  
- `product_id` (ASIN) refers to a specific variation or SKU (e.g., download code, collector's box).
- `product_parent` is the master franchise ID that groups all editions of the same product.  
Grouping by `product_parent` (Tasks 1, 3, 5) aggregates all customer feedback for the overarching game/franchise rather than splitting review counts across different editions.

### Q19: Explain the Pig `TOP()` function used in Task 1, 2, 3, 4, and 5.
**Answer:**  
`TOP(topN, sortColIndex, bag)` is a built-in evaluation function in Apache Pig.
- `topN`: Number of records to return (e.g., 5 or 10).
- `sortColIndex`: 0-based index of the column to sort descending.
- `bag`: The bag of tuples to evaluate.  
When combined with `GROUP relation ALL` and `FLATTEN(TOP(...))`, it efficiently pulls top records without incurring the overhead of a full global sort.

### Q20: What is `FLATTEN` and why is it used after `TOP()`?
**Answer:**  
`TOP()` outputs a bag containing the top tuples. In Pig Latin, bags are nested collections. Using `FLATTEN(TOP(...))` un-nests the bag, converting each internal tuple into a standard flat row so it can be written to disk with `STORE`.

### Q21: How did you clean the raw data in Pig?
**Answer:**  
```pig
clean_data = FILTER raw_data BY 
    (star_rating IS NOT NULL) AND 
    (total_votes IS NOT NULL) AND 
    (marketplace != 'marketplace');
```
1. Strips out the column header row (`marketplace != 'marketplace'`).
2. Eliminates corrupted records with null ratings or vote counts.

### Q22: How was the Review Helpfulness Ratio calculated? How did you prevent division by zero?
**Answer:**  
```pig
(total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpfulness_ratio
```
We used:
1. Float casting `(float)helpful_votes` to avoid integer division truncation (which would yield 0).
2. Ternary condition `total_votes > 0 ? ... : 0.0f` to guard against `ArithmeticException: / by zero`.

### Q23: How did you detect underperforming products (Task 6)?
**Answer:**  
We grouped reviews by product franchise, computed the average star rating, and applied a filter:
```pig
underperforming = FILTER avg_rate BY avg_rating <= 2.5;
```
This flagged `Cyber Warrior RPG` (1.25★) and `FIFA 24 Ultimate Edition` (2.50★).

### Q24: What is `rmf` in Pig Latin? Why is it at the beginning of the script?
**Answer:**  
`rmf` stands for "Remove File/Folder". In Hadoop, `STORE` throws an error if the target output directory already exists (to prevent accidental data loss). `rmf <dir>` automatically clears old outputs before running the job.

---

## Category D: Java MapReduce Design Patterns

### Q25: Why did you write custom Java MapReduce jobs when you already had Apache Pig?
**Answer:**  
While Apache Pig is optimal for relational transformations (filtering, grouping, top-N), certain algorithmic patterns are more efficiently expressed in Java MapReduce:
1. **`MovingAverage.java`**: Pig cannot easily maintain state across row-by-row streaming to compute running moving averages without complex windowing or UDFs.
2. **`TopReviewer.java`**: Demonstrates the **In-Reducer Cleanup Pattern**, where intermediate key emissions are bypassed to minimize network I/O.
3. Demonstrates proficiency in both high-level dataflow abstractions (Pig) and low-level distributed systems programming (Java MapReduce).

### Q26: Explain the In-Reducer Cleanup Pattern in `TopReviewer.java`.
**Answer:**  
In standard MapReduce, every call to `reduce()` emits a key-value pair to `context.write()`. In `TopReviewer.java`:
- The Reducer maintains private state variables: `private String bestReviewer` and `private int maxReviews`.
- In `reduce()`, it updates `bestReviewer` whenever a customer's review count exceeds `maxReviews`.
- The single emission happens in `protected void cleanup(Context context)`, which is called only once after all keys have been processed.  
**Benefit**: Reduces reducer output records from $N$ to exactly $1$, saving disk writes and network transfer.

### Q27: How does `MovingAverage.java` work?
**Answer:**  
- **Mapper**: Emits `Text(product_id)` as Key and `FloatWritable(star_rating)` as Value.
- **Reducer**: Hadoop aggregates all ratings for a product ID into `Iterable<FloatWritable> values`. The reducer maintains `sum += rating` and `count++`, appending $\frac{\text{sum}}{\text{count}}$ to a running progression string for that product.

### Q28: How does `GameStats.java` aggregate metrics?
**Answer:**  
- **Mapper**: Emits `product_id` as Key, and a compound Text value: `star_rating + "," + helpful_votes + "," + total_votes`.
- **Reducer**: Splits the compound string, computes `Math.max()` for star rating, and sums `helpful_votes` and `total_votes`.

### Q29: What Hadoop Writable types did you use, and why?
**Answer:**  
- `Text`: For string identifiers (`product_id`, customer ID).
- `IntWritable`: For counts and integer votes.
- `FloatWritable`: For fractional star ratings.  
Hadoop requires types to implement `Writable` (for serialization) and `WritableComparable` (for keys during shuffle-sort).

### Q30: What is the role of `job.setJarByClass(GameStats.class)`?
**Answer:**  
It tells the Hadoop JobTracker/ResourceManager which JAR file on the cluster contains the mapper and reducer classes so it can distribute the code to worker nodes.

### Q31: What is a Combiner? Could you use a Combiner in `GameStats`?
**Answer:**  
A Combiner (semi-reducer) runs locally on the Mapper node before the shuffle phase.  
Yes! Because `Math.max()` and addition (`SUM`) are associative and commutative:
$$\max(a, \max(b, c)) = \max(\max(a, b), c)$$
A Combiner could pre-aggregate max ratings and vote sums locally, reducing shuffle network traffic.

---

## Category E: Advanced, Architecture & Scalability Questions

### Q32: What happens if the dataset size grows from megabytes to 1 Terabyte?
**Answer:**  
The beauty of Hadoop and Pig is horizontal scalability:
1. **HDFS**: The 1TB file is automatically split into $\sim 8,000$ blocks (at 128MB per block) distributed across DataNodes.
2. **Pig/MapReduce**: Automatically spins up $\sim 8,000$ mappers in parallel across the cluster.
3. No code changes are required; only cluster capacity (number of worker nodes) needs to be scaled.

### Q33: What is Data Skewness in Pig / MapReduce, and how would you fix it?
**Answer:**  
Data skew occurs when one key has significantly more records than others (e.g. a viral product with 10 million reviews). That single reducer will become a bottleneck (straggler).  
**Solutions**:
- In Pig: Use `PARALLEL <num>` to increase reducers, or use `skewed join` / `replicated join`.
- In MapReduce: Use **salting** (appending a random prefix `1..k` to the key) to distribute records across multiple reducers, then run a secondary aggregation.

### Q34: What is the difference between a Map-side Join and a Reduce-side Join in Pig?
**Answer:**  
- **Reduce-side Join (Default)**: Sends all records from both relations through shuffle-and-sort to reducers. Handles any dataset size but incurs heavy network I/O.
- **Map-side Join (`USING 'replicated'`)**: Used when one dataset is small enough to fit in memory (e.g. metadata table). Pig copies the small relation to each Mapper's memory via DistributedCache, performing joins instantly with zero shuffle overhead.

### Q35: How does your Python runner (`tools/pig_runner.py`) work?
**Answer:**  
It inspects whether the native `pig` binary is present on the OS:
- If installed: It delegates directly to `pig -x local`.
- If not installed (e.g. Windows without Hadoop): It parses the TSV files, executes the identical relational transformations in Python (grouping, aggregations, top-N, filtering), and writes standard Hadoop `part-r-00000` output files, making the project 100% portable.

### Q36: How does the live interactive dashboard work without a backend server?
**Answer:**  
The dashboard ([`index.html`](file:///C:/Users/jagad/.gemini/antigravity/scratch/product-review-pig/index.html)) is a self-contained Single-Page Application (SPA):
- Uses Tailwind CSS and FontAwesome via CDN for styling.
- Uses Chart.js via CDN for interactive visualizations.
- Embeds verified execution dataset records and outputs in client-side JavaScript.
- Deployed via **GitHub Pages**, providing a zero-cost, serverless, high-availability live demo URL.

### Q37: What are the security mechanisms in a production Hadoop cluster?
**Answer:**  
- **Kerberos**: For authentication (preventing unauthorized nodes from joining cluster).
- **HDFS Permissions & ACLs**: POSIX-style file permissions (read/write/execute).
- **Apache Ranger**: Centralized security administration and fine-grained access control.
- **Apache Knox**: Perimeter gateway protecting cluster endpoints.

### Q38: If you had 6 more months, what would you add to this project?
**Answer:**  
1. **Natural Language Processing (NLP) UDF**: Integrate a Python/Java UDF using VADER or HuggingFace transformers to perform real-time sentiment polarity scoring on `review_body`.
2. **Streaming Pipeline**: Use Apache Kafka and Apache Spark Streaming / Apache Flink to process live review streams instead of static batch files.
3. **Automated ML Classification**: Train a Spark MLlib classifier to predict verified review authenticity and automatically flag fake/bot reviews.
