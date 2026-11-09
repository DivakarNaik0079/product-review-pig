# Product Review Data Processing Using Pig & MapReduce
## Comprehensive Viva & Technical Interview Questions & Answers

---

### Q1: What is the primary architecture of this project?
**Answer:**  
The project implements a hybrid Big Data analytics pipeline on the **Amazon US Customer Reviews Dataset** using:
1. **Apache Pig (Pig Latin)**: For high-level relational dataflow operations including data hygiene, product franchise grouping, top-N ranking (`TOP` UDF), helpfulness ratio calculation, and underperforming product filtering.
2. **Hadoop MapReduce (Java)**: For customized low-level distributed jobs:
   - `GameStats`: Aggregates max rating, helpful votes, and total votes per product.
   - `MovingAverage`: Computes progressive chronological rating average progression.
   - `TopReviewer`: Identifies the top reviewer using Reducer state retention and `cleanup()`.

---

### Q2: How does the built-in Pig function `TOP()` work?
**Answer:**  
`TOP(topN, sortColIndex, bag)` is a built-in Pig evaluation UDF (User Defined Function).
- `topN`: The number of top records to return (e.g. 5 or 10).
- `sortColIndex`: The 0-based index of the field in the tuple by which to sort in descending order.
- `bag`: The bag of tuples to evaluate.  
When combined with `GROUP ALL` and `FLATTEN()`, it efficiently extracts top-N records without loading the entire relation into client memory.

---

### Q3: What is the purpose of `FLATTEN` in Pig Latin?
**Answer:**  
`FLATTEN` in Pig Latin is an operator that un-nests complex data types:
- Applied to a **bag**, it produces a tuple for each tuple in the bag (cross-product or unrolling).
- Applied to a **tuple**, it substitutes the fields of the tuple into the enclosing tuple.  
In our project, `FLATTEN(TOP(...))` extracts individual tuples from the bag returned by the `TOP` function so they can be stored as normal flat rows in HDFS.

---

### Q4: Explain the In-Reducer aggregation pattern used in `TopReviewer.java`.
**Answer:**  
In standard MapReduce, a reducer writes output for every key using `context.write()`. In `TopReviewer.java`:
- The Reducer maintains class-level state variables: `bestReviewer` and `maxReviews`.
- In the `reduce()` method, it updates these variables if the current customer's review count exceeds `maxReviews`.
- The actual emission occurs only once inside the `cleanup(Context context)` method, which executes after all reducer keys have been processed.  
**Advantage**: This avoids emitting thousands of intermediate key-value pairs, drastically reducing network and disk I/O.

---

### Q5: How is Moving Average computed in MapReduce?
**Answer:**  
In `MovingAverage.java`:
- **Map Phase**: The Mapper extracts `product_id` as the Key and `star_rating` as a `FloatWritable` Value.
- **Shuffle & Sort**: Hadoop routes all ratings for the same `product_id` to the same Reducer.
- **Reduce Phase**: The Reducer iterates over `Iterable<FloatWritable> values`, maintaining running variables:
  $$\text{sum} \mathrel{+}= \text{rating}, \quad \text{count}++$$
  For each iteration, it computes $\frac{\text{sum}}{\text{count}}$ and appends it to a comma-separated moving average sequence, illustrating the chronological trend of customer sentiment.

---

### Q6: What are Hadoop Writable types (`Text`, `IntWritable`, `FloatWritable`)? Why not standard Java types?
**Answer:**  
Standard Java serialization (`java.io.Serializable`) is too heavy and includes excessive metadata overhead for distributed network transmission. Hadoop defines the `org.apache.hadoop.io.Writable` interface, which provides a compact, high-efficiency, lightweight binary serialization protocol optimized for cluster RPC and disk persistence.

---

### Q7: Explain the 15-column Amazon Review schema used in this project.
**Answer:**  
The official schema includes:
1. `marketplace`: 2-letter country code.
2. `customer_id`: Author identifier.
3. `review_id`: Unique review ID.
4. `product_id`: ASIN code.
5. `product_parent`: Franchise parent identifier.
6. `product_title`: Product title.
7. `product_category`: Broad category name.
8. `star_rating`: Rating (1.0 to 5.0).
9. `helpful_votes`: Number of helpful votes.
10. `total_votes`: Total votes cast.
11. `vine`: Amazon Vine program indicator.
12. `verified_purchase`: Verified purchase indicator (Y/N).
13. `review_headline`: Review headline.
14. `review_body`: Text content of review.
15. `review_date`: Date review was posted.

---

### Q8: What is the difference between `product_id` and `product_parent` in Amazon reviews?
**Answer:**  
- `product_id` (ASIN) represents a specific edition or packaging of a product (e.g. standard edition, deluxe download, physical disc).
- `product_parent` is a common identifier that groups all editions and variants belonging to the same product franchise.  
Grouping by `product_parent` (as done in Task 1, 3, and 5) aggregates sentiment and volume across the entire franchise rather than fragmenting it.

---

### Q9: How did you calculate the Review Helpfulness Ratio in Pig?
**Answer:**  
Using the ternary conditional operator and float type casting:
```pig
(total_votes > 0 ? ROUND(((float)helpful_votes / (float)total_votes) * 100.0) / 100.0 : 0.0f) AS helpfulness_ratio
```
This safeguards against division-by-zero errors when a review has zero total votes.

---

### Q10: How did you identify defective / underperforming products?
**Answer:**  
In Task 6, we computed the average rating per product franchise and filtered:
```pig
underperforming = FILTER avg_rate BY avg_rating <= 2.5;
```
In our dataset, this flagged `Cyber Warrior RPG` (1.25 / 5.0) and `FIFA 24 Ultimate Edition` (2.50 / 5.0), alerting product teams to severe crash bugs and microtransaction dissatisfaction.

---

### Q11: How do you execute Pig scripts on local mode vs. Hadoop cluster mode?
**Answer:**  
- **Local Mode**: `pig -x local scripts/amazon_pig_analysis.pig`  
  Executes on a single JVM using local file system paths without requiring active Hadoop daemons.
- **MapReduce Mode**: `pig -x mapreduce scripts/amazon_pig_analysis.pig`  
  Translates the Pig Latin script into physical MapReduce jobs executed on HDFS and managed by YARN.

---

### Q12: What is the purpose of `rmf` in Pig Latin scripts?
**Answer:**  
In Hadoop/Pig, attempting to store data into an already existing HDFS directory results in an `Output directory already exists` exception (to prevent accidental data overwrite). `rmf <path>` (Remove File/Folder) forcibly deletes existing output directories before execution begins.

---

### Q13: What is the Combiner in MapReduce? Could one be used in `GameStats`?
**Answer:**  
A **Combiner** is a mini-reducer that runs on the Mapper node before data is sent across the network to reducers. For `GameStats`:
- The max operation is associative and commutative ($\max(\max(a, b), c) = \max(a, \max(b, c))$).
- The sum operations for helpful votes and total votes are also associative and commutative.  
Therefore, a Combiner can pre-aggregate these values on each mapper, reducing network shuffle traffic.

---

### Q14: How does Apache Pig perform lazy evaluation?
**Answer:**  
Statements like `LOAD`, `FILTER`, `FOREACH`, and `GROUP` do not trigger actual computation when parsed. Instead, Pig compiles them into an internal Directed Acyclic Graph (DAG) logical plan. Actual execution begins only when an action statement (`DUMP` or `STORE`) is executed. This enables optimizations such as projection pruning and filter pushdown.

---

### Q15: How does this project handle systems where Hadoop is not installed?
**Answer:**  
We developed `tools/pig_runner.py` and `tools/mapreduce_runner.py`. If native Hadoop and Pig are installed, they delegate execution directly to `pig` and `hadoop jar`. If not installed (common on Windows personal machines), they faithfully simulate the identical MapReduce and Pig relational operations and generate standard Hadoop `part-r-00000` text output files for demonstration and verification.
