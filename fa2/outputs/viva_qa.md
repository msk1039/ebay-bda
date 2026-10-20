# Viva Q&A

**1. Why Spark instead of MapReduce?**
MapReduce writes intermediate results to disk between every stage, while Spark keeps data in memory where it can, so multi-step analysis (joins, aggregations, iterative work) is much faster. Spark also gives DataFrames, SQL and window functions, so the code is shorter and easier to read.

**2. What are transformations and actions, and what is lazy evaluation?**
Transformations (filter, select, join, groupBy) only build a plan; nothing runs. Actions (count, show, collect, toPandas, write) trigger execution, and Spark optimises the whole plan first. We used `explain()` to show the plan before any action.

**3. Why did you store data as Parquet?**
Parquet is columnar and compressed, so Spark reads only the columns a query needs and keeps the schema with the data. CSV has to be parsed in full each time. Our cleaned tables (for example order_lines, 112,650 rows) are written once and re-read fast.

**4. What does caching do?**
`cache()` keeps a DataFrame in memory after it is first computed, so later actions reuse it instead of recomputing the whole chain from disk. It helps when the same table feeds several queries, like our joined order_lines table, but it is wasted if used only once.

**5. RDD versus DataFrame: what did your demo show?**
A DataFrame has a schema and goes through Spark's optimiser, so it is usually faster and shorter. In our demo, selecting a single column before calling `.rdd` cut the runtime from about 74 s to about 1 s, because `df.rdd` converts every row into Python objects, so fewer columns means far less conversion. Both approaches gave the same top-5 states (SP first with 41,746 orders).

**6. What is a shuffle and which operations cause it?**
A shuffle moves data across the network so that rows with the same key end up on the same partition. `groupBy`, joins and `repartition` cause it, and it is usually the most expensive step in a Spark job, so we avoid unnecessary ones.

**7. Why use window functions?**
They compute a value for each row using related rows without collapsing the data. We used `lag` to get month-over-month growth (for example November 2017 orders +63.2%) and `rank` to order categories by revenue.

**8. Why Olist as a stand-in for eBay?**
eBay does not publish transaction data, and Olist has the same marketplace model: independent sellers selling to buyers. It is public, has about 100k orders over nine linked tables (good for joins), and includes delivery dates and reviews.

**9. How did you choose each advanced visualization?**
We matched the chart to the question: a time-series line for growth over time (Q1), a weekday-by-hour heatmap for two-dimensional patterns (Q2), a map for geographic concentration (Q3), and a network graph for seller-state to buyer-state flows (Q5). Simple bars were enough for the late-vs-score comparison (Q4) and the top categories (Q6).

**10. How would you scale this to eBay volumes?**
Run the same PySpark code on a YARN cluster reading Parquet from HDFS, partition data by date, and avoid collecting large results to the driver. Use Spark Structured Streaming for real-time alerts, and serve results from HBase or Cassandra.

**11. Why do late orders score 2.27 but on-time orders 4.29?**
Late orders average 2.27 stars against 4.29 for on-time ones, a gap of 2.02, and 53.77% of late orders get 1 star versus 6.62%. This is a correlation: lateness probably drives dissatisfaction, but product and seller differences may also play a part, and the overall delay-vs-score correlation is only -0.27.

**12. Why did you drop some months and clean the data?**
Months with only a handful of orders (2016-09, 2016-12, 2018-09, 2018-10) would make growth percentages meaningless, so Q1 uses 2017-01 to 2018-08. In cleaning we kept the latest review per order to avoid duplicate rows in joins, and averaged 1,000,163 geolocation points into 19,010 zip-prefix rows.
