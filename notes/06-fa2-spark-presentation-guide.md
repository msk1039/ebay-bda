# FA2 Spark presentation guide (read this before you present)

You will present FA2 (Units 3 and 4, 10 marks) by scrolling through one executed Jupyter notebook. You do not need to be a Spark expert. You need to (1) know what is in the data, (2) say in plain words what Spark did to it, and (3) say what we found and what we recommend.

The notebook is `fa2/notebooks/fa2_olist_spark.ipynb` (29 cells, already executed, outputs saved). It follows a guided path: Part 1 Dataset (Steps 1-3), Part 2 Spark (Steps 4-7), Part 3 Outcomes (Findings 1-3), Summary. Each markdown cell has a "Say:" cue. This guide expands those cues.

Sources: the notebook outputs and `fa2/outputs/metrics.json` (source of truth for numbers), plus `insights.md`, `viva_qa.md` and `data_quality.md` in the same folder. Theory depth is in `03-unit3-apache-spark.md` (Unit 3) and `04-unit4-big-data-visualization.md` (Unit 4). Sibling guide for FA1: `05-fa1-ebay-presentation-guide.md`.

Cell numbers below count from 0, as in the notebook file.

---

## 1. The project in 60 seconds

**Problem.** An online marketplace wants to know when and where its buyers are, and whether bad delivery hurts its reputation. The data is too big and too spread over many tables to eyeball, so we use Apache Spark to join and summarise it, then charts to show the answer.

**Why Olist and not eBay.** Our FA1 case study was eBay. eBay publishes no transaction data, so we cannot analyse its orders. The Olist Brazilian e-commerce dataset (public, on Kaggle) has the same model: independent sellers sell to buyers through one platform. So Olist is a public stand-in. We do not claim the numbers are eBay's.

**The question.** *When and where do people buy, and does late delivery hurt customer reviews?*

**The three answers.**

1. **When:** demand peaks at Black Friday. November 2017 had 7,421 orders and about BRL 1.0 million, and monthly orders grew 8.16 times between January 2017 and August 2018.
2. **Where:** buyers are concentrated in Sao Paulo. The state SP has 41.88% of all orders.
3. **Late delivery:** late orders average 2.27 stars, on-time orders average 4.29.

**Thesis (say this one sentence):** *Using Spark on a marketplace's nine linked tables, we found that demand spikes at Black Friday, buyers cluster in Sao Paulo, and late delivery cuts review scores by about two stars, so the marketplace should plan for peaks, spread its sellers and warehouses, and promise delivery dates it can keep.*

---

## 2. Words you must know

Skim these first. Every one appears in the notebook. "Depth" says which unit note explains it (U3 = `03-unit3-apache-spark.md`, U4 = `04-unit4-big-data-visualization.md`).

| Term | Plain meaning | Depth |
| --- | --- | --- |
| **Spark** | An engine that processes big data across many machines, keeping data in memory where it can. Faster and easier than MapReduce. | U3 "Big data analytics with Apache Spark" |
| **PySpark** | Spark used from Python. All our code is PySpark. | U3 |
| **SparkSession** | The front door to Spark. You create one (`spark`) and everything starts from it. Notebook cell 2. | U3 "Spark core" |
| **Driver / executor** | The driver is the boss program that plans the work. Executors are the workers that do it. | U3 "Architecture" |
| **Local mode** | Driver and executors all run on one laptop. We used 4 CPU cores. On a cluster the same code runs unchanged. | U3 "Deploy modes" |
| **DataFrame** | A table in Spark: rows and named, typed columns. Like a pandas table or an SQL table, but spread over many machines. | U3 "DataFrame / Dataset vs RDD" |
| **Schema** | The list of column names and types. We wrote it by hand (cell 4) so zip codes stay text and keep their leading zero. | U3 |
| **CSV vs Parquet** | CSV is plain text with no types. Parquet is a compressed, column-based binary format that carries its schema and lets Spark read only the columns it needs. Our raw data is CSV. | U3 |
| **Transformation** | A step that only describes work: `filter`, `withColumn`, `join`, `groupBy`. Spark does nothing yet. | U3 "Transformations vs actions" |
| **Action** | A step that makes Spark really run: `count`, `show`, `collect`, `toPandas`. | U3 |
| **Lazy evaluation** | Spark waits until an action, then optimises the whole chain of steps at once. | U3 "Lazy evaluation, DAG and lineage" |
| **explain() / execution plan / Catalyst** | `explain()` prints the plan Spark will follow without running it. Catalyst is the optimiser that rewrites the plan to make it cheaper. | U3 |
| **cache()** | Keep a DataFrame in memory after it is first computed, so later questions reuse it instead of re-reading CSV files. | U3 "Optimization techniques" |
| **Join (left / inner)** | Combine two tables on a shared key. A left join keeps every row of the left table. An inner join keeps only rows that match on both sides. | U3 |
| **groupBy / agg** | Group rows by a column (for example state) and calculate a summary per group (count, sum, average). | U3 |
| **Window function** | Calculates a value for each row using related rows, without collapsing the table. `row_number` numbers rows inside a group (we keep one review per order). `lag` reads the previous row (we get last month's orders). | U3 |
| **Spark SQL / temp view** | Register a DataFrame as a named view, then query it with ordinary SQL. Same engine, same answer as the DataFrame code. | U3 |
| **Shuffle** | Moving data between machines so rows with the same key land together. Needed for `groupBy`, joins and window functions. The most expensive step in Spark. | U3 "Narrow vs wide transformations and shuffles" |
| **toPandas()** | Brings a small Spark result to the driver as a pandas table so matplotlib can chart it. We use it only on tiny results (20 rows). | U3, U4 |
| **GMV / revenue** | GMV is gross merchandise value: the sum of item prices. In our notebook, "revenue" means item price in BRL, freight excluded. | - |
| **Valid orders** | All orders except those with status `canceled` or `unavailable`. | - |
| **Time-series** | Values tracked over time (orders per month). The chart is a line. | U4 "3.3 Time-series visualization" |
| **Geospatial** | Data with a place on Earth (latitude and longitude). The chart is a map. | U4 "3.1 Geospatial visualization" |
| **Zip prefix** | The first five digits of a Brazilian postal code. The geolocation table gives one lat/lng per prefix, which is how we place buyers on the map. | - |

---

## 3. What our dataset has

**Source.** Olist Brazilian E-Commerce Public Dataset (Kaggle). **Period:** 5 September 2016 to 18 October 2018. **Size:** 99,441 orders and 112,650 order lines (one row per product bought), across nine CSV files.

### The nine files

| File | Rows | What it holds | Key columns |
| --- | ---: | --- | --- |
| orders | 99,441 | One row per order: status, purchase date, delivered date, promised (estimated) date | `order_id`, `customer_id` |
| order_items | 112,650 | One row per product in an order: price, freight | `order_id`, `product_id`, `seller_id` |
| customers | 99,441 | Buyer city, state, zip prefix | `customer_id`, zip prefix |
| sellers | 3,095 | Seller city, state | `seller_id` |
| products | 32,951 | Category and size of each product | `product_id` |
| payments | 103,886 | How each order was paid | `order_id` |
| reviews | 99,224 | 1-5 star score and comment | `order_id` |
| geolocation | 1,000,163 | Latitude and longitude points per zip prefix | zip prefix |
| category translation | 71 | Portuguese to English category names | category name |

The row counts appear in the notebook's table at Step 2 (cell 4) and match `data_quality.md`.

### How the tables link

```text
                      customers ---- customer_id ----+
                      (state, zip prefix)            |
                                                     v
 reviews --- order_id --> ORDERS <--- order_id --- order_items --- seller_id ---> sellers
 payments -- order_id -->  (dates, status)         (price, freight)                (state)
                                                     |
                                                     +--- product_id ---> products --- category name ---> translation

 geolocation --- zip prefix ---> customers (and sellers): gives lat/lng for the map
```

Say it simply: **orders is the hub.** Items, reviews and payments point to it by `order_id`. Customers hang off it by `customer_id`. Products and sellers hang off items. Geolocation joins by zip prefix.

### Data-quality points worth saying out loud

- **Canceled and unavailable orders removed** for the analysis: 625 canceled and 609 unavailable. After removal the notebook shows 98,207 valid orders (cell 8) and 112,101 valid order lines (cell 12).
- **Thin months excluded from the trend.** September 2016, December 2016, September 2018 and October 2018 each have fewer than 100 orders (the extract is incomplete there), so the time-series uses January 2017 to August 2018.
- **One review per order.** 547 orders have more than one review. We keep the latest, using a window function, so joins do not duplicate rows.
- **Join check.** Joining items to orders, customers and reviews must keep 112,650 rows. The notebook asserts this (cell 10): no row lost or duplicated.
- **Geolocation had 1,000,163 rows** with many repeats, so we average them to one point per zip prefix and keep only points inside Brazil.
- **Missing delivery dates are left empty, not guessed.** Late-delivery results use only orders with a delivery date.

---

## 4. Walking through the notebook

For each step: what the cell does, which syllabus tool it shows, what to point at, what to say, and likely questions.

### Opening (cell 0): title, context, question

- **Tools:** none yet (this is problem identification).
- **Point at:** "Group members", the context paragraph, the research question, the presentation path.
- **Say:** "Our FA1 case was eBay. eBay publishes no transaction data, so we use the Olist Brazilian marketplace dataset, which has the same seller-to-buyer model, as a public stand-in. Our question is: when and where do people buy, and does late delivery hurt reviews? We will go in three parts: the dataset, how we used Spark, and what we found."

**If they ask...**

- *Why Olist?* eBay has no public order data. Olist is public, about 100 thousand orders, nine linked tables, with delivery dates and reviews, which is what our question needs.
- *Does this prove anything about eBay?* No. It shows the method on a marketplace of the same type. Brazilian patterns will not transfer exactly.
- *Why this question?* Timing, location and delivery quality are the three levers any marketplace controls: capacity, supply and logistics.

### Part 1, Step 1: Start Spark (cells 1-2)

- **Tools:** SparkSession, local mode.
- **Point at:** `common.get_spark("fa2-notebook", cores=4, memory="3g")` and the printed "Spark version: 4.2.0". (The warnings above it are normal.)
- **Say:** "First we start Apache Spark. The SparkSession is the entry point. We run in local mode on this laptop with 4 cores. On a real cluster the same code runs with the work spread over many machines."

**If they ask...**

- *What is local mode?* The driver and workers all run on one machine, using 4 cores in parallel. Good for development and small data.
- *Would you change the code for a cluster?* Almost not at all. You change the master setting, the memory sizes and the file paths (HDFS instead of local folders).
- *What is the SparkSession?* The single object through which we read data, run SQL and set up configuration.

### Part 1, Step 2: Load the nine CSV files (cells 3-4)

- **Tools:** DataFrame, explicit schema, CSV read, action (`count`).
- **Point at:** the `SCHEMAS` dictionary, the `multiLine` and `escape` options, the output table with a "rows" column.
- **Say:** "The data comes as nine CSV files. Spark reads each one into a DataFrame. We write the column types ourselves, so zip codes stay text and keep their leading zero. Review comments have line breaks and quotes, so we turn on multiLine. Then we count the rows. Counting is an action, so this is the moment Spark really reads the files."

**If they ask...**

- *Why write a schema instead of inferSchema?* inferSchema needs an extra pass over the data and can guess wrong (a zip code like 01037 would lose its zero). An explicit schema is faster and safer.
- *Why is the count the "real" moment?* Reading is lazy. Nothing is read until an action asks for a result.
- *Why not Parquet here?* The raw data arrives as CSV, so we read CSV. Parquet is better for repeated analysis; the backup notebook shows a Parquet round-trip.

### Part 1, Step 3: What the dataset has (cells 5-6)

- **Tools:** `show()` (an action).
- **Point at:** the nine-row "Table / Contains / Linked by" table and the first five rows of orders.
- **Say:** "About 99 thousand orders from September 2016 to October 2018, spread over nine linked tables. The orders table is the hub. Here are five rows: each order has its status, when it was bought, when it was delivered and the date we promised."

**If they ask...**

- *How are the tables linked?* By keys: `order_id` links orders to items, reviews and payments; `customer_id` links to customers; `seller_id` and `product_id` link items to sellers and products; zip prefix links to geolocation.
- *How big is the data?* 99,441 orders, 112,650 order lines, and 1,000,163 geolocation points before we averaged them.
- *Any missing data?* Some delivery dates are empty (not yet delivered, or canceled) and some orders have no review. We leave them empty rather than guess.

### Part 2, Step 4: Clean with transformations (cells 7-8)

- **Tools:** transformations (`withColumn`, `filter`), action (`count`), `cache`, `datediff`.
- **Point at:** the three new columns `purchase_month`, `delivery_days`, `delay_days`, the `is_late` flag, `.cache()`, and the output "All orders: 99,441 Valid orders: 98,207".
- **Say:** "A transformation only describes what to do; an action makes Spark run. Here we describe the cleaning: we add the purchase month, the days to deliver, and a late flag that is true when the parcel arrived after the promised date. We keep valid orders, meaning everything except canceled and unavailable. Only the count at the end actually runs it: 99,441 orders in total and 98,207 valid."

**If they ask...**

- *Why is is_late sometimes empty?* If an order has no delivery date, we cannot say it is late. We leave it empty rather than guess.
- *Why cache the orders?* We reuse the table in later steps. cache keeps it in memory after first use.
- *Transformation vs action?* Transformations build a plan and are lazy. Actions trigger the run.

### Part 2, Step 5: Join the tables into `order_lines` (cells 9-10)

- **Tools:** window function `row_number`, left joins, `dropDuplicates`, cache, an assert check.
- **Point at:** the window definition (`partitionBy("order_id")`), `row_number`, the three `.join(..., "left")` lines, and the output "order_items = 112,650 rows, order_lines = 112,650 rows -> no row lost or duplicated".
- **Say:** "Now we join orders, items, customers and reviews so that every product bought becomes one row. A window function keeps only the latest review per order, because some orders have two. We start from the items table and use left joins, so every item row is kept. The check at the end proves the count stays 112,650. If it grew, a join would have duplicated rows."

**If they ask...**

- *Why a left join?* It keeps every row of the left table (items) even when there is no match, for example an order with no review.
- *What is the window function doing?* It numbers the reviews of each order from newest to oldest, and we keep number one. No rows are collapsed by mistake.
- *Why check the row count?* A join on a non-unique key silently multiplies rows. Equal counts prove it did not.

### Part 2, Step 6: Lazy evaluation and cache (cells 11-12)

- **Tools:** `explain()`, Catalyst, lazy evaluation, `cache`.
- **Point at:** the "== Physical Plan ==" output: `BroadcastHashJoin ... LeftOuter`, `FileScan csv`, `InMemoryTableScan`; then the line "valid order lines in memory: 112,101".
- **Say:** "Spark first plans, then runs. explain() prints the plan without running anything. You can see the three left joins, and that it reads the CSV files. Then we keep the valid lines in memory with cache, so the three findings reuse them instead of re-reading the files. Valid lines: 112,101."

**If they ask...**

- *What is Catalyst?* Spark's optimiser. It rewrites our chain of steps into a cheaper plan before running. It can only do that because Spark is lazy and sees the whole chain.
- *What is that BroadcastHashJoin?* For a small table, Spark sends a copy to every worker so the big table does not need to be shuffled. This is Catalyst choosing a cheaper join.
- *When is cache a bad idea?* When the data is used only once, or does not fit in memory.

### Part 2, Step 7: groupBy / agg and Spark SQL (cells 13-14)

- **Tools:** `groupBy`, `agg`, `countDistinct`, Spark SQL temp view.
- **Point at:** the top-5 table (SP 41,125, RJ 12,697, MG 11,496, RS 5,415, PR 4,982) and "DataFrame result == SQL result: True".
- **Say:** "The same question, orders per state, can be asked in DataFrame code or in plain SQL. We register a temporary view and write ordinary SQL. Both give identical results, because both go through the same optimiser. SP is already first with 41,125 orders."

**If they ask...**

- *Which would you use?* DataFrame API for building pipelines in code, SQL when the logic is easy to read as a query or for people who know SQL. Same speed.
- *Why countDistinct?* One order can have many item rows, so counting rows would count the same order several times.
- *Does groupBy cause a shuffle?* Yes. Rows with the same state must be brought together.

### Part 3, Finding 1: Sales peak at Black Friday (cells 15-18)

- **Tools:** `groupBy`/`agg`, window `lag`, `toPandas`, matplotlib. **Advanced visualization: time-series.**
- **Point at:** the monthly table (orders, revenue, mom_pct), the two-line chart with the red peak dot labelled "Peak Nov 2017, 7,421 orders" and "BRL 1,004k", and the Result 1 text.
- **Say:** "First, when do people buy? We count orders and revenue per month. `lag` looks at the previous month, so we get month-over-month growth. We use January 2017 to August 2018, because the other months have fewer than 100 orders. The chart is a time series. November 2017 is the peak: 7,421 orders, about 1.0 million reais, 63.2 percent up on October. Orders grew 8.16 times from January 2017 to August 2018. We tag the peak as Black Friday, which is a likely reason but we did not prove it."

**If they ask...**

- *Why exclude some months?* Months with 1 to 16 orders would make growth percentages meaningless, for example "+500%" from tiny numbers.
- *Why two panels?* Orders and revenue have different units, so we give each its own y-axis and share the time axis.
- *Why a line chart?* The question is "how does it change over time". A line shows trend and peak at a glance.
- *Why does the chart title say 8.2x but the result say 8.16x?* Same number, rounded differently. 8.16 is the exact one.

### Part 3, Finding 2: Buyers are concentrated in Sao Paulo (cells 19-22)

- **Tools:** `groupBy`, `countDistinct`, `filter` with `between` (Brazil box), `avg` per zip prefix, a join to coordinates, `toPandas`. **Advanced visualization: geospatial.**
- **Point at:** the state table (SP 41.88%), the left bar chart (SP in red) and the right map with the colour bar labelled "log scale", then Result 2.
- **Say:** "Second, where do people buy? We count orders per state, and we put buyers on a map using the zip-code coordinates. The geolocation table has a million points, so we first average them to one point per zip prefix. SP alone is about 42 percent of orders, 41,125 of 98,199. SP, RJ and MG together hold 66.5 percent of orders. The dots cluster on the south-east coast. The colour scale is logarithmic, so dense areas do not wash out the rest."

**If they ask...**

- *Why a log colour scale?* A few zip prefixes have hundreds of orders and most have one or two. On a straight scale, nearly all dots would look the same pale colour. Log spreads the colours out.
- *Why average the geolocation points?* One zip prefix has many nearby points. One point per prefix gives one dot per place and avoids joining a million rows onto the orders.
- *What does the bar plus map combination add?* The bars give exact ranking; the map shows geography, which bars cannot.
- *Why is the SP total 98,199 and not 98,207?* The 98,207 valid orders come from the orders table, while the state counts start from order lines (orders that have at least one item), which gives 98,199.

### Part 3, Finding 3: Late delivery lowers review scores (cells 23-26)

- **Tools:** inner join to reviews, `groupBy`/`agg`, `F.when` buckets, `toPandas`. Chart: bar chart with annotations.
- **Point at:** the table `is_late` true: 6,381 orders, 2.27; false: 89,443 orders, 4.29. Then the bucket table and the bar chart (red bars are late).
- **Say:** "Third, does late delivery hurt reviews? We compare the average star score of on-time and late orders. On time is 4.29 stars over 89,443 orders. Late is 2.27 stars over 6,381 orders, a drop of two stars. And it gets worse with delay: 1 to 3 days late is 3.29, 4 to 7 days is 2.10, and more than a week late is around 1.7."

**If they ask...**

- *Does late delivery cause low scores?* We show correlation, not proof. Late orders might also differ in product or seller. The overall delay-versus-score correlation is only -0.27, so lateness is not the only factor, but the drop grows steadily with delay, which supports the link.
- *What is "late"?* Delivered date later than the promised (estimated) date.
- *Why inner join here?* We only want orders that have both a delivery result and a review.
- *Why is "On time (0)" worse than "Early"?* Orders delivered exactly on the promised day score 4.03 against 4.29 for early ones: the margin matters to buyers. We would not over-read it.

### Summary (cells 27-28)

- **Tools:** all of the above in one slide.
- **Point at:** the Finding / Evidence / Recommendation table, then the "Syllabus tools we used" line, then the last cell ("Notebook runtime: 170s").
- **Say:** "Three findings, each backed by Spark numbers, each leading to one action. Plan capacity and promotions before sale peaks. Recruit sellers and consider warehouses outside SP. Give realistic delivery estimates and warn buyers early about delays. Tools we used: SparkSession, DataFrames, transformations and actions, joins, groupBy and agg, the window functions row_number and lag, Spark SQL, cache, lazy evaluation with explain, and time-series and geospatial charts. We have more analysis ready if you want to see it."

**If they ask...**

- *Which recommendation matters most?* Delivery promises: the drop from 4.29 to 2.27 stars is the largest effect we measured, and the marketplace controls the promise.
- *What are the limitations?* Stand-in data from Brazil 2016-2018, correlation not proof, a short period.
- *How would this change on eBay?* Same PySpark on a YARN cluster reading Parquet from HDFS; results could be served from HBase or Cassandra.

---

## 5. The 5-minute talk

About 650 words, roughly 5 minutes at a calm pace. Scroll the notebook as you speak; the bold cue tells you where to be.

**[Cell 0, about 20 seconds: opening]**

"Good morning. We are [names]. Our FA1 case was eBay. eBay publishes no transaction data, so we use the Olist Brazilian marketplace data as a stand-in: independent sellers selling to buyers through one platform, the same model. Our question: when and where do people buy, and does late delivery hurt reviews?"

**[Steps 2-3, cells 4-6, about 1 minute: the dataset]**

"The data comes as nine linked CSV files: about 99 thousand orders, 112 thousand order lines, 3 thousand sellers, 33 thousand products, and a one-million-row geolocation file, from September 2016 to October 2018. The orders table is the hub; items, reviews and payments link by order id, customers by customer id, and geolocation by zip prefix. We removed canceled and unavailable orders, kept one review per order, and left out months with fewer than a hundred orders, because the extract is incomplete there."

**[Steps 1, 4-7, cells 2-14, about 2 minutes: how we used Spark]**

"We start a Spark session in local mode on four cores. Spark reads each file into a DataFrame with a schema we wrote ourselves. Transformations like filter, withColumn and join only describe the work. Actions like count and show make Spark run it. That is lazy evaluation, and explain shows the plan before anything runs.

We added a purchase month, delivery days and a late flag. We joined orders, items, customers and reviews into one table of 112,650 rows. We checked the count stays the same, so no join duplicated rows. A window function, row_number, kept the latest review per order. We cached the result so the next questions reuse it from memory. Then we used groupBy and agg for summaries, and showed that Spark SQL on a temporary view gives exactly the same answer as DataFrame code. Later, the window function lag gives month-over-month growth."

**[Findings 1-3, cells 15-26, about 2 minutes: outcomes]**

"First, when. The time-series shows orders growing 8.16 times between January 2017 and August 2018, peaking in November 2017 at 7,421 orders and about one million reais, probably Black Friday. Second, where. The map shows buyers clustered on the south-east coast. Sao Paulo alone has 41.88 percent of orders, and SP, Rio and Minas Gerais together 66.5 percent. Third, late delivery. On-time orders average 4.29 stars. Late orders average 2.27, and the longer the delay the lower the score."

**[Summary, cell 27, about 30 seconds: recommendations]**

"Three recommendations. One, plan capacity and promotions before sale peaks. Two, recruit sellers and consider warehouses outside Sao Paulo. Three, give realistic delivery estimates and warn buyers early about delays. These are correlations, not proof of cause, and Olist is a stand-in for eBay. The same PySpark code would run on a cluster at eBay scale. Thank you; we have more analysis ready, and we are happy to take questions."

### Who says what

| Group size | Speaker A | Speaker B | Speaker C | Speaker D |
| --- | --- | --- | --- | --- |
| 3 | Opening and Part 1 (cells 0-6): problem, dataset | Part 2 (cells 7-14): Spark steps | Part 3 and Summary (cells 15-27): findings, recommendations | none |
| 4 | Opening and Part 1 (cells 0-6) | Spark Steps 4-7 (cells 7-14), including explain and cache | Findings 1 and 2 (cells 15-22) | Finding 3, Summary and recommendations (cells 23-27); leads the questions |

Everyone should know every cell: the examiner picks who answers.

---

## 6. Rubric checklist

| Criterion | Marks | Where it is shown | Sentence that proves it |
| --- | ---: | --- | --- |
| Problem identification | 1 | Cell 0 (context, research question, presentation path) | "eBay publishes no transaction data, so we use Olist, the same seller-to-buyer model; our question is when and where people buy and whether late delivery hurts reviews." |
| Spark implementation | 3 | Part 2, Steps 1 and 4-7 (cells 1-14); tools list in cell 27 | "We used SparkSession, DataFrames, transformations and actions, joins, groupBy and agg, row_number and lag, Spark SQL, cache and explain, on 112,650 order lines." |
| Data analysis and results | 2 | Result cells 18, 22, 26; tables in cells 16, 20, 24 | "Orders grew 8.16 times with a peak in November 2017; SP has 41.88 percent of orders; late orders average 2.27 stars against 4.29 on time." |
| Visualization techniques | 2 | Cell 17 (time-series), cell 21 (geospatial map with log colour scale), cell 25 (annotated bar chart) | "We matched each chart to its question: a time-series for change over time, a geospatial map for location, and an annotated bar chart for the late-versus-on-time comparison." |
| Insights, interpretation, recommendation | 2 | Summary table, cell 27 | "Each finding has evidence and one action: plan for peaks, spread sellers beyond SP, set honest delivery estimates. These are correlations, not proof." |

---

## 7. Numbers to memorise

All numbers are checked against the notebook outputs and `metrics.json`.

| Fact | Number | Where it appears |
| --- | --- | --- |
| Files / tables | 9 | Step 2 table |
| Orders (all) | 99,441 | Step 4 output |
| Order lines (items) | 112,650 | Step 5 output |
| Valid orders (not canceled or unavailable) | 98,207 | Step 4 output |
| Valid order lines | 112,101 | Step 6 output |
| Period | 5 Sep 2016 to 18 Oct 2018 | `data_quality.md`, Step 3 |
| Orders growth, Jan 2017 to Aug 2018 | 787 to 6,421 = 8.16x | Finding 1 result |
| Peak month | Nov 2017: 7,421 orders, BRL 1,003,862 (about 1.0M), +63.2% on October | Finding 1 |
| SP orders | 41,125 of 98,199 valid orders = 41.88% | Finding 2 |
| Top 3 states (SP, RJ, MG) | 66.52% of orders; 63.35% of GMV | Finding 2 result; `metrics.json` |
| Review score, on time vs late | 4.29 (89,443 orders) vs 2.27 (6,381 orders) | Finding 3 |
| National late rate | 6.77% (6,535 of 96,476 delivered orders with a date) | `metrics.json` |
| 1-star share, late vs on time | 53.77% vs 6.62% | `metrics.json`, `insights.md` |
| Late by 1-3 days / 4-7 days / 8-14 days | 3.29 / 2.10 / 1.67 stars | Finding 3 bucket table |
| Spark setup and runtime | Spark 4.2.0, local mode, 4 cores, notebook ran in 170 s | Steps 1 and 28 |

Note on late counts: 6.77% counts every delivered order with a delivery date. The 6,381 late orders in Finding 3 are only those that also have a review, so the share there is slightly different. Do not mix the two.

---

## 8. If they ask for more

The backup notebook `fa2/notebooks/backup/fa2_olist_spark_FULL.ipynb` has the full analysis with six questions, plus extras. Open it from `fa2/notebooks/backup/` only if asked, and scroll to the section name.

| Extra analysis | What it shows | Key number |
| --- | --- | --- |
| **Purchase-time heatmap** (section 6, Q2) | Weekday by hour of purchases, built with `pivot` into a 7 x 24 grid. Buyers shop on weekdays, afternoon and evening. | Busiest cell Tuesday 14:00 with 1,106 orders; Monday busiest day (16.3%), Saturday quietest (11.0%) |
| **Seller-state network graph** (section 9, Q5) | Arrows from seller states to buyer states. SP is the hub that ships to the whole country. | 64.18% of orders cross state lines; SP sellers originate 70.76% of flows |
| **Top categories** (section 10, Q6) | Revenue per category, with a `rank()` window. | Top 10 of 74 categories = 62.37% of GMV; health_beauty leads at 9.31% |
| **RDD vs DataFrame demo** (section 11) | Same question (orders per state) with low-level RDD code and with DataFrame code; identical answers. | In this run the DataFrame took 0.13 s and the RDD 1.27 s. Our earlier script run found that calling `.rdd` on the whole wide table took about 74 s and about 1.2 s after selecting one column first (see `03-unit3-apache-spark.md`). The 74 s figure is not in the backup notebook's output. |
| **Parquet round-trip** (section 3) | Write a table to Parquet and read it back; columnar, schema carried. | The per-zip table has 19,010 rows both before and after; the geolocation CSV is 61.3 MB, the per-zip Parquet 0.48 MB (this compares raw to an averaged table, so do not call it pure compression) |
| **Interactive folium map** (section 7, Q3) | A zoomable map of GMV by 0.1-degree grid cell. | 1,898 grid cells |
| **Q4 detail** (section 8) | Late rate by state | MA 17.43% late (21.5 days average delivery) vs SP 4.49% |

---

## 9. Viva question bank (30 questions)

Answer in your own words. Short model answers below.

### A. Dataset and problem

**1. What is your dataset and why that one?**
The Olist Brazilian e-commerce dataset: 99,441 orders in nine linked tables from 2016 to 2018. eBay publishes no order data and Olist has the same seller-to-buyer model, so it is a public stand-in.

**2. What does one row of your main table represent?**
One product bought in one order (an order line). Our joined table `order_lines` has 112,650 rows, the same as the items table.

**3. What did you clean or remove, and why?**
We removed canceled and unavailable orders, kept one review per order, left out months with fewer than 100 orders from the trend, and averaged the geolocation file to one point per zip prefix. Each step avoids misleading numbers or duplicated rows.

**4. What does "valid order" mean?**
Any order whose status is not `canceled` or `unavailable`. That leaves 98,207 of 99,441.

**5. Is 100 thousand rows really big data?**
By volume, no, and we say so. The point is that the pipeline is the same PySpark code that would scale unchanged to a cluster. The dataset also has the Variety and the many-table joins of a real marketplace, and the geolocation file alone has a million rows.

### B. Spark concepts

**6. Spark vs MapReduce / Hadoop?**
MapReduce writes results to disk between every stage; Spark keeps data in memory where it can, so multi-step work is much faster. Spark has DataFrames, SQL and window functions, so code is shorter. Spark can run on Hadoop (YARN, HDFS) rather than replace it.

**7. RDD vs DataFrame?**
An RDD is the low-level distributed collection where you write each step and Spark runs it as written. A DataFrame has named, typed columns and goes through the Catalyst optimiser, so it is faster and shorter. We used DataFrames.

**8. Transformations vs actions?**
Transformations (`filter`, `withColumn`, `join`, `groupBy`) only build a plan. Actions (`count`, `show`, `collect`, `toPandas`) make Spark run it.

**9. What is lazy evaluation and why does it help?**
Spark waits for an action, then sees the whole chain of steps and optimises it together, for example reading only needed columns. `explain()` shows that plan without running it.

**10. Why did you cache?**
We reuse the joined, filtered table for all three findings. cache keeps it in memory so Spark does not re-read and re-join the CSV files each time. It is wasted if data is used only once.

**11. What is a shuffle and which of your steps cause one?**
A shuffle moves data across the network so rows with the same key end up together. Our `groupBy` steps, joins that are not broadcast, and the window functions (`row_number`, `lag`) cause one. It is usually the most expensive part of a job.

**12. Narrow vs wide transformations?**
Narrow: each output partition depends on one input partition, no shuffle (`select`, `filter`, `withColumn`). Wide: needs data from many partitions, so a shuffle (`groupBy`, `join`, window functions).

**13. Spark SQL vs DataFrame API?**
Same engine and same optimiser, same results. Our check "DataFrame result == SQL result: True" proves it. Use SQL where the logic reads naturally as a query, DataFrames for code pipelines.

**14. Why window functions?**
They give a value per row using related rows without collapsing the table. `row_number` picked the latest review per order; `lag` gave last month's orders to compute growth. A plain `groupBy` would lose the rows.

**15. Why local mode, and how would it run on a cluster?**
We have one laptop, and local mode with 4 cores is enough for 100 thousand rows. On a cluster the same code is submitted to YARN, reads Parquet from HDFS, and executors run on many nodes. Only the master, memory sizes and paths change. See `02-unit2-hadoop-and-nosql.md` and `05-fa1-ebay-presentation-guide.md` for HDFS and YARN.

**16. Why not just pandas?**
Pandas works on one machine in memory and would not scale to a million-row geolocation file joined to millions of orders. We use Spark for the heavy joins and aggregation, then `toPandas()` only on tiny results (20 rows) to chart them.

**17. What is Parquet and why mention it?**
A compressed, column-based format that keeps the schema and lets Spark read only needed columns. Raw data here is CSV, but for repeated analysis we would store Parquet (the backup notebook shows a round-trip).

**18. What does `explain()` show?**
The physical plan: how Spark will read the files and join the tables, such as the file scans and the broadcast joins. It runs nothing.

### C. Visualization

**19. Why these chart types?**
We matched each to its question: line for change over time, map for location, bar for comparing two groups. The map and the time-series are the "advanced" ones from Unit 4.

**20. What makes the time-series and the map "advanced"?**
They carry extra dimensions: time and trend with an annotated peak, and latitude/longitude with a colour scale on a log axis. A basic bar or pie chart shows only a list of values. See `04-unit4-big-data-visualization.md`, sections 3.1 and 3.3.

**21. Why a log colour scale on the map?**
Order counts per zip prefix are very uneven: a few have hundreds, most have one or two. On a straight scale most dots would look the same. Log spreads the colours so both dense and sparse areas show.

**22. Why exclude the thin months?**
September 2016, December 2016, September 2018 and October 2018 have 1 to 16 orders. Growth percentages from such numbers are meaningless and would mislead the trend.

**23. Why `toPandas()` only at the end?**
Spark does the heavy work on the big data, and the result is small (20 monthly rows, 27 states). Matplotlib needs a small in-memory table. Calling `toPandas()` on a large table would overload the driver.

### D. Results and recommendations

**24. What are your three main findings?**
Demand peaks at Black Friday (November 2017: 7,421 orders). Buyers are concentrated in SP (41.88% of orders). Late deliveries average 2.27 stars against 4.29 on time.

**25. What do you recommend?**
Plan capacity, stock and promotions before sale peaks. Recruit sellers and consider warehouses outside SP. Show realistic delivery estimates and send proactive delay messages.

**26. How do these tie to an eBay-type marketplace?**
Any marketplace has peak days, geographic concentration of buyers and sellers, and delivery quality that shapes ratings. The method (join, aggregate, chart) transfers; the numbers do not.

### E. Tricky ones

**27. Does late delivery cause low reviews?**
We cannot say cause. We show correlation: late orders score 2.27 and the score keeps falling with the number of days late. Late orders might differ in product or seller, and the overall delay-versus-score correlation is -0.27, so delay is not the whole story.

**28. Was November 2017 really Black Friday?**
It is a plausible cause: the biggest single day in our full analysis was 24 November 2017, which was Black Friday. But the data alone does not prove the reason.

**29. Brazil data vs eBay: can you generalise?**
No, not the numbers. Olist is one Brazilian platform from 2016 to 2018. The pipeline and the questions carry over to eBay or any marketplace.

**30. What would you do next with more time?**
Run the same code on a cluster with Parquet in HDFS, use Structured Streaming for real-time delay alerts, and test the lateness link properly, for example by comparing orders of the same product or seller.

---

## 10. Running it live (or not)

**Default: present from the saved outputs.** The notebook is already executed. Open it and scroll. Do not re-run anything you do not have to.

**To open it**

```text
cd /Users/meow/pccoe/btech/BDA/fa1
source .venv/bin/activate
jupyter lab fa2/notebooks/fa2_olist_spark.ipynb
# choose the kernel "Python (fa2-olist)"
```

**Timing.** A full run took 170 seconds in the saved run. On a busy laptop it can take a few minutes, and the first Spark start is slowest. If you demo live, run only Step 1 (start Spark) and one Finding, then scroll the rest.

**If Spark fails to start**

- Spark needs **Java 17 or 21**. Check with `java -version`.
- Make sure the kernel is "Python (fa2-olist)" and the virtual environment is activated.
- Restart the kernel and run the cell again; close other heavy apps to free memory.
- If it still fails, say calmly: "The outputs are saved from our run; here is what it produced." This is why we keep the executed notebook.

**Before the exam.** Do not click "Restart and Run All" unless you are sure the run works. A clean saved output is better than a half-finished new one.

---

## 11. Last-night checklist

**Before you sleep**

- [ ] Fill in **"Group members: [add names]"** in notebook cell 0, save the notebook, and check the outputs are still there.
- [ ] Decide who speaks which part (section 5) and who scrolls.
- [ ] Say the 5-minute talk aloud three times with a timer. Target under 5 minutes.
- [ ] Learn the thesis sentence and the three headline numbers: 7,421 (Nov 2017), 41.88% (SP), 4.29 vs 2.27 stars.
- [ ] Learn the tool list: SparkSession, DataFrame, transformation vs action, lazy evaluation and `explain`, `cache`, left join, `groupBy`/`agg`, `row_number` and `lag`, Spark SQL temp view.
- [ ] Open the notebook once from a cold start, so you know where each cell is.
- [ ] Charge the laptop; keep the backup notebook closed but ready; keep a PDF export of the main notebook if you can.

**What to read**

| Time | Task | File |
| --- | --- | --- |
| 30 min | Sections 1, 2 and 4 of this guide | this file |
| 45 min | Quick revision and viva questions on Spark | `03-unit3-apache-spark.md` |
| 45 min | Quick revision and viva questions on visualization, especially time-series and geospatial | `04-unit4-big-data-visualization.md` |
| 20 min | Say the talk and the viva answers aloud | sections 5 and 9 |

**If you do not know an answer**

- Do not invent a number. Say: "I do not remember the exact figure; the notebook shows it in Finding 2" and scroll to it.
- Bring it back to the notebook: "In Step 5 we show..."
- If a teammate knows, say "My teammate may add to this."
