# Unit 3: Apache Spark Frameworks

How to use this file: each syllabus topic follows the same shape (one-liner, plain explanation, mechanism, example, key points). The running example is your own **FA2 project**: Spark on the Olist marketplace data (a public stand-in for eBay). Wherever a concept appears, the exact file in `fa2/analysis/` is named, so you can say "I did this" in the viva. Related notes: `01-unit1-big-data-basics.md`, `02-unit2-hadoop-and-nosql.md` (MapReduce, HDFS, YARN), `04-unit4-big-data-visualization.md`, `05-fa1-ebay-presentation-guide.md`.

---

## Big data analytics with Apache Spark

### In one line
**Apache Spark** is an open-source, distributed engine that processes large datasets in parallel across a cluster, mostly in memory, with one unified toolkit for batch, SQL, streaming, machine learning and graphs.

### Explain it like I'm new
Imagine a restaurant kitchen with one chef. Orders pile up. A big-data engine is a kitchen with many chefs (machines) who each cook part of the order at the same time, and a head chef (the **driver**) who splits the work and collects the plates.

Hadoop **MapReduce** (Unit 2) was the first popular such kitchen, but after every step each chef wrote the half-cooked food to the disk store room and the next step read it back. That is safe but slow. Spark's idea: **keep the half-cooked food on the counter (RAM)** between steps, and plan the whole recipe in advance so unnecessary trips are skipped.

Key terms: a **cluster** is a group of machines working together; a **node** is one machine; **in-memory** means data held in RAM instead of disk; **batch** means processing a large stored dataset in one go; **streaming** means processing data continuously as it arrives.

### History
- **2009**: started as a research project at **AMPLab, UC Berkeley** (Matei Zaharia), motivated by MapReduce being slow for iterative algorithms and interactive queries.
- **2010**: open-sourced.
- **2013**: donated to the Apache Software Foundation.
- **2014**: became an Apache **top-level project**; Spark 1.0 released the same year.
- **Spark 2.x (2016)**: SparkSession, DataFrames/Datasets unified, Structured Streaming.
- **Spark 3.x (2020)**: Adaptive Query Execution, dynamic partition pruning. **Spark 4.x** (2025) continues this line (ANSI SQL mode by default and other changes).
- The company Databricks was founded by the creators of Spark.

### Why Spark is faster than MapReduce
```text
MapReduce job chain                       Spark job
 HDFS -> Map -> Reduce -> HDFS             HDFS -> transform -> transform -> transform -> result
        (disk write)    (disk write)                 (kept in RAM, planned as one DAG)
 HDFS -> Map -> Reduce -> HDFS
```
1. **In-memory processing**: intermediate results stay in RAM (spilling to disk only if memory is full).
2. **DAG execution**: Spark builds a **Directed Acyclic Graph** (a flow chart with arrows and no loops) of the whole job, then optimises it, instead of treating every map/reduce as a separate disk-to-disk job.
3. **Fewer disk writes**: only shuffles and final output touch disk.
4. **Cheap tasks**: tasks run as threads inside long-lived executor processes, so there is no JVM start-up per task.
5. **Caching** makes iterative algorithms (machine learning, PageRank) much faster, since data is re-read from RAM, not HDFS.

Spark is often reported to be up to ~10x faster on disk-heavy jobs and up to ~100x on in-memory iterative jobs (the Spark project's own claim; real speed-ups depend on the workload).

### The unified stack
| Component | What it does | Typical API | FA2 link |
|---|---|---|---|
| **Spark Core** | Scheduling, memory management, fault recovery, the **RDD** API | `sc.textFile`, `map`, `reduceByKey` | `rdd_demo.py` |
| **Spark SQL / DataFrames** | Structured data, SQL queries, Catalyst optimiser | `spark.read`, `groupBy`, `spark.sql` | Every script (`q1`...`q6`) |
| **Structured Streaming** | Continuous processing of live data treated as an ever-growing table (older API: **DStreams**, now legacy) | `spark.readStream`, `writeStream` | Mentioned as future work in `insights.md` |
| **MLlib** | Scalable machine learning (classification, clustering, recommendation) | `Pipeline`, `KMeans`, `ALS` | Not used, covered below |
| **GraphX** | Graph computation (PageRank, connected components); Scala/Java API | Pregel | Q5 was a "network" idea but done as a DataFrame groupBy |

### Languages and where it runs
- **Languages**: Scala (Spark's native language), Java, **Python (PySpark)**, R (SparkR), and SQL. Your project uses PySpark.
- **Cluster managers**: **local** (one machine, for learning), **Standalone** (Spark's own), **Hadoop YARN**, **Kubernetes** (and historically Apache Mesos, now removed in Spark 4). Spark is storage-agnostic: it reads HDFS, S3, local files, Cassandra, HBase, JDBC databases, Kafka.

### Remember
- Born at **AMPLab, UC Berkeley, 2009**; Apache top-level **2014**.
- Faster than MapReduce because of **in-memory**, **DAG**, **fewer disk writes**.
- One stack: **Core, SQL/DataFrames, Streaming, MLlib, GraphX**.
- Spark **replaces MapReduce, not HDFS**: it is a compute engine and uses HDFS/S3 for storage.

**Common confusion:** "Spark is always 100x faster." Not always; the gain is largest for iterative and multi-step jobs. For a single pass over huge data, the gap is small.

---

## Spark core

### In one line
Spark's core is a **driver** that turns your code into a DAG of **stages** and **tasks**, which **executors** on worker machines run in parallel, with a **cluster manager** handing out the machines.

### Explain it like I'm new
Think of a post office sorting a mountain of parcels. The **driver** is the manager with the master plan. The **cluster manager** is HR: it assigns workers. **Executors** are the workers, each with their own table (memory) and several hands (cores). A **task** is one worker sorting one pile of parcels (one **partition** of data).

A **partition** is a slice of the dataset. One task processes one partition, so more partitions means more parallelism, up to the number of cores available.

### Architecture
```text
            +----------------------------------+
            |  Driver program                  |
            |  (your main script)              |
            |   SparkSession / SparkContext    |
            |   DAG Scheduler -> Task Scheduler|
            +---------------+------------------+
                            | 1. ask for executors
                            v
                  +-------------------+
                  |  Cluster Manager  |  (local / Standalone / YARN / Kubernetes)
                  +---------+---------+
          2. launches       |
   +------------------------+------------------------+
   v                        v                        v
+-----------+          +-----------+          +-----------+
| Executor  |          | Executor  |          | Executor  |   (JVM processes on worker nodes)
| cache     |          | cache     |          | cache     |
| task task |          | task task |          | task task |   3. driver sends tasks,
+-----------+          +-----------+          +-----------+      executors return results
```
- **Driver program**: runs `main()`, holds the `SparkSession` (which contains the `SparkContext`, the connection to the cluster), and plans the work.
- **Cluster manager**: allocates resources (CPU, memory). Spark itself does not care which one.
- **Executor**: a JVM process on a worker node that runs tasks and stores cached data. One application has its own executors for its whole life.
- **SparkSession** (since Spark 2.0) is the single entry point for DataFrames and SQL. `spark.sparkContext` gives the older `SparkContext` (entry point for RDDs).

### Application -> Job -> Stage -> Task
| Level | Meaning | Created when |
|---|---|---|
| **Application** | One running program (one SparkSession) | You start your script |
| **Job** | Work triggered by **one action** (`count`, `show`, `write`) | Each action call |
| **Stage** | A group of tasks that can run without shuffling data; stage boundaries fall at **shuffles** | Wide transformations split stages |
| **Task** | Smallest unit: one stage's code on **one partition** | One per partition per stage |

Example: `df.groupBy("customer_state").count().show()` is one job with two stages (read+partial count, then after the shuffle the final count). If the first stage has 200 partitions, it has 200 tasks.

### DAG Scheduler and Task Scheduler
1. The **DAG Scheduler** takes the lineage of the action, builds the DAG, and cuts it into **stages** at shuffle boundaries.
2. The **Task Scheduler** receives each stage as a set of tasks and sends them to executors through the cluster manager, retrying failed tasks and re-running tasks on slow ("straggler") nodes if configured.
3. Executors run tasks and return results (or write them to storage).

### Deploy modes
| Mode | Where does the driver run? | Use |
|---|---|---|
| **client** | On the machine that ran `spark-submit` | Interactive work, shells, notebooks, debugging |
| **cluster** | Inside the cluster, on a worker node | Production jobs, so the job survives if your laptop disconnects |

### `local[*]` in the FA2 project
`fa2/src/common.py` creates the session with `.master("local[2]")`: Spark runs **inside one JVM on your laptop**, with the driver and executor in the same process and 2 threads (2 tasks in parallel). `local[*]` means "use as many threads as cores". It is not a cluster, but the code is identical to what runs on YARN: only the `master` changes. That is the line to say in the viva: "Only paths and cluster size change when we move to eBay scale."

Also in `common.py`: `spark.sql.shuffle.partitions = 8` (the default is 200, too many for a small laptop dataset) and `spark.driver.memory = 1500m`.

### Remember
- Driver plans, executors work, cluster manager allocates.
- **Action -> job; shuffle -> stage boundary; partition -> task.**
- Client mode: driver on your machine. Cluster mode: driver in the cluster.
- FA2 ran in **local mode** (`local[2]`).

---

## Interactive data analysis with spark shell

### In one line
The **Spark shell** is a **REPL** (Read-Eval-Print Loop: type a line, Spark runs it and prints the result) that gives you a ready-made Spark session so you can explore data interactively.

### Explain it like I'm new
Writing a full program, submitting it and waiting is slow when you are only exploring. A shell lets you try one line at a time, see the answer, and adjust. It is like a calculator versus writing a spreadsheet macro.

Two shells exist:
- `spark-shell` : **Scala** REPL.
- `pyspark` : **Python** REPL.

When they start, two variables already exist: **`spark`** (the SparkSession) and **`sc`** (the SparkContext). You never need to create them.

### Example session (PySpark shell on the FA2 data)
```text
$ pyspark --master local[2]
>>> orders = spark.read.parquet("data/processed/orders")   # transformation (lazy)
>>> orders.printSchema()                                   # structure
>>> orders.count()                                         # ACTION: runs a job
99441
>>> late = orders.filter(orders.is_late == True)           # lazy
>>> late.count()                                           # action
>>> orders.groupBy("order_status").count().show()          # action: table of counts
>>> sc.parallelize([1, 2, 3, 4]).map(lambda x: x * x).collect()   # RDD via sc
[1, 4, 9, 16]
```
The same in the Scala shell:
```text
scala> val df = spark.read.option("header", true).csv("orders.csv")
scala> df.filter($"order_status" === "delivered").count()
```
(`99441` is the real row count of the cleaned `orders` table from `p1_prepare.py`.)

### Spark UI
While a session is alive, Spark serves a web dashboard at **http://localhost:4040** (the next application takes 4041, and so on). It shows jobs, stages, tasks, storage (cached data), executors, SQL plans and timelines. Use it to spot a slow stage, a skewed task, or a huge shuffle. It disappears when the session stops (the **History Server** replays finished jobs).

### Notebooks: the modern equivalent
**Jupyter / Databricks / Colab notebooks** are shells with saved cells, text and charts. Your **FA2 deliverable, `notebooks/fa2_olist_spark.ipynb`**, is exactly this: an interactive, executed, top-to-bottom narrative of Spark code. The notebook calls `common.get_spark()`, so `spark` exists just like in the shell.

### Remember
- `spark-shell` = Scala, `pyspark` = Python; both pre-create `spark` and `sc`.
- A REPL gives instant feedback; it is for **exploration**, not production.
- Spark UI = **localhost:4040**.
- Notebooks = the shell with memory and documentation.

---

## Writing a Spark application

### In one line
A Spark application is a standalone program that creates a SparkSession, reads data, transforms it, triggers actions, writes results and stops the session, and is launched with **`spark-submit`**.

### Explain it like I'm new
The shell is for exploring; an **application** is what you schedule nightly. The skeleton is always the same six steps:

```text
1. create SparkSession   2. read data   3. transform (lazy)
4. action (computes)     5. write output   6. spark.stop()
```

### A complete small PySpark app
```python
# top_states.py : orders per customer state, written as Parquet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Create the session (master is usually given by spark-submit instead)
spark = (SparkSession.builder
         .appName("top-states")
         .getOrCreate())

# 2. Read: Parquet keeps the schema; for CSV pass an explicit schema
orders    = spark.read.parquet("data/processed/orders")
customers = spark.read.parquet("data/processed/customers")

# 3. Transform (lazy: only builds the plan)
result = (orders.join(customers, "customer_id")
                .groupBy("customer_state")
                .agg(F.count("*").alias("orders"))
                .orderBy(F.desc("orders")))

# 4. Action: show() triggers the job
result.show(5)

# 5. Write (also an action); "overwrite" replaces old output
result.write.mode("overwrite").parquet("outputs/top_states")

# 6. Always release resources
spark.stop()
```

### Running it with `spark-submit`
```text
spark-submit \
  --master yarn \                  # local[*] | spark://host:7077 | yarn | k8s://https://...
  --deploy-mode cluster \          # client (default) or cluster
  --driver-memory 2g \
  --executor-memory 4g \           # RAM per executor
  --executor-cores 2 \             # cores (parallel tasks) per executor
  --num-executors 10 \             # number of executors (YARN/Kubernetes)
  --conf spark.sql.shuffle.partitions=200 \
  top_states.py
```
| Option | Meaning |
|---|---|
| `--master` | Which cluster manager to use |
| `--deploy-mode` | Where the driver runs (client/cluster) |
| `--executor-memory` | Memory per executor |
| `--num-executors` | How many executors (cores total = executors x cores each) |
| `--conf key=value` | Any Spark setting |

### The FA2 scripts as real applications
Each file in `fa2/analysis/` follows the six steps:
- **`p1_prepare.py`**: reads 9 CSVs, profiles, cleans, writes Parquet. Ends with `spark.stop()`.
- **`q1_time_series.py`**: `spark.read.parquet` -> filter -> `groupBy` -> `lag` -> `toPandas()` (action, tiny result) -> chart -> `spark.stop()`.
- Header comments say `Run: python analysis/q1_time_series.py`. Run with plain `python`, the session is created in local mode by `common.get_spark()`; on a cluster you would drop the hard-coded master and use `spark-submit`.

### Remember
- Six steps: **session, read, transform, action, write, stop**.
- Set the master in `spark-submit`, not in code, for portability.
- Nothing runs until an **action** (or write).

---

## RDD's and Optimization Techniques

### RDD definition

#### In one line
An **RDD (Resilient Distributed Dataset)** is Spark's basic data structure: an **immutable**, **partitioned** collection of records spread across the cluster, processed in parallel, **lazily evaluated** and **fault-tolerant through lineage**.

#### Explain it like I'm new
Take the phrase word by word.
- **Distributed**: the data is split into **partitions** living on different machines.
- **Dataset**: a collection of records (lines of text, tuples, Python objects).
- **Resilient**: if a machine dies and a partition is lost, Spark rebuilds it, rather than failing.
- **Immutable**: you never edit an RDD; every transformation produces a **new** RDD.
- **Lazy**: transformations only record *what to do*; computation waits for an action.

**Lineage** is the recorded recipe (the chain of transformations from the source data). If partition 3 of the final RDD is lost, Spark replays just the steps needed for that partition from the original file. No need to keep 3 copies as HDFS does.

#### Creating RDDs
```python
sc = spark.sparkContext

rdd1 = sc.parallelize([1, 2, 3, 4, 5], numSlices=2)   # from a Python list (testing)
rdd2 = sc.textFile("hdfs:///logs/app.log")              # from a file: one element per line
rdd3 = df.select("customer_state").rdd                  # from a DataFrame: an RDD of Row objects
rdd4 = rdd1.map(lambda x: x * 10)                       # from another RDD (transformation)
```
Other: `sc.wholeTextFiles(dir)` (one record per file), `sc.sequenceFile`.

### Transformations vs actions

**Transformations** build a new RDD and are **lazy**. **Actions** return a value to the driver or write to storage and **trigger** execution.

| Transformations (lazy) | What it does | Actions (run now) | What it does |
|---|---|---|---|
| `map(f)` | Apply `f` to each element | `collect()` | Return all elements to driver (dangerous on big data) |
| `filter(f)` | Keep elements where `f` is true | `count()` | Number of elements |
| `flatMap(f)` | Map, then flatten (one -> many) | `take(n)` / `first()` | First n elements |
| `reduceByKey(f)` | Combine values per key (pair RDD) | `reduce(f)` | Combine all elements into one value |
| `groupByKey()` | Gather all values per key | `saveAsTextFile(path)` | Write to storage |
| `join(other)` | Join two pair RDDs on key | `foreach(f)` | Run `f` on each element (side effects, runs on executors) |
| `distinct()` | Remove duplicates | `countByKey()` | Count per key, returned as dict |
| `union(other)` | Combine two RDDs | `takeOrdered(n)` | n smallest elements |

Mini example:
```python
nums = sc.parallelize(range(1, 11))
evens = nums.filter(lambda x: x % 2 == 0)     # lazy
squares = evens.map(lambda x: x * x)          # lazy
print(squares.collect())                      # ACTION -> [4, 16, 36, 64, 100]
print(squares.reduce(lambda a, b: a + b))     # ACTION -> 220
```

**Common confusion:** "`rdd.map(...)` read the file." No: it only records the step. Nothing is read until the action.

### Narrow vs wide transformations and shuffles

- **Narrow**: each output partition depends on **one** input partition (`map`, `filter`, `flatMap`, `union`). No data moves across the network; runs in the same stage (**pipelined**).
- **Wide**: an output partition needs data from **many** input partitions (`reduceByKey`, `groupByKey`, `join`, `distinct`, `repartition`, DataFrame `groupBy`). Spark must **shuffle**: write data to local disk, send it across the network so equal keys meet, and read it back. A shuffle is the most expensive operation and creates a **stage boundary**.

```text
NARROW (map/filter)                    WIDE (reduceByKey / groupBy)
 P1 -----> P1'                          P1 --\  /--> P1'
 P2 -----> P2'                          P2 ---\/---> P2'      every input partition
 P3 -----> P3'                          P3 ---/\---> P3'      may send data to every
 (one-to-one, no network)               (SHUFFLE: disk + network)   output partition
 |------ Stage 1 ------|                |-- Stage 1 --|  |-- Stage 2 --|
```

Spark's performance advice in one sentence: **do as many narrow operations as possible, and as few, and as small, shuffles as possible**.

### Lazy evaluation, DAG and lineage
- Transformations only extend the **lineage graph**.
- An action makes the **DAG Scheduler** walk back through the lineage, cut it into stages, and run it.
- Benefits: Spark sees the **whole** plan before running, so it can pipeline steps, push filters early, and skip unneeded work.
- You can see the lineage with `rdd.toDebugString()` and the DataFrame plan with `df.explain()`.

**FA2 example:** in `p1_prepare.py` the lines `order_lines = items.join(...).join(...)...select(...)` run instantly because they build only a plan. The script then calls `order_lines.explain()` **before** the first action to print that plan, with the comment "lazy: only a plan, no job run", and `write(order_lines, "order_lines")` is the action that makes Spark do the 5 joins. That is a demonstration of lazy evaluation you can describe in a viva.

### DataFrame / Dataset vs RDD
| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Structure | Unstructured objects | Rows with named, typed columns (like a table) | Typed objects (Scala/Java only) |
| Schema | None | Yes | Yes |
| Optimiser | None (your code runs as written) | **Catalyst** + **Tungsten** | Catalyst + Tungsten |
| Speed | Slower (Python objects serialised) | Usually much faster | Fast |
| Style | Functional lambdas | Declarative, SQL-like | Type-safe |
| Languages | All | All | Scala/Java only (PySpark has no Dataset) |
| Use when | Unstructured data, custom low-level control | **Default choice** for structured data | Compile-time type safety in Scala |

Rule of thumb: **use DataFrames; drop to RDDs only when you must.**

### The FA2 RDD demo and the 74-second finding
`rdd_demo.py` answers the same question (orders per `customer_state`) in two ways, asserts the results are equal (SP first with 41,746 orders, then RJ 12,852, MG 11,635, RS 5,466, PR 5,045), and compares:

```python
# DataFrame: Catalyst plans it for us
df.groupBy("customer_state").count().orderBy(F.desc("count"), "customer_state").take(5)

# RDD: we spell out every step
(df.select("customer_state").rdd
   .map(lambda r: (r.customer_state, 1))     # emit (key, 1)
   .reduceByKey(add)                          # sum per key, combining within each partition first
   .sortBy(lambda kv: (-kv[1], kv[0]))
   .take(5))
```
**Your own finding:** calling `df.rdd` on the **whole** joined DataFrame (about 20 columns, including timestamps) took about **74 seconds**; selecting just `customer_state` first dropped it to about **1.2 seconds** (the saved run recorded 1.16 s for the RDD and 0.41 s for the DataFrame).

**Why:** `df.rdd` has to convert **every row into a Python `Row` object**, moving every column out of Spark's compact internal format into the Python interpreter. Cost is roughly rows x columns, so fewer columns means far less conversion. The DataFrame path never leaves the JVM and its optimiser reads only the needed column.

Lessons: (1) select/prune columns **before** crossing into Python; (2) the DataFrame API is faster *and* shorter; (3) both give identical answers, so the difference is engineering, not correctness. Timings are informational and machine-dependent.

The demo also prints partitions: **2** before and **4** after `repartition(4)`, showing partitions as the unit of parallelism.

### Optimization techniques

#### 1. Caching and persistence
If a dataset feeds several actions, Spark would otherwise **recompute it from the source each time**. `cache()` or `persist()` keeps it after the first computation.

```python
df.cache()          # shorthand for the default storage level
df.count()          # an action is needed to actually fill the cache
# ... many later queries reuse it ...
df.unpersist()      # free memory when done
```
```python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
```
| Storage level | Meaning |
|---|---|
| `MEMORY_ONLY` | In RAM only; partitions that do not fit are recomputed (default for RDD `cache()`) |
| `MEMORY_AND_DISK` | RAM first, spill the rest to disk (default for DataFrame `cache()`) |
| `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` | Stored serialised: smaller, slower to read (Scala/Java; PySpark always stores pickled data) |
| `DISK_ONLY` | Disk only |
| `*_2` (e.g. `MEMORY_AND_DISK_2`) | Replicate each partition on 2 nodes |
| `OFF_HEAP` | Outside the JVM heap |

Caching is lazy too: it fills on the first action. **Cache only data reused more than once**, otherwise you waste memory.

**FA2:** `p1_prepare.py` does `raw = {k: read(k).cache() for k in SCHEMAS}` because each raw table is profiled many times; `q1_time_series.py` caches `monthly` (used for printing, the SQL cross-check, and `toPandas`); `q2_heatmap.py` caches `orders` before counting and pivoting; `rdd_demo.py` caches the joined `df` and runs `df.count()` first "to materialise the cache so timings compare fairly".

#### 2. Partitioning: `repartition` vs `coalesce`
| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | Increase **or** decrease | Only **decrease** |
| Shuffle | Full shuffle, evenly balanced | No full shuffle, merges neighbouring partitions |
| Use | Fix skew, increase parallelism, partition by a column (`repartition("col")`) | Reduce small output files before writing |

Guidelines: too few partitions means idle cores and huge tasks; too many means scheduling overhead. A common target is 2-4 tasks per CPU core and partitions of roughly 100-200 MB. `spark.sql.shuffle.partitions` (default 200) controls the number of partitions after a shuffle; FA2 set it to 8 for a laptop.

#### 3. Avoid `groupByKey`; use `reduceByKey` (map-side combine)
```python
# BAD: every (word,1) pair crosses the network, then are summed
pairs.groupByKey().mapValues(sum)
# GOOD: each partition first adds its own counts, then only small partial sums cross the network
pairs.reduceByKey(lambda a, b: a + b)
```
```text
groupByKey:                 reduceByKey:
P1: (a,1)(a,1)(b,1) --->    P1: (a,2)(b,1) ---> shuffle only 2 records
 all 3 records shuffled     (combined locally first = "map-side combine")
```
Less data shuffled means faster and less risk of running out of memory. Related: `aggregateByKey`, `combineByKey`. `rdd_demo.py` uses `reduceByKey(add)` for exactly this reason (comment: "combining within partitions first").

#### 4. Broadcast variables and broadcast joins
A **broadcast variable** is a read-only value copied **once to each executor** instead of being shipped with every task.
```python
states = {"SP": "Sao Paulo", "RJ": "Rio de Janeiro"}
b = sc.broadcast(states)
rdd.map(lambda s: b.value.get(s, "other"))
```
A **broadcast join** applies the same idea to DataFrames: when one table is small, copy it to every executor so the big table is **never shuffled**.
```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small_lookup), "category")
```
Spark does this automatically when a table is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). **FA2 fit:** the tiny `product_category_name_translation` lookup (joined in `p1_prepare.py`) and the 3,095-row `sellers` table are ideal broadcast candidates. Spark will pick broadcast on its own for them.

#### 5. Accumulators
A write-only-from-tasks, read-on-driver **shared counter** for things like "how many bad records did I skip?".
```python
bad = sc.accumulator(0)
def parse(line):
    try: return int(line)
    except ValueError: bad.add(1); return None
rdd.map(parse).count()      # read bad.value only AFTER an action
```
Caveat: inside transformations, updates may be applied more than once if tasks are retried, so use accumulators for diagnostics only.

#### 6. Predicate pushdown and column pruning (with Parquet)
- **Column pruning**: read only the columns the query uses.
- **Predicate pushdown**: send filters (`WHERE price > 100`) down to the file reader so unneeded **row groups** are skipped using Parquet's min/max statistics.
Both work because **Parquet is columnar** (values of one column are stored together). CSV forces reading and parsing every column of every row. You can prove it in `explain()`: look for `PushedFilters: [...]` and a short `ReadSchema` in the `FileScan parquet` line.

**FA2:** `p1_prepare.py` writes all cleaned tables with `df.write.mode("overwrite").parquet(...)`; every `q*.py` then reads Parquet, so `q2_heatmap.py` reading only three columns (`order_id`, `purchase_dow_num`, `purchase_hour`) from a ~27-column `order_lines` file touches only those three.

#### 7. Catalyst optimizer
**Catalyst** is the rule-based query optimiser behind DataFrames, Datasets and SQL.
```text
Your DataFrame code or SQL
        |  parse
        v
 Unresolved logical plan
        |  analyse (check tables/columns against the catalog)
        v
 Analysed logical plan
        |  optimise (rules: push filters down, prune columns, fold constants, reorder joins)
        v
 Optimised logical plan
        |  generate candidate physical plans, pick cheapest by cost
        v
 Physical plan  --->  Tungsten code generation  --->  RDD tasks on executors
```
```python
df.explain()                  # physical plan
df.explain("extended")        # parsed, analysed, optimised logical + physical
df.explain("formatted")       # readable, numbered version
```
**FA2:** `p1_prepare.py` calls `order_lines.explain()` before the write. In the physical plan you would see `BroadcastHashJoin` for small tables and `Exchange` nodes (= shuffles) for large ones. Because both DataFrame and SQL go through Catalyst, `q1_time_series.py` can compute monthly totals with the DataFrame API and with `spark.sql(...)` on a temp view and **assert the two are equal**. They compile to the same kind of plan.

#### 8. Tungsten
The execution engine under Catalyst: stores data in compact **binary format off the Java object heap** (less garbage collection), uses **cache-friendly layouts**, and **whole-stage code generation** (fuses a pipeline of operators into one tight generated Java function). This is why DataFrames beat RDDs, which hold bulky Python/Java objects.

#### 9. Adaptive Query Execution (AQE)
Since Spark 3.0 (enabled by default from 3.2; `spark.sql.adaptive.enabled`), Spark re-optimises **while the query runs**, using real statistics from finished shuffle stages:
- **Coalesce shuffle partitions**: merges many tiny post-shuffle partitions.
- **Switch join strategy**: turns a sort-merge join into a broadcast join if one side turns out small.
- **Skew join handling**: splits oversized partitions.

#### 10. Avoid Python UDFs
A **UDF** (user-defined function) runs your Python per row, so rows must be serialised JVM -> Python -> JVM, and Catalyst cannot see inside it. Prefer built-in `pyspark.sql.functions` (the whole of FA2 uses them: `F.datediff`, `F.when`, `F.lag`). If you must use Python, use **pandas UDFs** (`@pandas_udf`, vectorised with Apache Arrow), which are much faster than row-at-a-time UDFs.

#### 11. Data skew and salting
**Skew**: one key holds most of the data, so one task runs for ages while others idle (in Olist, state `SP` has 41,746 of 99,441 orders, so grouping or joining by state is skewed). Fixes: AQE skew-join, broadcast the small side, or **salting**: append a random number to the hot key to split it across partitions, aggregate, then aggregate again.
```python
salted = df.withColumn("key_salt", F.concat("customer_state", F.lit("_"), (F.rand() * 8).cast("int")))
# aggregate on key_salt first, then strip the salt and aggregate again
```

#### 12. File formats
| Format | Layout | Compression | Schema | Pushdown | Use |
|---|---|---|---|---|---|
| **CSV** | Row-based text | None/poor | None (guess or give one) | No | Exchange, small data |
| **JSON** | Row-based text | Poor | Self-describing, slow to infer | No | APIs, logs |
| **Parquet** | **Columnar** binary | Excellent (snappy default) | Stored in file | Yes | Default for Spark analytics |
| **ORC** | Columnar binary | Excellent | Stored in file | Yes | Hive/Hadoop world |
| **Avro** | Row-based binary | Good | Stored in file | Limited | Streaming/Kafka, write-heavy |

**FA2:** `p1_prepare.py` uses an **explicit schema** (`StructType`/`StructField`) on every CSV read instead of `inferSchema`. Reasons given in its comments: faster (no extra pass to guess types) and zip prefixes stay `STRING` so `"01037"` keeps its leading zero. Then it writes Parquet; every later script just reads it.

### Remember
- RDD = immutable, partitioned, lazy, fault-tolerant via **lineage**.
- Narrow = no shuffle (map/filter); wide = shuffle (reduceByKey/join/groupBy), creates a stage boundary.
- Optimisation checklist: **cache reused data, reduce shuffles (reduceByKey, broadcast join), right-size partitions, Parquet + column pruning + pushdown, built-in functions not UDFs, watch skew.**
- Catalyst plans, Tungsten executes, AQE adapts at run time.
- Your finding: `df.rdd` on all columns about 74 s vs one column about 1.2 s.

---

## Spark Algorithm

The syllabus wording is vague. These are the algorithms normally taught with Spark: word count, PageRank, K-means, classification, collaborative filtering, and graph processing.

### In one line
Spark algorithms are standard data-processing and machine-learning procedures rewritten as **parallel transformations on partitioned data**; Spark's edge is on **iterative** ones that re-use the same data many times.

### 1. Word count (the "Hello World" of big data)
**Idea:** split text into words, map each to `(word, 1)`, add the 1s per word. Same logic as the MapReduce word count in `02-unit2-hadoop-and-nosql.md`, but in a few lines.
```python
lines = sc.textFile("data/sample.txt")                      # RDD of lines
counts = (lines.flatMap(lambda line: line.lower().split())  # one line -> many words
               .map(lambda word: (word, 1))                  # emit (word, 1)
               .reduceByKey(lambda a, b: a + b))             # wide: sum per word
for word, n in counts.sortBy(lambda kv: -kv[1]).take(10):    # actions
    print(word, n)
counts.saveAsTextFile("out/wordcount")                       # action: writes part files
```
```text
"to be or not to be"  -> flatMap -> to,be,or,not,to,be
 -> map -> (to,1)(be,1)(or,1)(not,1)(to,1)(be,1)
 -> reduceByKey -> (to,2)(be,2)(or,1)(not,1)
```
**FA2 link:** `rdd_demo.py` section (3) is the same pattern, "Word-count style on categories": `cats.map(lambda c: (c, 1)).reduceByKey(add)` gives top categories by item count (bed_bath_table 11,115 items, health_beauty 9,670, sports_leisure 8,641). Counting orders per state is word count where the "word" is the state.

### 2. PageRank (iterative graph algorithm)
**Idea:** a page is important if important pages link to it. Each page starts with rank 1.0; each iteration, every page splits its rank equally among the pages it links to, and a page's new rank = 0.15 + 0.85 x (sum of received contributions) (0.85 is the damping factor).
```python
links = sc.parallelize([("A", ["B", "C"]), ("B", ["C"]), ("C", ["A"]), ("D", ["C"])]).cache()
ranks = links.mapValues(lambda _: 1.0)

for i in range(10):                                           # iterate
    contribs = (links.join(ranks)                             # (page, (neighbours, rank))
                .flatMap(lambda x: [(dst, x[1][1] / len(x[1][0])) for dst in x[1][0]]))
    ranks = (contribs.reduceByKey(lambda a, b: a + b)         # shuffle: sum contributions
                     .mapValues(lambda s: 0.15 + 0.85 * s))
print(ranks.collect())
```
**Why Spark beats MapReduce here:** PageRank loops tens of times over the **same link graph**. MapReduce re-reads the graph from HDFS and writes ranks back **in every iteration**. Spark `cache()`s `links` in memory once and each iteration reuses it. The same holds for K-means and logistic regression (below). **GraphX** (Scala/Java) and the **GraphFrames** package provide built-in `pageRank()`, connected components, shortest paths and triangle count.

**FA2 link:** Q5 (`q5_network.py`) built a seller-state to buyer-state flow network. That is the type of data you would feed PageRank, but it used a DataFrame `groupBy` rather than a graph algorithm.

### 3. K-means clustering (unsupervised)
**Idea:** group similar rows into **k** clusters. Pick k random centres; assign each point to the nearest centre; move each centre to the mean of its points; repeat until stable. Each iteration scans all data, so **cache the input**.
**Pipeline: `VectorAssembler` -> model -> evaluate.** MLlib models need all features packed into **one vector column**.
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# customer segments by spending behaviour, from FA2-style data (illustrative)
cust = (spark.read.parquet("data/processed/order_lines")
        .groupBy("customer_unique_id")
        .agg(F.count("*").alias("items"), F.sum("price").alias("spend"),
             F.avg("review_score").alias("avg_score"))
        .na.drop())
feats = VectorAssembler(inputCols=["items", "spend", "avg_score"], outputCol="features").transform(cust)
model = KMeans(k=4, seed=42, featuresCol="features").fit(feats.cache())
pred = model.transform(feats)                                  # adds a 'prediction' column
print(ClusteringEvaluator().evaluate(pred))                    # silhouette score (-1..1, higher = better)
print(model.clusterCenters())
```
In practice, scale features first (`StandardScaler`), since `spend` would otherwise dominate distance. Choose k by trying several values and comparing silhouette scores.

### 4. Classification: logistic regression
**Idea:** supervised learning that predicts a **category** (spam/not spam, late/on-time). Logistic regression outputs a probability between 0 and 1 and a class by threshold.
**Same pipeline shape:** assemble features -> split -> fit -> predict -> evaluate.
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# predict late delivery (is_late) from price, freight, estimated days (illustrative)
data = (spark.read.parquet("data/processed/order_lines")
        .filter("is_late IS NOT NULL")
        .withColumn("label", F.col("is_late").cast("double"))
        .select("label", "price", "freight_value", "estimated_days").na.drop())

train, test = data.randomSplit([0.8, 0.2], seed=42)
pipe = Pipeline(stages=[
    VectorAssembler(inputCols=["price", "freight_value", "estimated_days"], outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label")])
model = pipe.fit(train)
auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC") \
        .evaluate(model.transform(test))
print("AUC:", auc)
```
Other MLlib classifiers: `DecisionTreeClassifier`, `RandomForestClassifier`, `GBTClassifier`, `NaiveBayes`. Use `MulticlassClassificationEvaluator` for accuracy/F1. Note: late deliveries are only about 6.77% of orders in FA2 (class imbalance), so accuracy alone would mislead; AUC or recall on the late class is better. (This model is a teaching illustration, not something the FA2 project ran.)

### 5. Collaborative filtering with ALS (recommendations)
**Idea:** recommend items by learning from **past behaviour of many users**: "people who bought what you bought also bought...". **ALS (Alternating Least Squares)** factorises the sparse user-item rating matrix into two small matrices (user factors and item factors); multiplying them predicts missing ratings. It alternates: fix item factors and solve for users, then fix users and solve for items; this parallelises well on Spark.
```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# ratings: userId (int), itemId (int), rating (float)
train, test = ratings.randomSplit([0.8, 0.2], seed=42)
als = ALS(userCol="userId", itemCol="itemId", ratingCol="rating",
          rank=10, maxIter=10, regParam=0.1,
          coldStartStrategy="drop")                 # drop users/items not seen in training
model = als.fit(train)
rmse = RegressionEvaluator(metricName="rmse", labelCol="rating",
                           predictionCol="prediction").evaluate(model.transform(test))
model.recommendForAllUsers(5).show(truncate=False)  # top-5 items per user
```
**eBay link:** "Recommended for you", "similar items" and "people also viewed" are recommendation problems. On Olist the users are `customer_unique_id`s and the items are products; the 4.29 vs 2.27 review-score finding could become the (implicit/explicit) rating signal. ALS also supports **implicit feedback** (`implicitPrefs=True`) for clicks and purchases, which suits marketplaces. Note: user and item ids must be integers.

### 6. GraphX (graph processing) in brief
**GraphX** is Spark's graph library (Scala/Java): a graph is two RDDs, **vertices** and **edges**, with built-in algorithms (PageRank, connected components, triangle counting, shortest paths) and the **Pregel** message-passing API for iterative graph computation. In Python use **GraphFrames** (a separate package built on DataFrames). Examples: social networks, fraud rings, seller-buyer networks.

### Compare: why these algorithms like Spark
| Algorithm | Type | Iterative? | Key Spark feature used |
|---|---|---|---|
| Word count | Batch aggregation | No | flatMap, reduceByKey |
| PageRank | Graph | Yes (about 10-20 loops) | cache, join, reduceByKey |
| K-means | Clustering | Yes | cache the data |
| Logistic regression | Classification | Yes | cache, MLlib Pipeline |
| ALS | Recommendation | Yes | MLlib, blocked matrices |

### Remember
- Word count = `flatMap -> map -> reduceByKey`.
- PageRank, K-means, logistic regression are **iterative**: Spark's in-memory caching is the win over MapReduce.
- MLlib pattern: **VectorAssembler -> estimator.fit -> model.transform -> Evaluator**.
- ALS = collaborative filtering for recommendations (needs `coldStartStrategy="drop"` for evaluation).
- DataFrame-based `pyspark.ml` is the current API; the older RDD-based `pyspark.mllib` is in maintenance mode.

---

## Case study: Twitter Sentiment Analysis

### In one line
Classify each tweet as positive, negative or neutral at scale, then aggregate by time or topic to understand public opinion.

### Problem
Millions of short, noisy tweets arrive every day. A brand (say, eBay) wants to know **what people feel about it right now**: is a new fee policy causing anger, is a campaign well received? Reading them manually is impossible and the data keeps flowing in, so we need an automated, scalable and ideally near-real-time pipeline. This is a **volume, velocity and variety** problem, the 3 Vs from `01-unit1-big-data-basics.md`.

### Pipeline
```text
 Twitter/X API  or  stored tweet dataset (CSV/JSON)
        |
        v
 Ingestion: Kafka topic "tweets"   (buffer, decouples producer from Spark)
        |
        v
 Spark Structured Streaming   (readStream from Kafka, micro-batches)
        |
        v
 Clean text: lowercase, remove URLs/@mentions/#, punctuation, stopwords; tokenize
        |
        v
 Sentiment scoring
     (a) lexicon-based: VADER / TextBlob as a UDF  -- no training data needed
     (b) ML: TF-IDF + Logistic Regression / Naive Bayes (MLlib) -- needs labelled tweets
        |
        v
 Aggregate: groupBy(window(timestamp, "5 minutes"), hashtag / brand) -> counts, % positive
        |
        v
 Sink: Parquet / database / Kafka  ->  Dashboard (Grafana, Superset, Tableau, notebook)
```
- **Kafka** is a distributed message queue; it absorbs bursts so Spark can read at its own pace. (Introduced in the streaming/ingestion part of the course.)
- **Structured Streaming** treats the live stream as an unbounded table; your DataFrame code works on it unchanged. Use `spark.readStream.format("kafka")...` and `writeStream`.
- **Window**: `F.window("timestamp", "5 minutes")` buckets events into time slices, like FA2's `purchase_month` grouping but for minutes.

### Cleaning steps
1. Lowercase.
2. Remove URLs (`http\S+`), mentions (`@\w+`), the `#` symbol (keep the word), punctuation and digits.
3. Handle emojis (map to words, or keep as tokens, as they carry sentiment).
4. **Tokenize** (split into words), remove **stopwords** (the, is, and), optionally stem/lemmatize.

### Batch version: labelled CSV of tweets (MLlib pipeline)
Assume `tweets.csv` has columns `text` and `label` (0 = negative, 1 = positive).
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("tweet-sentiment").getOrCreate()

df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
      .csv("tweets.csv")
      .withColumn("label", F.col("label").cast("double"))
      .na.drop(subset=["text", "label"]))

# clean: lowercase, strip URLs, @mentions, non-letters
df = (df.withColumn("text", F.lower("text"))
        .withColumn("text", F.regexp_replace("text", r"http\S+|@\w+", " "))
        .withColumn("text", F.regexp_replace("text", r"[^a-z\s]", " ")))

train, test = df.randomSplit([0.8, 0.2], seed=42)

pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),                    # split into words
    StopWordsRemover(inputCol="words", outputCol="clean_words"),      # drop 'the', 'is'
    HashingTF(inputCol="clean_words", outputCol="tf", numFeatures=2**16),  # term frequency
    IDF(inputCol="tf", outputCol="features"),                          # down-weight common words
    LogisticRegression(featuresCol="features", labelCol="label", maxIter=20),
])
model = pipeline.fit(train)
pred = model.transform(test)

acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                        metricName="accuracy").evaluate(pred)
print("accuracy on held-out tweets:", acc)     # report the number you actually get; do not assume it

# the streaming version applies the SAVED model to new tweets:  model.transform(stream_df)
```
**What TF-IDF means:** **TF** (term frequency) = how often a word appears in a tweet; **IDF** (inverse document frequency) = down-weights words that appear in almost every tweet. The product highlights words that characterise a tweet. `HashingTF` maps words to vector slots by hashing (fast, no vocabulary kept; occasional collisions); `CountVectorizer` is the vocabulary-based alternative.

Lexicon option: **VADER** or **TextBlob** score each tweet from a word list with polarity values, requiring no training. In PySpark wrap it in a (pandas) UDF; it is simple but weak on context.

### Challenges
- **Sarcasm and irony** ("Great, another delayed parcel") look positive to word-based models.
- **Slang, abbreviations, misspellings, hashtags** ("lol", "gr8", "#fail").
- **Emojis and emoticons** carry sentiment but need special handling.
- **Multilingual and code-mixed text** (e.g., Hinglish).
- **Class imbalance** and **domain drift** (words change meaning over time or topic).
- **Noisy labels**: even humans disagree on neutral vs mildly positive.
- **Bots and spam**, **short length** (little context).
- **API access**: since 2023 X (formerly Twitter) restricted free API access and moved most useful access behind paid tiers, so projects now often use archived public datasets or other sources instead. Check the current X developer terms.

### Applications
Brand and reputation monitoring, election and political opinion tracking, customer service triage (route angry tweets first), product launch feedback, financial market sentiment, public-health or crisis monitoring.

### Link to FA2/eBay
The Olist `review_comment_message` column (kept in `reviews` by `p1_prepare.py`, with `review_score` 1-5) is a ready-made labelled sentiment dataset: the same pipeline could predict a review's score from its text. FA2 only used the numeric score.

### Remember
- Pipeline: **ingest (Kafka) -> stream (Spark) -> clean -> classify -> aggregate by window -> dashboard**.
- MLlib text pipeline: **Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> classifier**.
- Hard parts: sarcasm, slang, emojis, languages.
- Never quote an accuracy you did not measure.

---

## How our FA2 project used Spark

| Concept | Where in the project | What to say |
|---|---|---|
| **Local mode** | `src/common.py`: `master("local[2]")`, `shuffle.partitions=8`, `driver.memory=1500m` | "Same code would run on YARN; only master and sizes change." |
| **Explicit schema** | `p1_prepare.py`: `SCHEMAS` dict of `StructType`/`StructField`, `.schema(...)` on every `spark.read...csv` | "Faster than inferSchema and keeps zip prefixes as strings." |
| **Robust CSV reading** | `p1_prepare.py` `read()`: `multiLine`, `quote`, `escape`, `mode=PERMISSIVE` | "Review comments span lines and contain quotes." |
| **Cache** | `p1_prepare.py` (`raw` tables), `q1` (`monthly.cache()`), `q2` (`orders.cache()`), `rdd_demo.py` | "Reused data is kept in memory; first action fills it." |
| **Joins / denormalised fact table** | `p1_prepare.py` `order_lines`: `order_items` LEFT JOIN orders, customers, sellers, products, reviews; asserts 112,650 = 112,650 rows (no fan-out) | "Left joins on unique keys so row count cannot grow." |
| **left_anti / left_semi joins** | `p1_prepare.py`: orphan items and untranslated categories | "Anti-join finds rows with no match; semi-join filters." |
| **Window function (row_number)** | `p1_prepare.py`: latest review per order; most frequent city per zip | "Deduplicate with `row_number().over(Window.partitionBy(...).orderBy(...))`." |
| **Lazy evaluation + explain()** | `p1_prepare.py`: `order_lines.explain()` printed before `write(...)` | "Transformations build a plan; the write is the action." |
| **Parquet** | `p1_prepare.py` `write()` -> `data/processed/*.parquet`; all `q*.py` read it | "Columnar, compressed, schema included, column pruning." |
| **Window lag (Q1)** | `q1_time_series.py`: `Window.orderBy("purchase_month")`, `F.lag("orders").over(w)` -> MoM %: Nov 2017 +63.2% | "Lag looks at the previous row without collapsing data." |
| **Spark SQL temp view (Q1)** | `q1`: `valid.createOrReplaceTempView("valid_lines")`, `spark.sql(...)`, `assert sql_rows == df_rows` | "DataFrame and SQL go through the same Catalyst optimiser; results are equal." |
| **Pivot (Q2)** | `q2_heatmap.py`: `groupBy("purchase_dow_num").pivot("purchase_hour", list(range(24))).sum("orders")` -> 7x24 matrix | "Listing pivot values avoids an extra pass to find them." |
| **distinct + toPandas only at the end** | `q2`: `.select(...).distinct()`; `toPandas()` on 7 rows | "Heavy lifting in Spark; pandas only for a tiny result." |
| **Rank window (Q6)** | `q6_categories.py`: `F.rank().over(w)` and per-state partitioned rank | "Top-N per group." |
| **Multi-table join + groupBy (Q5)** | `q5_network.py` | "Seller state to buyer state flows." |
| **RDD demo** | `rdd_demo.py`: `map -> reduceByKey -> sortBy -> take`; results equal DataFrame; `df.rdd` 74 s vs 1.2 s with `select` first; `repartition(4)` 2 -> 4 partitions | "RDD = manual and unoptimised; DataFrame = optimised." |
| **Actions** | `count`, `show`, `take`, `collect`, `toPandas`, `write` | "Each triggers a job." |

Real results to quote (from `insights.md`): orders grew from 787 (Jan 2017) to 6,421 (Aug 2018), about 8.16x; peak November 2017 with 7,421 orders; Monday busiest (16.3%); SP, RJ, MG give 63.35% of GMV; late orders average 2.27 stars vs 4.29 on time.

**Scaling to eBay (from FA2 `insights.md`):** run the same PySpark on YARN reading HDFS, keep Parquet, use Structured Streaming for real-time alerts, serve results from HBase/Cassandra.

---

## Quick revision

1. **Spark** = distributed in-memory engine; AMPLab Berkeley 2009; Apache top-level 2014.
2. Faster than MapReduce: **RAM, DAG, fewer disk writes**, cached iterations.
3. Stack: **Core (RDD), SQL/DataFrames, Structured Streaming, MLlib, GraphX**; languages Scala/Java/Python/R/SQL.
4. Architecture: **Driver (SparkSession/SparkContext) + Cluster Manager + Executors**; runs on local, Standalone, YARN, Kubernetes.
5. **Application -> Job (per action) -> Stage (split at shuffles) -> Task (per partition)**; DAG Scheduler makes stages, Task Scheduler sends tasks.
6. Deploy mode: **client** (driver local) vs **cluster** (driver in cluster). FA2 = `local[2]`.
7. Shells: `spark-shell` (Scala), `pyspark`; `spark` and `sc` ready; Spark UI at **localhost:4040**.
8. App skeleton: **session -> read -> transform -> action -> write -> stop**; launch with `spark-submit --master --deploy-mode --executor-memory --num-executors`.
9. **RDD** = immutable, partitioned, lazy, resilient via lineage. Create: `parallelize`, `textFile`, `df.rdd`.
10. **Transformations lazy; actions trigger.** Narrow (map, filter) vs wide (reduceByKey, groupBy, join) = shuffle = stage boundary.
11. DataFrames beat RDDs because of **Catalyst + Tungsten**; FA2: `df.rdd` on all columns about 74 s vs one column about 1.2 s.
12. Optimise: **cache/persist, repartition vs coalesce, reduceByKey over groupByKey, broadcast join, accumulators, Parquet pushdown and pruning, AQE, no Python UDFs, fix skew by salting.**
13. Algorithms: **word count, PageRank, K-means, logistic regression, ALS**; MLlib: `VectorAssembler -> fit -> transform -> evaluate`.
14. Twitter sentiment: ingest -> Kafka -> Structured Streaming -> clean -> TF-IDF + LR or VADER -> window aggregate -> dashboard. Challenges: sarcasm, slang, emojis, API restrictions.

---

## Viva questions

**1. What is Apache Spark and where did it come from?**
Spark is an open-source distributed processing engine for big data that keeps data in memory across a cluster. It started at UC Berkeley's AMPLab in 2009 and became an Apache top-level project in 2014.

**2. Why is Spark faster than Hadoop MapReduce?**
MapReduce writes to disk after every map and reduce stage, while Spark keeps intermediate data in RAM, plans the whole job as a DAG, and avoids disk writes except at shuffles. Caching makes iterative algorithms (ML, PageRank) especially fast. Gains vary by workload.

**3. Name the components of the Spark stack.**
Spark Core (RDDs, scheduling), Spark SQL/DataFrames, Structured Streaming (older: DStreams), MLlib for machine learning, and GraphX for graphs. All share the same engine, so one application can mix them.

**4. Explain Spark architecture.**
The driver program holds the SparkSession/SparkContext and builds the DAG. A cluster manager (local, Standalone, YARN, Kubernetes) provides resources. Executors on worker nodes run tasks on partitions and cache data, returning results to the driver.

**5. What is the difference between a job, a stage and a task?**
A job is triggered by one action. It is divided into stages at shuffle boundaries, and each stage is made of tasks, one per partition. The task is the smallest unit of work.

**6. Client vs cluster deploy mode? What did your project use?**
In client mode the driver runs on the submitting machine (good for shells and notebooks); in cluster mode it runs inside the cluster (good for production). The FA2 project ran in local mode, `local[2]`, on one laptop, with the same code that would run on YARN.

**7. How do you run a Spark application on a cluster?**
Package the script and run `spark-submit` with options such as `--master yarn`, `--deploy-mode cluster`, `--executor-memory 4g`, `--num-executors 10`. Inside the script: create a SparkSession, read, transform, run an action, write, and call `spark.stop()`.

**8. What is an RDD and why is it "resilient"?**
An RDD is an immutable, partitioned collection of records processed in parallel. It is resilient because Spark remembers its lineage (the chain of transformations), so a lost partition is recomputed from the source rather than the job failing.

**9. Transformations vs actions with examples. What is lazy evaluation?**
Transformations (map, filter, reduceByKey, join) return a new RDD/DataFrame and only record a plan. Actions (count, collect, take, saveAsTextFile) trigger execution. Lazy evaluation lets Spark see the whole plan first and optimise it; in FA2 we printed `order_lines.explain()` before the write that actually ran the joins.

**10. Narrow vs wide transformations, and what is a shuffle?**
Narrow transformations (map, filter) need data from one input partition and run pipelined in one stage. Wide ones (groupBy, reduceByKey, join) need data from many partitions, so Spark shuffles, writing and moving data across the network. A shuffle is costly and ends a stage.

**11. Why prefer `reduceByKey` over `groupByKey`?**
`reduceByKey` combines values within each partition first (map-side combine), so only small partial results are shuffled. `groupByKey` shuffles every record, which is slower and may run out of memory. `rdd_demo.py` uses `reduceByKey(add)`.

**12. RDD vs DataFrame: what did your demo show?**
A DataFrame has a schema and passes through Catalyst and Tungsten, so it is usually faster and shorter. Our demo gave identical top-5 states with both, but converting the whole DataFrame with `df.rdd` took about 74 s versus about 1.2 s after selecting one column, because `df.rdd` builds a Python object for every row and every column.

**13. What are cache() and persist(), and when should you use them?**
They keep a dataset in memory (or memory and disk) after the first action so later actions reuse it. Use them for data reused several times, such as the Parquet tables profiled repeatedly in `p1_prepare.py`, and call `unpersist()` afterwards. `persist` lets you pick a storage level such as MEMORY_AND_DISK; caching a dataset used only once wastes memory.

**14. `repartition` vs `coalesce`?**
`repartition(n)` can increase or decrease partitions with a full shuffle and balanced sizes. `coalesce(n)` only decreases, merging partitions without a full shuffle, so it is cheaper, for example to reduce the number of output files.

**15. What are broadcast variables/joins and accumulators?**
A broadcast variable is a read-only copy of a small value sent once to every executor; a broadcast join uses this to join a big table with a small one without shuffling the big one (`broadcast(small)`). An accumulator is a shared counter that tasks add to and only the driver reads, used for diagnostics such as counting bad records.

**16. Explain Catalyst, Tungsten and AQE.**
Catalyst is the optimiser: it turns your query into a logical plan, applies rules (predicate pushdown, column pruning), and chooses a physical plan; `explain()` shows it. Tungsten is the execution engine with binary memory layout and whole-stage code generation. AQE re-optimises at run time using actual statistics, for example coalescing small partitions, switching to a broadcast join, or splitting skewed partitions.

**17. Why use Parquet instead of CSV?**
Parquet is columnar, compressed and stores the schema, so Spark reads only the needed columns (column pruning) and skips data using filters (predicate pushdown). CSV must be parsed completely each time and has no types. FA2 converted all cleaned tables to Parquet once and re-read them quickly.

**18. Why avoid Python UDFs, and how do you handle data skew?**
A Python UDF moves every row between the JVM and Python and hides logic from Catalyst, so built-in functions (or Arrow-based pandas UDFs) are faster. Skew means one key dominates (such as state SP); fix with AQE skew handling, a broadcast join, or salting the key so work spreads over more partitions.

**19. Describe a typical Spark ML pipeline and one algorithm that benefits from Spark.**
Assemble features with `VectorAssembler`, fit an estimator (e.g., `KMeans`, `LogisticRegression`, `ALS`) in a `Pipeline`, call `transform` on test data, and score it with an evaluator. Iterative algorithms such as K-means and PageRank benefit most because the cached data is reused in every iteration instead of being re-read from disk like in MapReduce.

**20. Outline a Twitter sentiment analysis pipeline in Spark and its limitations.**
Tweets are ingested (API or dataset, through Kafka), read by Structured Streaming, cleaned (lowercase, remove URLs/mentions/stopwords, tokenize), scored by a lexicon (VADER) or a TF-IDF plus logistic regression/Naive Bayes model in MLlib, aggregated by time window or hashtag, and shown on a dashboard. Limits: sarcasm, slang, emojis, multiple languages, noisy labels and restricted, paid X API access since 2023.
