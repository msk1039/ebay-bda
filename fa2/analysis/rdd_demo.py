"""RDD vs DataFrame: the SAME question (orders per customer_state) answered two ways."""
import sys, time
from operator import add
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
import pyspark.sql.functions as F

spark = common.get_spark("rdd_demo")
# One row per order (orders table has no duplicates) joined to customer state
df = (spark.read.parquet(common.processed("orders")).join(
      spark.read.parquet(common.processed("customers")), "customer_id").cache())
df.count()   # action: materialise the cache so timings compare fairly

# (1) DataFrame API: declarative; Catalyst optimiser plans it for us
t = time.time()
df_res = df.groupBy("customer_state").count().orderBy(F.desc("count"), "customer_state").take(5)
t_df = time.time() - t

# (2) RDD API: low-level; WE spell out map -> reduceByKey (map-side combine) -> sort. No optimiser.
t = time.time()
# select() the one column first: df.rdd turns every row into a Python object, so converting
# all ~20 columns (incl. timestamps) is very slow (~70 s here vs ~1 s for one column).
rdd_res = (df.select("customer_state").rdd
           .map(lambda r: (r.customer_state, 1))          # emit (key, 1) pairs
           .reduceByKey(add)                              # sum per key, combining within partitions first
           .sortBy(lambda kv: (-kv[1], kv[0]))            # descending count, tie-break by name
           .take(5))                                      # action: triggers the whole job
t_rdd = time.time() - t
assert [(r[0], r[1]) for r in df_res] == rdd_res, "RDD and DataFrame results differ!"
print("DataFrame:", df_res, "\nRDD:", rdd_res)

# (3) Word-count style on categories (classic RDD example): items per category
cats = spark.read.parquet(common.processed("order_lines")).select("category_en").rdd.map(lambda r: r[0])
top_cat = cats.map(lambda c: (c, 1)).reduceByKey(add).sortBy(lambda kv: -kv[1]).take(5)
print("Top categories by item count (RDD):", top_cat)

# Partitions = units of parallelism; repartition() shuffles data into a new number of partitions
n_before = df.rdd.getNumPartitions()
n_after = df.repartition(4).rdd.getNumPartitions()
print(f"partitions before={n_before}, after repartition(4)={n_after}")
print(f"timing (informational): DataFrame {t_df:.2f}s, RDD {t_rdd:.2f}s")

common.save_metrics("rdd_demo", {
    "top5_states_orders_dataframe": [[r[0], int(r[1])] for r in df_res],
    "top5_states_orders_rdd": [[k, int(v)] for k, v in rdd_res],
    "results_match": True,
    "top5_categories_by_items_rdd": [[k, int(v)] for k, v in top_cat],
    "partitions_before": n_before, "partitions_after_repartition4": n_after,
    "time_dataframe_s": round(t_df, 2), "time_rdd_s": round(t_rdd, 2),
    "note": "all orders incl. canceled (simple demo); timings informational only",
})
spark.stop()
