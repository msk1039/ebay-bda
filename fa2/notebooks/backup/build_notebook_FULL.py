"""Generates fa2_olist_spark.ipynb (unexecuted). Run:  python notebooks/build_notebook.py
Then execute with nbconvert (see bottom of this file for the command)."""
import nbformat as nbf
from pathlib import Path

OUT = Path(__file__).resolve().parent / "fa2_olist_spark_FULL.ipynb"
cells = []
def md(s): cells.append(nbf.v4.new_markdown_cell(s.strip("\n")))
def code(s): cells.append(nbf.v4.new_code_cell(s.strip("\n")))

# ---------------------------------------------------------------- 0 TITLE
md(r'''
# eBay-style Marketplace Analytics with Apache Spark — FA2, Big Data Analytics

**Group members:** [add names]

## Context
Our FA1 case study was **eBay** (Hadoop/HDFS/NoSQL architecture for a large online marketplace).
eBay does not publish its transaction data, so for FA2 we use the **Olist Brazilian e-commerce marketplace dataset**
(2016-2018, about 100k orders, 9 linked tables): independent sellers sell to buyers through one platform, which is the
same marketplace model as eBay. It is a **public stand-in**, not eBay data.

## Problem statement
A marketplace grows by knowing **when** and **where** demand happens and **how well it is fulfilled**.
Using Apache Spark (PySpark, local mode) we answer six questions:

| # | Question | Spark work | Visualization |
|---|---|---|---|
| Q1 | How did orders and revenue grow month by month? | join, groupBy month, window `lag` for MoM growth | **Time-series line (advanced)** |
| Q2 | Which weekdays and hours do buyers purchase most? | `date_format`/`hour`, groupBy, `pivot` | **Heatmap (advanced)** |
| Q3 | Where are buyers and revenue concentrated? | aggregate 1M geolocation rows per zip prefix, join customers | **Geospatial map (advanced)** |
| Q4 | Do late deliveries lower review scores? | `datediff`, late flag, join reviews | Bar charts |
| Q5 | Which seller states ship to which buyer states? | 4-table join, groupBy (seller_state, customer_state) | **Network graph (advanced)** |
| Q6 | Which product categories earn the most? | join translation, groupBy, `rank` window | Horizontal bar |

## Table of contents
1. Spark setup
2. Data ingestion (explicit schemas)
3. Cleaning and feature engineering (lazy evaluation, cache, Parquet)
4. Spark SQL
5. Q1 Monthly growth (advanced: time series)
6. Q2 Weekday x hour demand (advanced: heatmap)
7. Q3 Geography of buyers (advanced: geospatial)
8. Q4 Late delivery and reviews
9. Q5 Seller-state to buyer-state flows (advanced: network graph)
10. Q6 Top product categories
11. RDD vs DataFrame
12. Insights and Recommendations
13. Spark concepts used
14. Limitations and References
''')

# ---------------------------------------------------------------- 1 SETUP
md(r'''
## 1. Spark setup
`SparkSession` is the entry point of a Spark application. We start it in **local mode with 4 cores**.
''')
code(r'''
import sys, warnings, time
from pathlib import Path
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

ROOT = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "common.py").exists())   # .../fa2
sys.path.insert(0, str(ROOT / "src"))
import common

NB_T0 = time.time()
spark = common.get_spark("fa2-notebook", cores=4, memory="3g")
spark.sparkContext.setLogLevel("ERROR")

print("Spark version      :", spark.version)
print("Master             :", spark.sparkContext.master)
print("defaultParallelism :", spark.sparkContext.defaultParallelism)
print("Session time zone  :", spark.conf.get("spark.sql.session.timeZone"))
''')
md(r'''
`local[4]` means the **driver and the executors all run inside one JVM on this machine, using 4 cores** (4 tasks can run in parallel).
On a real cluster (like eBay's Hadoop/YARN) the same code would run unchanged with executors spread over many machines.
''')

# ---------------------------------------------------------------- 2 INGEST
md(r'''
## 2. Data ingestion
We read the raw Olist CSVs with **explicit schemas** (`StructType`) instead of `inferSchema`: it is faster (no extra pass over the data)
and keeps zip prefixes as strings so `01037` does not lose its leading zero. Review comments contain line breaks and quotes,
hence `multiLine` and `escape`.
''')
code(r'''
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, TimestampType)
import pandas as pd
from IPython.display import display, Markdown

def S(name, typ=StringType()):
    return StructField(name, typ, True)          # nullable field shorthand
I, D, TS = IntegerType(), DoubleType(), TimestampType()

SCHEMAS = {
    "olist_orders_dataset": StructType([
        S("order_id"), S("customer_id"), S("order_status"),
        S("order_purchase_timestamp", TS), S("order_approved_at", TS),
        S("order_delivered_carrier_date", TS), S("order_delivered_customer_date", TS),
        S("order_estimated_delivery_date", TS)]),
    "olist_order_items_dataset": StructType([
        S("order_id"), S("order_item_id", I), S("product_id"), S("seller_id"),
        S("shipping_limit_date", TS), S("price", D), S("freight_value", D)]),
    "olist_customers_dataset": StructType([
        S("customer_id"), S("customer_unique_id"), S("customer_zip_code_prefix"),
        S("customer_city"), S("customer_state")]),
    "olist_sellers_dataset": StructType([
        S("seller_id"), S("seller_zip_code_prefix"), S("seller_city"), S("seller_state")]),
    "olist_products_dataset": StructType([
        S("product_id"), S("product_category_name"), S("product_name_lenght", I),
        S("product_description_lenght", I), S("product_photos_qty", I),
        S("product_weight_g", D), S("product_length_cm", D), S("product_height_cm", D),
        S("product_width_cm", D)]),
    "olist_order_payments_dataset": StructType([
        S("order_id"), S("payment_sequential", I), S("payment_type"),
        S("payment_installments", I), S("payment_value", D)]),
    "olist_order_reviews_dataset": StructType([
        S("review_id"), S("order_id"), S("review_score", I), S("review_comment_title"),
        S("review_comment_message"), S("review_creation_date", TS),
        S("review_answer_timestamp", TS)]),
    "olist_geolocation_dataset": StructType([
        S("geolocation_zip_code_prefix"), S("geolocation_lat", D), S("geolocation_lng", D),
        S("geolocation_city"), S("geolocation_state")]),
    "product_category_name_translation": StructType([
        S("product_category_name"), S("product_category_name_english")]),
}

def read(name):
    return (spark.read.option("header", True).option("multiLine", True)
            .option("quote", '"').option("escape", '"').option("mode", "PERMISSIVE")
            .schema(SCHEMAS[name]).csv(common.raw_csv(name)))

raw = {k: read(k).cache() for k in SCHEMAS}      # cache: every table is reused many times
orders_raw, items_raw = raw["olist_orders_dataset"], raw["olist_order_items_dataset"]
cust_raw, sell_raw = raw["olist_customers_dataset"], raw["olist_sellers_dataset"]
prod_raw, pay_raw = raw["olist_products_dataset"], raw["olist_order_payments_dataset"]
rev_raw, geo_raw = raw["olist_order_reviews_dataset"], raw["olist_geolocation_dataset"]
tr_raw = raw["product_category_name_translation"]

# count() is an ACTION: it triggers reading each CSV
counts = pd.DataFrame({"file": [k.replace("olist_", "").replace("_dataset", "") for k in raw],
                       "rows": [df.count() for df in raw.values()],
                       "columns": [len(df.columns) for df in raw.values()]})
display(counts.style.format({"rows": "{:,}"}).hide(axis="index"))
''')
code(r'''
orders_raw.printSchema()
orders_raw.show(3, truncate=False)
''')

# ---------------------------------------------------------------- 3 CLEAN
md(r'''
## 3. Cleaning and feature engineering
Condensed from our Phase 1 script. Steps: derive time and delivery columns on `orders`, normalise text, translate product categories,
keep **one review per order** (window function), average the 1M geolocation points to **one row per zip prefix**, then build one
denormalised fact table `order_lines` (one row per order item).
''')
code(r'''
# ---- orders: keep ALL orders, add derived columns (transformations only: nothing runs yet)
purchase_date = F.to_date("order_purchase_timestamp")
deliv_date = F.to_date("order_delivered_customer_date")
est_date = F.to_date("order_estimated_delivery_date")

orders = (orders_raw
    .withColumn("purchase_ts", F.col("order_purchase_timestamp"))
    .withColumn("purchase_month", F.date_format("purchase_ts", "yyyy-MM"))
    .withColumn("purchase_dow", F.date_format("purchase_ts", "E"))                 # Mon..Sun
    .withColumn("purchase_dow_num", ((F.dayofweek("purchase_ts") + 5) % 7) + 1)    # 1=Mon..7=Sun
    .withColumn("purchase_hour", F.hour("purchase_ts"))
    .withColumn("delivery_days", F.datediff(deliv_date, purchase_date))
    .withColumn("estimated_days", F.datediff(est_date, purchase_date))
    .withColumn("delay_days", F.datediff(deliv_date, est_date))                    # >0 = late
    .withColumn("is_late", F.when(F.col("delay_days").isNull(), F.lit(None).cast("boolean"))
                            .otherwise(F.col("delay_days") > 0))
    .withColumn("is_delivered", F.col("order_status") == "delivered"))

# ---- items / customers / sellers: drop duplicate keys, normalise text
items = (items_raw.dropDuplicates(["order_id", "order_item_id"])
         .join(orders_raw.select("order_id"), "order_id", "left_semi"))             # drop orphans
customers = (cust_raw.dropDuplicates(["customer_id"])
    .withColumn("customer_city", F.lower(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state"))))
sellers = (sell_raw.dropDuplicates(["seller_id"])
    .withColumn("seller_city", F.lower(F.trim("seller_city")))
    .withColumn("seller_state", F.upper(F.trim("seller_state"))))

# ---- products + English category (left join keeps every product)
products = (prod_raw.dropDuplicates(["product_id"])
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .join(tr_raw, "product_category_name", "left")
    .withColumn("category_en", F.coalesce(F.col("product_category_name_english"),
                                          F.col("product_category_name"), F.lit("unknown")))
    .drop("product_category_name_english"))

# ---- reviews: ONE row per order = the latest answer (Window + row_number)
w = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last(),
                                           F.col("review_id"))
reviews = (rev_raw.filter(F.col("review_score").between(1, 5))
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
           .drop("rn", "review_comment_title"))

# ---- geolocation: drop points outside Brazil, then ONE row per zip prefix
LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX = -34.0, 6.0, -74.0, -34.0
in_brazil = (F.col("geolocation_lat").between(LAT_MIN, LAT_MAX) &
             F.col("geolocation_lng").between(LNG_MIN, LNG_MAX))
geo_ok = geo_raw.filter(in_brazil).withColumn("geolocation_city", F.lower(F.trim("geolocation_city")))
wz = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
        F.desc("n"), F.col("geolocation_state"), F.col("geolocation_city"))
top_place = (geo_ok.groupBy("geolocation_zip_code_prefix", "geolocation_state", "geolocation_city")
             .agg(F.count("*").alias("n")).withColumn("rn", F.row_number().over(wz))
             .filter("rn = 1").select("geolocation_zip_code_prefix",
                                      F.col("geolocation_state").alias("state"),
                                      F.col("geolocation_city").alias("city")))
geo_by_zip = (geo_ok.groupBy("geolocation_zip_code_prefix")
              .agg(F.avg("geolocation_lat").alias("lat"), F.avg("geolocation_lng").alias("lng"),
                   F.count("*").alias("n_points"))
              .join(top_place, "geolocation_zip_code_prefix")
              .withColumnRenamed("geolocation_zip_code_prefix", "zip_prefix"))

# ---- order_lines: LEFT JOINs from order_items onto tables unique on their key -> no fan-out
o_cols = orders.select("order_id", "customer_id", "order_status", "purchase_ts", "purchase_month",
                       "purchase_dow", "purchase_dow_num", "purchase_hour", "delivery_days",
                       "estimated_days", "delay_days", "is_late", "is_delivered")
c_cols = customers.select("customer_id", "customer_unique_id", "customer_state", "customer_city",
                          F.col("customer_zip_code_prefix").alias("customer_zip"))
s_cols = sellers.select("seller_id", "seller_state", "seller_city",
                        F.col("seller_zip_code_prefix").alias("seller_zip"))
p_cols = products.select("product_id", "category_en")
r_cols = reviews.select("order_id", "review_score")

order_lines = (items.join(o_cols, "order_id", "left")
               .join(c_cols, "customer_id", "left")
               .join(s_cols, "seller_id", "left")
               .join(p_cols, "product_id", "left")
               .join(r_cols, "order_id", "left")
               .select("order_id", "order_item_id", "product_id", "seller_id", "customer_id",
                       "customer_unique_id", "order_status", "purchase_ts", "purchase_month",
                       "purchase_dow", "purchase_dow_num", "purchase_hour", "delivery_days",
                       "estimated_days", "delay_days", "is_late", "is_delivered",
                       "customer_state", "customer_city", "customer_zip",
                       "seller_state", "seller_city", "seller_zip",
                       "category_en", "price", "freight_value", "review_score"))
print("order_lines defined. Columns:", len(order_lines.columns))
''')
md(r'''
### Lazy evaluation and the Catalyst plan
Everything above only **builds a plan**. In Spark, **transformations** (`select`, `filter`, `join`, `groupBy`, `withColumn`) are lazy:
they add steps to a DAG (directed acyclic graph) and no data is read. An **action** (`count`, `show`, `collect`, `write`, `toPandas`)
triggers real execution. Before running, Spark's **Catalyst optimiser** rewrites the logical plan (pushes filters down, prunes columns,
picks join strategies such as broadcast joins for small tables) into a physical plan. `explain()` prints that plan **without running a job**.
''')
code(r'''
# explain() BEFORE any action on order_lines: only a plan is printed, no job is run
order_lines.explain()
''')
code(r'''
order_lines = order_lines.cache()      # keep in memory: the six questions reuse this table
t = time.time()
n_lines = order_lines.count()          # ACTION: now the whole DAG actually runs (and fills the cache)
n_items = items.count()
print(f"order_items = {n_items:,} rows, order_lines = {n_lines:,} rows  ({time.time()-t:.1f}s)")
assert n_items == n_lines, "join fan-out detected!"
print("No join fan-out: every join was on a unique key.")
''')
md(r'''
### Parquet round-trip (columnar format, schema preserved)
CSV is row-oriented text with no types. **Parquet** is a compressed *columnar* binary format: smaller on disk, a query reads only the
columns it needs, and the schema travels with the file. We write one table to a scratch folder and read it back.
''')
code(r'''
import shutil, os
TMP = ROOT / "data" / "notebook_tmp"
shutil.rmtree(TMP, ignore_errors=True); TMP.mkdir(parents=True, exist_ok=True)
pq_path = str(TMP / "geo_by_zip.parquet")

geo_by_zip.write.mode("overwrite").parquet(pq_path)            # ACTION
back = spark.read.parquet(pq_path)
back.printSchema()                                              # types come back without us declaring them

def du(p):
    p = Path(p)
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.is_dir() else p.stat().st_size
print(f"rows written = {geo_by_zip.count():,}, rows read back = {back.count():,}")
print(f"schema identical: {geo_by_zip.schema == back.schema}")
print(f"raw geolocation CSV = {du(common.raw_csv('olist_geolocation_dataset'))/1e6:.1f} MB "
      f"(1,000,163 rows)  ->  per-zip Parquet = {du(pq_path)/1e6:.2f} MB ({back.count():,} rows)")
''')
md(r'''
### Data-quality summary
Numbers below are computed live from the DataFrames (the full report is in `outputs/data_quality.md`).
''')
code(r'''
late_row = orders.agg(F.sum(F.col("is_late").cast("int")).alias("late"),
                      F.count("delay_days").alias("with_delivery")).first()
dq = [
 ("orders", "rows", orders_raw.count(), "all kept (canceled/unavailable are real demand)"),
 ("orders", "no delivery date", orders_raw.filter(F.col("order_delivered_customer_date").isNull()).count(), "left null, never guessed"),
 ("orders", "late deliveries", f"{late_row['late']:,} of {late_row['with_delivery']:,} ({100*late_row['late']/late_row['with_delivery']:.2f}%)", "late = delivered after estimated day"),
 ("orders", "orders without items", orders_raw.join(items_raw, "order_id", "left_anti").count(), "excluded from revenue by the join"),
 ("order_items", "duplicate keys", items_raw.count() - items.count(), "none"),
 ("customers", "unique people behind customer_id", cust_raw.select("customer_unique_id").distinct().count(), "customer_id is per order"),
 ("products", "no category name", prod_raw.filter(F.col("product_category_name").isNull()).count(), "category = 'unknown'"),
 ("reviews", "raw rows -> one per order", f"{rev_raw.count():,} -> {reviews.count():,}", "kept latest review per order (Window)"),
 ("geolocation", "raw points -> zip rows", f"{geo_raw.count():,} -> {geo_by_zip.count():,}", "dropped 42 points outside Brazil, avg lat/lng per zip"),
 ("order_lines", "null review_score", order_lines.filter(F.col("review_score").isNull()).count(), "order never reviewed"),
]
display(pd.DataFrame(dq, columns=["table", "check", "value", "treatment"]).style.hide(axis="index"))
''')

# ---------------------------------------------------------------- 4 SQL
md(r'''
## 4. Spark SQL
A DataFrame can be registered as a **temporary view** and queried with plain SQL. SQL and the DataFrame API compile to the same
Catalyst plan. **Valid orders** everywhere below means status is not `canceled` / `unavailable`; **GMV** = sum of item price (freight excluded).
''')
code(r'''
valid = order_lines.filter(~F.col("order_status").isin("canceled", "unavailable"))
order_lines.createOrReplaceTempView("order_lines")
valid.createOrReplaceTempView("valid_lines")
orders.createOrReplaceTempView("orders")

# SQL 1: monthly orders and GMV (used for Q1)
sql_monthly = spark.sql("""
    SELECT purchase_month,
           COUNT(DISTINCT order_id) AS orders,
           ROUND(SUM(price), 2)     AS gmv
    FROM valid_lines
    GROUP BY purchase_month
    ORDER BY purchase_month
""")
sql_monthly.show(30)

# SQL 2: order status mix (all orders)
spark.sql("""
    SELECT order_status, COUNT(*) AS orders,
           ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM orders GROUP BY order_status ORDER BY orders DESC
""").show()

# SQL 3: top 10 categories by GMV (cross-checked against the DataFrame version in Q6)
sql_top_cat = spark.sql("""
    SELECT category_en, ROUND(SUM(price), 2) AS gmv
    FROM valid_lines GROUP BY category_en ORDER BY gmv DESC LIMIT 10
""")
sql_top_cat.show(truncate=False)
''')

# ---------------------------------------------------------------- 5 Q1
md(r'''
## 5. Q1 — How did orders and revenue grow month by month?  *(Advanced visualization: time series)*
**Why it matters for an eBay-like marketplace:** growth rate and seasonal spikes drive capacity planning (servers, support, logistics partners)
and seller onboarding. Spark steps: `groupBy` month, `lag()` over a window for month-over-month (MoM) growth. Months with fewer than 100 orders
(Sep 2016, Dec 2016, Sep 2018, Oct 2018: the extract is incomplete there) are excluded from the analysis window Jan 2017 - Aug 2018.
''')
code(r'''
THIN_MONTHS = ["2016-09", "2016-12", "2018-09", "2018-10"]
WIN_START, WIN_END = "2017-01", "2018-08"
FIG = common.FIGURES / "notebook"; FIG.mkdir(parents=True, exist_ok=True)   # same file names, separate folder
SOURCE = "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)"

# Transformations: groupBy month -> aggregate -> lag() window for MoM growth
monthly = (valid.groupBy("purchase_month")
           .agg(F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("price"), 2).alias("gmv"))
           .withColumn("aov", F.round(F.col("gmv") / F.col("orders"), 2)))
wm = Window.orderBy("purchase_month")
monthly = (monthly
           .withColumn("orders_prev", F.lag("orders").over(wm))
           .withColumn("gmv_prev", F.lag("gmv").over(wm))
           .withColumn("orders_mom_pct", F.round((F.col("orders") / F.col("orders_prev") - 1) * 100, 1))
           .withColumn("gmv_mom_pct", F.round((F.col("gmv") / F.col("gmv_prev") - 1) * 100, 1))
           .orderBy("purchase_month")).cache()

# Cross-check with the Spark SQL result from section 4
sql_rows = [(r.purchase_month, r.orders, round(r.gmv, 2)) for r in sql_monthly.collect()]
df_rows = [(r.purchase_month, r.orders, round(r.gmv, 2)) for r in monthly.select("purchase_month", "orders", "gmv").collect()]
assert sql_rows == df_rows, "Spark SQL and DataFrame results differ"
print("Spark SQL monthly totals equal DataFrame totals: OK\n")

# Restrict to the analysis window; blank Jan-2017 growth (its lag points at thin Dec 2016)
win = monthly.filter((F.col("purchase_month") >= WIN_START) & (F.col("purchase_month") <= WIN_END))
win.drop("orders_prev", "gmv_prev").show(20, truncate=False)
pdf = win.toPandas()                         # tiny: 20 rows
pdf.loc[pdf.index[0], ["orders_mom_pct", "gmv_mom_pct"]] = float("nan")
peak = pdf.loc[pdf["orders"].idxmax()]
peak_gmv = pdf.loc[pdf["gmv"].idxmax()]
first, last = pdf.iloc[0], pdf.iloc[-1]

# Daily orders for the busiest day
daily = (valid.withColumn("d", F.to_date("purchase_ts"))
         .filter((F.col("purchase_month") >= WIN_START) & (F.col("purchase_month") <= WIN_END))
         .groupBy("d").agg(F.countDistinct("order_id").alias("orders")).orderBy("d"))
dpdf = daily.toPandas(); dpeak = dpdf.loc[dpdf["orders"].idxmax()]
''')
code(r'''
import matplotlib.pyplot as plt, matplotlib.dates as mdates
common.set_style()
months = pd.to_datetime(pdf["purchase_month"] + "-01")
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), sharex=True, facecolor="white")
ax1.plot(months, pdf["orders"], color=common.PRIMARY, lw=2.5, marker="o")
ax1.set_ylabel("Orders per month (count)"); ax1.set_ylim(0, pdf["orders"].max() * 1.2)
ax2.plot(months, pdf["gmv"] / 1e3, color=common.ACCENT, lw=2.5, marker="o")
ax2.set_ylabel("Revenue / GMV per month (BRL thousand)"); ax2.set_ylim(0, pdf["gmv"].max() / 1e3 * 1.2)
pm = pd.to_datetime(peak.purchase_month + "-01")
ax1.scatter([pm], [peak.orders], s=140, color=common.ALERT, zorder=5)
ax1.annotate(f"Peak {pm:%b %Y}\n{int(peak.orders):,} orders", (pm, peak.orders), xytext=(-110, -5),
             textcoords="offset points", ha="right", va="center", color=common.ALERT, fontweight="bold",
             arrowprops=dict(arrowstyle="->", color=common.ALERT))
gm = pd.to_datetime(peak_gmv.purchase_month + "-01")
ax2.scatter([gm], [peak_gmv.gmv / 1e3], s=140, color=common.ALERT, zorder=5)
ax2.annotate(f"Peak {gm:%b %Y}\nBRL {peak_gmv.gmv/1e3:,.0f}k", (gm, peak_gmv.gmv / 1e3), xytext=(-110, -5),
             textcoords="offset points", ha="right", va="center", color=common.ALERT, fontweight="bold",
             arrowprops=dict(arrowstyle="->", color=common.ALERT))
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y")); ax2.set_xlabel("Purchase month")
fig.suptitle(f"Orders rose {last.orders/first.orders:.1f}x from Jan 2017 to Aug 2018, peaked in "
             f"{pm:%b %Y} (Black Friday), then plateaued", fontsize=14, fontweight="bold", y=0.98)
fig.text(0.01, -0.01, "Valid orders only (excl. canceled/unavailable); revenue = item price excl. freight. "
         "Window Jan 2017-Aug 2018: months with <100 orders\n(Sep 2016, Dec 2016, Sep 2018, Oct 2018; extract ends there) excluded.   "
         + SOURCE, fontsize=8.5, color=common.GREY, ha="left", va="top")
fig.tight_layout(rect=(0, 0, 1, 0.96))
fig.savefig(FIG / "q1_monthly_trend.png", facecolor="white")
plt.show()
''')
code(r'''
def ytd(year):
    m = pdf[(pdf["purchase_month"] >= f"{year}-01") & (pdf["purchase_month"] <= f"{year}-08")]
    return int(m["orders"].sum()), float(m["gmv"].sum())
o17, g17 = ytd(2017); o18, g18 = ytd(2018)
nov = pdf[pdf.purchase_month == "2017-11"].iloc[0]; dec = pdf[pdf.purchase_month == "2017-12"].iloc[0]
q1 = dict(peak_month=peak.purchase_month, peak_orders=int(peak.orders), peak_gmv=float(peak.gmv),
          nov_mom=float(nov.orders_mom_pct), dec_mom=float(dec.orders_mom_pct),
          peak_day=str(dpeak.d), peak_day_orders=int(dpeak.orders),
          growth=float(last.orders / first.orders), yoy_orders=(o18/o17-1)*100, yoy_gmv=(g18/g17-1)*100)
display(Markdown(f"""
**Result (Q1)**
- Peak month by orders and by GMV: **{q1['peak_month']}**, {q1['peak_orders']:,} orders and BRL {q1['peak_gmv']:,.2f} GMV.
- November 2017 grew {q1['nov_mom']:+.1f}% on October in orders, then December fell {q1['dec_mom']:+.1f}%. The busiest single day was {q1['peak_day']} ({q1['peak_day_orders']:,} orders).
- Orders grew {q1['growth']:.2f}x from Jan 2017 ({int(first.orders):,}) to Aug 2018 ({int(last.orders):,}); Jan-Aug 2018 vs Jan-Aug 2017: orders {q1['yoy_orders']:+.1f}%, GMV {q1['yoy_gmv']:+.1f}%.
- Growth flattened after Jan 2018 (about 6,100-7,200 orders a month).
"""))
''')

# ---------------------------------------------------------------- 6 Q2
md(r'''
## 6. Q2 — Which weekdays and hours do buyers purchase most?  *(Advanced visualization: heatmap)*
**Why it matters:** the busiest slots tell a marketplace when to schedule maintenance, run promotions and staff support.
Spark steps: one row per order (`distinct`), `groupBy` weekday and hour, **`pivot`** the 24 hours into columns to get a 7 x 24 matrix.
All months are used (we look for patterns, not trends).
''')
code(r'''
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
# One row per order (an order has several item lines), valid orders only
ord_time = valid.select("order_id", "purchase_dow_num", "purchase_hour").distinct().cache()
total_orders_q2 = ord_time.count()

counts = ord_time.groupBy("purchase_dow_num", "purchase_hour").agg(F.count("*").alias("orders"))
matrix = (counts.groupBy("purchase_dow_num")
          .pivot("purchase_hour", list(range(24)))        # listing the values avoids an extra pass
          .sum("orders").na.fill(0).orderBy("purchase_dow_num"))
matrix.show(7, truncate=False)

m = matrix.toPandas().set_index("purchase_dow_num")       # only now: 7 x 24 to pandas
m.columns = [int(c) for c in m.columns]; m.index = DAYS
assert int(m.values.sum()) == total_orders_q2
day_tot, hour_tot = m.sum(axis=1), m.sum(axis=0)
pk_day, pk_hour = m.stack().idxmax()[0], int(m.stack().idxmax()[1]); pk_val = int(m.values.max())
''')
code(r'''
import seaborn as sns
from matplotlib.patches import Rectangle
fig, ax = plt.subplots(figsize=(13, 6.2), facecolor="white")
sns.heatmap(m, cmap="Blues", ax=ax, linewidths=0.6, linecolor="white",
            cbar_kws={"label": "Orders (count, Sep 2016-Oct 2018)"})
ax.set_xticklabels([f"{h:02d}" for h in range(24)], rotation=0); ax.set_yticklabels(DAYS, rotation=0)
ax.set_xlabel("Hour of day purchased (00-23, Brasilia time)"); ax.set_ylabel("Weekday"); ax.grid(False)
r, c = DAYS.index(pk_day), pk_hour
ax.add_patch(Rectangle((c, r), 1, 1, fill=False, edgecolor=common.ALERT, lw=3))
ax.annotate(f"Peak: {pk_day} {pk_hour:02d}:00\n{pk_val} orders", (c, r + 0.5), xytext=(2.4, r + 0.5),
            textcoords="data", color=common.ALERT, fontweight="bold", ha="left", va="center",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=common.ALERT),
            arrowprops=dict(arrowstyle="->", color=common.ALERT))
ax.set_title(f"Buyers order on weekdays, mostly 10:00-22:00; busiest slot is {pk_day} {pk_hour:02d}:00", loc="left")
fig.text(0.01, -0.02, "Valid orders only (excl. canceled/unavailable), one count per order, all months in the dataset.   " + SOURCE,
         fontsize=8.5, color=common.GREY, ha="left")
fig.tight_layout(); fig.savefig(FIG / "q2_weekday_hour_heatmap.png", facecolor="white")
plt.show()
''')
code(r'''
band = max(range(0, 22), key=lambda h: hour_tot.loc[h:h + 2].sum())
wk_share = day_tot[["Sat", "Sun"]].sum() / total_orders_q2 * 100
display(Markdown(f"""
**Result (Q2)**, over {total_orders_q2:,} valid orders:
- Busiest weekday **{day_tot.idxmax()}** ({day_tot.max()/total_orders_q2*100:.1f}% of orders); quietest **{day_tot.idxmin()}** ({day_tot.min()/total_orders_q2*100:.1f}%). Weekends together take {wk_share:.1f}%.
- Busiest hour **{int(hour_tot.idxmax()):02d}:00** ({int(hour_tot.max()):,} orders); busiest 3-hour band {band:02d}:00-{band+3:02d}:00 ({hour_tot.loc[band:band+2].sum()/total_orders_q2*100:.1f}%).
- Single busiest cell: **{pk_day} {pk_hour:02d}:00** with {pk_val:,} orders. Night (00:00-05:59) is only {hour_tot.loc[0:5].sum()/total_orders_q2*100:.1f}% of orders; 10:00-21:59 holds {hour_tot.loc[10:21].sum()/total_orders_q2*100:.1f}%.
"""))
''')

# ---------------------------------------------------------------- 7 Q3
md(r'''
## 7. Q3 — Where are buyers and revenue concentrated?  *(Advanced visualization: geospatial map)*
**Why it matters:** demand concentration decides where to place warehouses, partner carriers and marketing spend.
Spark steps: aggregate valid orders per customer zip prefix and per state, then **join** to `geo_by_zip` (built from 1M raw geolocation rows)
for coordinates. A left join keeps zips without coordinates so we can count what the map misses.
''')
code(r'''
# Aggregate per customer zip prefix: distinct orders and GMV
zip_agg = (valid.groupBy("customer_zip")
           .agg(F.countDistinct("order_id").alias("orders"), F.round(F.sum("price"), 2).alias("gmv")))
zip_geo = (zip_agg.join(geo_by_zip.select("zip_prefix", "lat", "lng"),
                        zip_agg.customer_zip == geo_by_zip.zip_prefix, "left")).cache()
total_orders = valid.select("order_id").distinct().count()
unmapped_orders = zip_geo.filter(F.col("lat").isNull()).agg(F.sum("orders")).first()[0] or 0
unmapped_zips = zip_geo.filter(F.col("lat").isNull()).count()
print(f"Orders that could not be geolocated: {unmapped_orders} of {total_orders:,} ({unmapped_zips} zip prefixes)")

# Aggregate per state; share of national orders and GMV; sellers per state
national_gmv = valid.agg(F.sum("price")).first()[0]
state_agg = (valid.groupBy("customer_state")
             .agg(F.countDistinct("order_id").alias("orders"), F.round(F.sum("price"), 2).alias("gmv"),
                  F.countDistinct("customer_unique_id").alias("customers"))
             .withColumn("gmv_share_pct", F.round(F.col("gmv") / national_gmv * 100, 2))
             .withColumn("orders_share_pct", F.round(F.col("orders") / total_orders * 100, 2))
             .withColumn("gmv_per_customer", F.round(F.col("gmv") / F.col("customers"), 2)))
sellers_state = sellers.groupBy("seller_state").agg(F.count("*").alias("sellers"))
state_agg = (state_agg.join(sellers_state, state_agg.customer_state == sellers_state.seller_state, "left")
             .drop("seller_state").fillna(0, subset=["sellers"]).orderBy(F.desc("gmv")).cache())
print("=== Top 10 states by GMV ===")
state_agg.show(10, truncate=False)

# Top cities
city_agg = (valid.groupBy("customer_city", "customer_state")
            .agg(F.countDistinct("order_id").alias("orders"), F.round(F.sum("price"), 2).alias("gmv"))
            .orderBy(F.desc("orders")))
print("=== Top 10 cities by orders ===")
city_agg.show(10, truncate=False)
n_sellers = sellers.count(); sellers_sp = sellers.filter(F.col("seller_state") == "SP").count()

states_pd = state_agg.toPandas().sort_values("gmv", ascending=False)
cities_pd = city_agg.limit(10).toPandas()
points = zip_geo.filter(F.col("lat").isNotNull()).toPandas()      # ~15k rows
print("Mapped zip prefixes:", len(points))
''')
md(r'''
**Interactive map.** To keep the notebook small we do the aggregation in Spark: coordinates are **rounded to 0.1 degree (about 11 km)** and
orders/GMV are summed per grid cell, so the map embeds only a few hundred circles instead of ~15,000 zip prefixes.
Circle area is proportional to GMV; hover for details.
''')
code(r'''
import folium
# Spark: round coordinates to a 0.1-degree grid and sum per cell
grid = (zip_geo.filter(F.col("lat").isNotNull())
        .withColumn("glat", F.round("lat", 1)).withColumn("glng", F.round("lng", 1))
        .groupBy("glat", "glng").agg(F.sum("orders").alias("orders"), F.round(F.sum("gmv"), 0).alias("gmv"))
        .filter(F.col("orders") >= 5))
gpd = grid.toPandas()
print(f"{len(gpd):,} grid cells on the interactive map")

fmap = folium.Map(location=[-14.5, -51.0], zoom_start=4, tiles="OpenStreetMap")
for r in gpd.itertuples():
    folium.CircleMarker(location=[r.glat, r.glng], radius=min(3 + (r.gmv ** 0.5) / 40, 22),
                        color=common.PRIMARY, weight=0.5, fill=True, fill_color=common.ALERT, fill_opacity=0.5,
                        tooltip=f"{int(r.orders):,} orders, BRL {r.gmv:,.0f} GMV").add_to(fmap)
fmap.get_root().html.add_child(folium.Element(
    "<div style='position:fixed;bottom:8px;left:8px;z-index:9999;background:white;padding:4px 8px;font:12px Arial'>"
    "Buyer locations on a 0.1-degree grid (circle size = GMV, BRL). " + SOURCE + "</div>"))
fmap
''')
code(r'''
import numpy as np
from matplotlib.colors import LogNorm
sp_row = states_pd[states_pd.customer_state == "SP"].iloc[0]
fig, ax = plt.subplots(figsize=(9, 9.5), facecolor="white"); ax.set_facecolor("white")
p = points.sort_values("orders")
sc = ax.scatter(p.lng, p.lat, c=p.orders, s=8 + 6 * np.log1p(p.orders), cmap="YlOrRd",
                norm=LogNorm(vmin=1, vmax=p.orders.max()), alpha=0.75, edgecolor="none")
cb = fig.colorbar(sc, ax=ax, shrink=0.55, pad=0.03); cb.set_label("Orders per zip prefix (log scale)")
ax.set_aspect(1 / np.cos(np.radians(15)))
ax.set_xlabel("Longitude (degrees)"); ax.set_ylabel("Latitude (degrees)")
ax.set_title(f"Buyers cluster on the south-east coast: SP alone is {sp_row.orders_share_pct:.0f}% of orders", loc="left")
label_pos = {"sao paulo": (-70, -25), "rio de janeiro": (-47, -31), "belo horizonte": (-62, -14),
             "brasilia": (-70, -6), "curitiba": (-70, -30), "campinas": (-70, -20)}
zip_city = valid.select("customer_zip", "customer_city").distinct().toPandas()
for c in cities_pd.head(6).itertuples():
    sub = points.merge(zip_city, on="customer_zip"); sub = sub[sub.customer_city == c.customer_city]
    x, y = np.average(sub.lng, weights=sub.orders), np.average(sub.lat, weights=sub.orders)
    ax.annotate(f"{c.customer_city.title()} ({c.orders:,})", (x, y), xytext=label_pos.get(c.customer_city, (-70, -10)),
                fontsize=9, fontweight="bold", color=common.PRIMARY,
                arrowprops=dict(arrowstyle="-", color=common.GREY, lw=0.8),
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=common.GREY, lw=0.5))
ax.set_xlim(-76, -30)
ax.annotate(f"{SOURCE}. Orders excl. canceled/unavailable; {unmapped_orders:,} orders "
            f"({unmapped_orders / total_orders * 100:.1f}%) have no coordinates.",
            xy=(0, 0), xycoords="axes fraction", xytext=(0, -42), textcoords="offset points", fontsize=8, color=common.GREY)
fig.savefig(FIG / "q3_buyer_map.png"); plt.show()
''')
code(r'''
top3 = states_pd.head(3); top3_o = states_pd.sort_values("orders", ascending=False).head(3)
display(Markdown(f"""
**Result (Q3)**, over {total_orders:,} valid orders and BRL {national_gmv:,.2f} GMV:
- **SP** has {int(sp_row.orders):,} orders = **{sp_row.orders_share_pct:.2f}%** of orders and {sp_row.gmv_share_pct:.2f}% of GMV.
- Top 3 states by GMV: {', '.join(top3.customer_state)} = {top3.gmv.sum()/national_gmv*100:.2f}% of GMV; top 3 by orders: {', '.join(top3_o.customer_state)} = {top3_o.orders.sum()/total_orders*100:.2f}% of orders.
- Top city: {cities_pd.customer_city[0].title()} ({int(cities_pd.orders[0]):,} orders, {cities_pd.orders[0]/total_orders*100:.2f}% of national orders).
- Sellers: {sellers_sp:,} of {n_sellers:,} ({sellers_sp/n_sellers*100:.2f}%) are in SP.
- Map coverage: {unmapped_orders:,} orders ({unmapped_orders/total_orders*100:.2f}%) in {unmapped_zips} zip prefixes could not be geolocated.
"""))
''')

# ---------------------------------------------------------------- 8 Q4
md(r'''
## 8. Q4 — Do late deliveries lower review scores?
**Why it matters:** reviews are a marketplace's trust currency. If late delivery destroys ratings, logistics quality is a growth lever.
Spark steps: `datediff` (done in section 3) gives `delay_days` = delivered date minus estimated date (positive = late). We **join** delivered orders
to reviews, compare on-time vs late, bucket the delay, and compute the correlation. Metrics are at **order level** (not item level).
''')
code(r'''
# Delivered orders with a known outcome, plus buyer state
q4_orders = orders.filter((F.col("order_status") == "delivered") & F.col("is_late").isNotNull())
cust_state = customers.select("customer_id", "customer_state")
delivered = q4_orders.join(cust_state, "customer_id").cache()
n_delivered = delivered.count()
# Inner join to reviews (one row per order): orders without a review say nothing about satisfaction
scored = delivered.join(reviews.select("order_id", "review_score"), "order_id").cache()
n_scored = scored.count()
print(f"Delivered orders with a delivery outcome: {n_delivered:,}; with a review score: {n_scored:,}")

by_late = (scored.groupBy("is_late")
           .agg(F.count("*").alias("orders"), F.round(F.avg("review_score"), 3).alias("avg_score"),
                F.round(F.avg((F.col("review_score") == 1).cast("int")) * 100, 2).alias("pct_1star")))
print("On-time (false) vs late (true):"); by_late.show()
late_rows = {r["is_late"]: r for r in by_late.collect()}

# Delay buckets
d = F.col("delay_days")
bucket = (F.when(d < -7, "Early >7d").when(d < 0, "Early 1-7d").when(d == 0, "On time (0)")
          .when(d <= 3, "Late 1-3d").when(d <= 7, "Late 4-7d").when(d <= 14, "Late 8-14d").otherwise("Late >14d"))
order_of = {"Early >7d": 1, "Early 1-7d": 2, "On time (0)": 3, "Late 1-3d": 4, "Late 4-7d": 5, "Late 8-14d": 6, "Late >14d": 7}
bpd = (scored.withColumn("bucket", bucket).groupBy("bucket")
       .agg(F.count("*").alias("orders"), F.round(F.avg("review_score"), 3).alias("avg_score"),
            F.round(F.avg((F.col("review_score") == 1).cast("int")) * 100, 2).alias("pct_1star")).toPandas())
bpd["ord"] = bpd.bucket.map(order_of); bpd = bpd.sort_values("ord").reset_index(drop=True)
print(bpd.drop(columns="ord").to_string(index=False))

corr = scored.stat.corr("delay_days", "review_score")        # Pearson correlation computed by Spark
print("\nPearson correlation delay_days vs review_score:", round(corr, 4))

# Late rate per state (>= 500 delivered orders so rates are stable)
nat_late_rate = delivered.agg(F.avg(F.col("is_late").cast("int"))).first()[0] * 100
states = (delivered.groupBy("customer_state")
          .agg(F.count("*").alias("delivered_orders"),
               F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("late_rate_pct"),
               F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"))
          .filter(F.col("delivered_orders") >= 500).orderBy(F.desc("late_rate_pct")))
states.show(20)
spd = states.toPandas()
''')
code(r'''
on, late = late_rows[False], late_rows[True]
fig, ax = plt.subplots(figsize=(10, 5.8), facecolor="white")
colors = [common.ALERT if b.startswith("Late") else common.PRIMARY for b in bpd.bucket]
bars = ax.bar(bpd.bucket, bpd.avg_score, color=colors)
for b, (s, n) in zip(bars, zip(bpd.avg_score, bpd.orders)):
    ax.text(b.get_x() + b.get_width() / 2, s + 0.04, f"{s:.2f}", ha="center", fontweight="bold")
    ax.text(b.get_x() + b.get_width() / 2, 0.1, f"n={n:,}", ha="center", color="white", fontsize=8)
ax.set_ylim(0, 5); ax.set_ylabel("Average review score (1-5 stars)"); ax.set_xlabel("Delivery vs estimated date (days)")
ax.set_title(f"Late deliveries drag reviews down: {on['avg_score']:.2f} stars on time vs {late['avg_score']:.2f} when late",
             loc="left", fontsize=13)
ax.tick_params(axis="x", labelsize=9)
ax.annotate(f"1-star reviews: {on['pct_1star']:.1f}% on time\nvs {late['pct_1star']:.1f}% late",
            xy=(5, bpd.avg_score[bpd.ord == 5].iloc[0]), xytext=(2.3, 4.55), fontsize=10, color=common.ALERT, ha="center",
            arrowprops=dict(arrowstyle="->", color=common.ALERT))
fig.text(0.01, -0.02, f"{SOURCE}. Delivered orders with a review.", fontsize=8, color=common.GREY)
fig.savefig(FIG / "q4_late_vs_score.png"); plt.show()
''')
code(r'''
sp4 = spd.sort_values("late_rate_pct")
fig, ax = plt.subplots(figsize=(9, 8), facecolor="white")
cols = [common.ALERT if v > nat_late_rate else common.PRIMARY for v in sp4.late_rate_pct]
ax.barh(sp4.customer_state, sp4.late_rate_pct, color=cols)
for y, v in enumerate(sp4.late_rate_pct): ax.text(v + 0.2, y, f"{v:.1f}%", va="center", fontsize=8)
ax.axvline(nat_late_rate, color=common.GREY, ls="--")
ax.text(nat_late_rate + 0.2, -0.9, f"National average {nat_late_rate:.1f}%", color=common.GREY, fontsize=9)
ax.set_xlabel("Share of delivered orders arriving after the estimated date (%)"); ax.set_ylabel("Customer state")
worst = spd.iloc[0]
ax.set_title(f"Late deliveries are concentrated: {worst.customer_state} runs {worst.late_rate_pct:.1f}% late "
             f"vs {nat_late_rate:.1f}% nationally", loc="left", fontsize=12)
ax.set_ylim(-1.3, len(sp4) - 0.4)
fig.text(0.01, -0.01, f"{SOURCE}. States with >= 500 delivered orders.", fontsize=8, color=common.GREY)
fig.savefig(FIG / "q4_late_rate_by_state.png"); plt.show()
''')
code(r'''
display(Markdown(f"""
**Result (Q4)**, {n_scored:,} delivered orders with a review:
- Average review score: **{on['avg_score']:.2f}** for {int(on['orders']):,} on-time orders vs **{late['avg_score']:.2f}** for {int(late['orders']):,} late orders (gap {on['avg_score']-late['avg_score']:.2f} stars).
- 1-star reviews: {on['pct_1star']:.2f}% on time vs {late['pct_1star']:.2f}% late. Correlation of delay days with score: {corr:.3f}.
- National late rate {nat_late_rate:.2f}%; worst state **{worst.customer_state}** at {worst.late_rate_pct:.2f}% (avg {worst.avg_delivery_days:.1f} days to deliver), best **{spd.iloc[-1].customer_state}** at {spd.iloc[-1].late_rate_pct:.2f}%.
- This is an association, not proof of causation (a late parcel may coincide with other problems).
"""))
''')

# ---------------------------------------------------------------- 9 Q5
md(r'''
## 9. Q5 — Which seller states ship to which buyer states?  *(Advanced visualization: network graph)*
**Why it matters:** the seller-state to buyer-state flow graph shows where the marketplace depends on one hub and where long, costly
shipping lanes exist. Spark steps: `order_lines` already joins orders, items, sellers and customers (a 4-table join); we `groupBy`
(seller_state, customer_state) and aggregate. States become **nodes**, flows become **directed edges** (drawn with `networkx`).
Note: an order with sellers in two states is counted once in each flow.
''')
code(r'''
flows = (valid.groupBy("seller_state", "customer_state")
         .agg(F.countDistinct("order_id").alias("orders"), F.round(F.sum("price"), 2).alias("gmv"),
              F.round(F.avg("freight_value"), 2).alias("avg_freight"),
              F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"))
         .withColumn("intra", F.col("seller_state") == F.col("customer_state")).cache())
flows.orderBy(F.desc("orders")).show(10)

split = flows.groupBy("intra").agg(F.sum("orders").alias("orders")).toPandas().set_index("intra")
tot_orders = int(split["orders"].sum()); intra_orders = int(split.loc[True, "orders"]); inter_orders = int(split.loc[False, "orders"])

# Average freight per ITEM, same-state vs cross-state
fr = (valid.groupBy((F.col("seller_state") == F.col("customer_state")).alias("same"))
      .agg(F.avg("freight_value").alias("f")).toPandas().set_index("same")["f"])

# Out-degree = number of buyer states a seller state serves
out = (flows.groupBy("seller_state").agg(F.countDistinct("customer_state").alias("out_degree"),
                                         F.sum("orders").alias("out_orders"))
       .withColumn("out_share_pct", F.round(100 * F.col("out_orders") / tot_orders, 2)).orderBy(F.desc("out_orders")))
out.show(8)

# Node positions = mean lat/lng of each state's zip prefixes
cent = (geo_by_zip.groupBy("state").agg(F.avg("lat").alias("lat"), F.avg("lng").alias("lng"))
        .toPandas().set_index("state"))
flows_pd = flows.toPandas()
top10 = flows_pd.sort_values("orders", ascending=False).head(10)
''')
code(r'''
import networkx as nx
THRESH = 500                                     # draw only inter-state flows with >= 500 orders
inter = flows_pd[~flows_pd["intra"]]; shown = inter[inter["orders"] >= THRESH]
as_seller = flows_pd.groupby("seller_state")["orders"].sum()
as_buyer = flows_pd.groupby("customer_state")["orders"].sum()
size = as_seller.add(as_buyer, fill_value=0)

G = nx.DiGraph()
for s in cent.index:
    if s in size.index: G.add_node(s, pos=(cent.loc[s, "lng"], cent.loc[s, "lat"]))
for r in shown.itertuples(): G.add_edge(r.seller_state, r.customer_state, w=r.orders)
pos = nx.get_node_attributes(G, "pos")
NS = [40 + 6 * size[n] ** 0.5 for n in G]

fig, ax = plt.subplots(figsize=(11, 10), facecolor="white"); ax.set_facecolor("white")
nx.draw_networkx_nodes(G, pos, ax=ax, node_size=NS, node_color=[common.ALERT if n == "SP" else common.ACCENT for n in G],
                       alpha=0.7, edgecolors="white")
nx.draw_networkx_edges(G, pos, ax=ax, width=[0.4 + G[u][v]["w"] / 900 for u, v in G.edges], edge_color=common.PRIMARY,
                       alpha=0.4, arrows=True, arrowsize=12, connectionstyle="arc3,rad=0.12", node_size=NS)
nx.draw_networkx_labels(G, pos, ax=ax, font_size=9, font_weight="bold", labels={n: n for n in G}, font_color="black",
                        bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.7))
ax.set_xlabel("Longitude (degrees)"); ax.set_ylabel("Latitude (degrees)")
ax.grid(alpha=0.2); ax.tick_params(labelbottom=True, labelleft=True)
ax.set_title(f"SP is the hub: {100*intra_orders/tot_orders:.0f}% of orders stay in-state, and most cross-state flows start in SP", loc="left")
fig.text(0.01, -0.01, f"Arrows go seller state -> buyer state; width ~ orders; node size ~ orders as seller + buyer. "
         f"Only inter-state flows with >= {THRESH} orders drawn ({len(shown)} edges).\n"
         "Nodes placed at mean zip-prefix coordinates per state. " + SOURCE, fontsize=8.5, color="dimgray")
fig.savefig(FIG / "q5_state_flow_network.png"); plt.show()
''')
code(r'''
sp_out = shown[shown.seller_state == "SP"]["orders"].sum() / shown["orders"].sum()
display(Markdown(f"""
**Result (Q5)**, {tot_orders:,} flow-orders (an order counts once per seller-state/buyer-state pair):
- **{100*intra_orders/tot_orders:.2f}%** of flow-orders are intra-state ({intra_orders:,}); {100*inter_orders/tot_orders:.2f}% cross a state border ({inter_orders:,}).
- Average freight per item: BRL {fr[True]:.2f} within a state vs BRL {fr[False]:.2f} across states.
- The 10 biggest flows carry {100*top10.orders.sum()/tot_orders:.1f}% of flow-orders; the biggest is {top10.iloc[0].seller_state} -> {top10.iloc[0].customer_state} ({int(top10.iloc[0].orders):,} orders).
- Of the {len(shown)} drawn edges (>= {THRESH} orders), {100*sp_out:.2f}% of orders leave from SP.
"""))
''')

# ---------------------------------------------------------------- 10 Q6
md(r'''
## 10. Q6 — Which product categories earn the most?
**Why it matters:** category concentration shows where seller supply and buyer demand are strongest, and which categories to promote or diversify.
Spark steps: category names were translated by a join in section 3; here `groupBy` category and a **`rank()` window** order them by GMV.
A second window partitioned by state finds each big state's leading category.
''')
code(r'''
cat = (valid.groupBy("category_en")
       .agg(F.round(F.sum("price"), 2).alias("gmv"), F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"), F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("review_score"), 2).alias("avg_review")).cache())
wr = Window.orderBy(F.desc("gmv"))                       # global rank window (~74 rows)
top10 = cat.withColumn("rank", F.rank().over(wr)).filter("rank <= 10").orderBy("rank")
top10.show(truncate=False)
top10_pd = top10.toPandas()
total_gmv = cat.agg(F.sum("gmv")).first()[0]
top10_share = 100 * top10_pd["gmv"].sum() / total_gmv
top10_pd["share_pct"] = 100 * top10_pd["gmv"] / total_gmv

# cross-check with the Spark SQL query of section 4
assert [r.category_en for r in sql_top_cat.collect()] == list(top10_pd.category_en), "SQL and DataFrame top-10 differ"
print("Spark SQL top-10 equals DataFrame top-10: OK")

# Partitioned window: best category WITHIN each of the 5 biggest buyer states
state_orders = valid.groupBy("customer_state").agg(F.countDistinct("order_id").alias("orders"))
big5 = [r.customer_state for r in state_orders.orderBy(F.desc("orders")).limit(5).collect()]
sc = (valid.filter(F.col("customer_state").isin(big5)).groupBy("customer_state", "category_en")
      .agg(F.round(F.sum("price"), 2).alias("gmv")))
ws = Window.partitionBy("customer_state").orderBy(F.desc("gmv"))
top_per_state = sc.withColumn("rank", F.rank().over(ws)).filter("rank = 1").orderBy(F.desc("gmv"))
top_per_state.show(truncate=False)
tps = top_per_state.toPandas()
''')
code(r'''
dd = top10_pd.iloc[::-1]
fig, ax = plt.subplots(figsize=(10, 6.2), facecolor="white")
bars = ax.barh(dd["category_en"], dd["gmv"] / 1e6, color=[common.PRIMARY if i == 9 else common.ACCENT for i in range(10)])
for b, s in zip(bars, dd["share_pct"]):
    ax.text(b.get_width() + 0.03, b.get_y() + b.get_height() / 2, f"{b.get_width():.2f}M ({s:.1f}%)", va="center", fontsize=10)
ax.set_xlim(0, dd["gmv"].max() / 1e6 * 1.22)
ax.set_xlabel("Revenue / GMV (million BRL, excl. freight)"); ax.set_ylabel("Product category")
ax.set_title(f"Top 10 categories earn {top10_share:.0f}% of GMV; {top10_pd.category_en[0].replace('_',' ')} leads", loc="left")
fig.text(0.01, -0.02, "Labels: GMV and share of total GMV of valid orders. " + SOURCE, fontsize=8.5, color="dimgray")
fig.savefig(FIG / "q6_top_categories.png"); plt.show()
''')
code(r'''
display(Markdown(f"""
**Result (Q6)**, {cat.count()} categories, total GMV BRL {total_gmv:,.2f}:
- Top 10 categories earn **{top10_share:.2f}%** of GMV.
- #1 **{top10_pd.category_en[0]}** (BRL {top10_pd.gmv[0]:,.2f}, {top10_pd.share_pct[0]:.2f}%), #2 {top10_pd.category_en[1]} ({top10_pd.share_pct[1]:.2f}%), #3 {top10_pd.category_en[2]} ({top10_pd.share_pct[2]:.2f}%).
- Leading category per big state: {', '.join(f'{r.customer_state}: {r.category_en}' for r in tps.itertuples())}.
"""))
''')

# ---------------------------------------------------------------- 11 RDD
md(r'''
## 11. RDD vs DataFrame
The same question (orders per buyer state) answered two ways. The **DataFrame API** is declarative and optimised by Catalyst.
The **RDD API** (Resilient Distributed Dataset) is the low-level core: we write `map` -> `reduceByKey` -> sort ourselves and Spark
runs it as written (no optimiser, Python lambdas).
''')
code(r'''
from operator import add
df = orders.join(customers, "customer_id").cache()      # one row per order + buyer state (all statuses)
df.count()                                              # action: fill the cache so timings are fair

t = time.time()
df_res = df.groupBy("customer_state").count().orderBy(F.desc("count"), "customer_state").take(5)
t_df = time.time() - t

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
print("DataFrame:", [tuple(r) for r in df_res]); print("RDD      :", rdd_res)
print(f"Same answer. Timing (informational): DataFrame {t_df:.2f}s, RDD {t_rdd:.2f}s")

# Classic word-count style RDD example: items per category
cats = order_lines.select("category_en").rdd.map(lambda r: r[0])
print("Top categories by item count (RDD):", cats.map(lambda c: (c, 1)).reduceByKey(add).sortBy(lambda kv: -kv[1]).take(5))
''')
code(r'''
# Partitions = units of parallelism; repartition() shuffles data into a new number of partitions
n_before = df.rdd.getNumPartitions()
n_after = df.repartition(4).rdd.getNumPartitions()
print(f"partitions before = {n_before}, after repartition(4) = {n_after}")
''')
md(r'''
### Transformations vs actions

| Kind | Examples used in this notebook | Behaviour |
|---|---|---|
| **Narrow transformation** | `select`, `filter`, `withColumn`, `map` | lazy; each output partition depends on one input partition, no shuffle |
| **Wide transformation** | `groupBy`/`agg`, `join`, `pivot`, window functions, `reduceByKey`, `repartition` | lazy; needs a **shuffle** (data moves between partitions) |
| **Action** | `count`, `show`, `take`, `collect`, `toPandas`, `write.parquet` | triggers a job that executes the whole DAG |
| **Caching** | `cache()` | lazy marker; the data is stored after the first action that computes it |
''')

# ---------------------------------------------------------------- 12 INSIGHTS (from outputs/insights.md)
md("## 12. Insights and Recommendations")
md((Path(__file__).resolve().parents[2] / "outputs" / "insights.md").read_text())

# ---------------------------------------------------------------- 13 CONCEPTS
md(r'''
## 13. Spark concepts used

| Concept | Where in this notebook |
|---|---|
| SparkSession, local mode, parallelism | Section 1 |
| Explicit schema (`StructType`) on CSV read, `multiLine` | Section 2 |
| Actions (`count`, `show`, `take`, `collect`, `toPandas`) | Sections 2-11 |
| `filter`, `select`, `withColumn`, `dropDuplicates`, `coalesce` | Section 3 |
| Joins (left, left_semi, inner) and the no-fan-out check | Section 3, Q4, Q5 |
| Window functions: `row_number` (dedup), `lag` (MoM growth), `rank` (top categories) | Section 3, Q1, Q6 |
| Lazy evaluation, DAG, Catalyst plan (`explain()`) | Section 3 |
| `cache()` / persistence | Sections 2, 3, Q2-Q6 |
| Parquet write/read (columnar, schema preserved) | Section 3 |
| Spark SQL and temporary views | Section 4, Q1, Q6 |
| `groupBy` / `agg` / `countDistinct` | Q1-Q6 |
| `pivot` | Q2 |
| `datediff`, `date_format`, `hour`, `dayofweek` | Section 3, Q1, Q2, Q4 |
| Statistics (`stat.corr`) | Q4 |
| Spark-side reduction before plotting (grid aggregation for the map) | Q3 |
| RDD API (`map`, `reduceByKey`, `sortBy`), partitions, `repartition` | Section 11 |
| Visualization: time series, heatmap, geospatial map, network graph, bar charts | Q1, Q2, Q3, Q5, Q4/Q6 |
''')

# ---------------------------------------------------------------- 14 LIMITS
md(r'''
## 14. Limitations and References

### Limitations
- **Stand-in data:** Olist is not eBay; patterns (Brazil, 2016-2018, one platform's logistics model) may not transfer to eBay or to today.
- **Short period:** the data covers Sep 2016 - Oct 2018; thin months (Sep 2016, Dec 2016, Sep 2018, Oct 2018, fewer than 100 orders each) are excluded from the Q1 trend, so seasonality over several years cannot be assessed.
- **Correlation is not causation:** late delivery goes with low scores (Q4), but we did not control for product type, price or seller.
- **Q5 double counting:** an order with sellers in several states counts once in each seller-state/buyer-state flow.
- **Geography:** coordinates are zip-prefix averages, not exact addresses; a small share of orders (about 0.3%) has no coordinates.
- **Scale:** Spark ran in local mode on one laptop; the code is cluster-ready but we did not test cluster performance.

### References
- Olist, *Brazilian E-Commerce Public Dataset by Olist*, Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Apache Spark documentation: https://spark.apache.org/docs/latest/
''')

# ---------------------------------------------------------------- 15 STOP
code(r'''
print(f"Notebook compute time so far: {(time.time()-NB_T0)/60:.1f} min")
shutil.rmtree(TMP, ignore_errors=True)       # remove the scratch Parquet folder
spark.stop()
''')

nb = nbf.v4.new_notebook()
nb.cells = cells
nb.metadata["kernelspec"] = {"name": "fa2-olist", "display_name": "fa2-olist", "language": "python"}
nbf.write(nb, OUT)
print("wrote", OUT, "cells:", len(cells))
# Execute:  jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=fa2-olist \
#           --ExecutePreprocessor.timeout=900 fa2/notebooks/fa2_olist_spark.ipynb
