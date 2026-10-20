"""Phase 1: profile + clean the Olist CSVs and write Parquet tables.

Steps:  1) read every CSV with an explicit schema
        2) profile (row counts, nulls, duplicate keys, outliers)
        3) clean + derive columns, write Parquet to data/processed/
        4) save numbers to outputs/metrics.json and a report to outputs/data_quality.md
Run:    python analysis/p1_prepare.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType)

T0 = time.time()
spark = common.get_spark("fa2-p1-prepare")
common.PROCESSED.mkdir(parents=True, exist_ok=True)
OUT_MD = common.ROOT / "outputs" / "data_quality.md"


def S(name, typ=StringType()):
    """Shorthand for a nullable StructField."""
    return StructField(name, typ, True)


I, D, TS = IntegerType(), DoubleType(), TimestampType()

# ---------------------------------------------------------------------------
# 1. EXPLICIT SCHEMAS (no inferSchema: faster, and zip prefixes stay STRING
#    so "01037" keeps its leading zero)
# ---------------------------------------------------------------------------
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
    """Read one raw CSV. multiLine + escape='"' handle review comments that span lines
    and contain doubled quotes."""
    return (spark.read.option("header", True).option("multiLine", True)
            .option("quote", '"').option("escape", '"').option("mode", "PERMISSIVE")
            .schema(SCHEMAS[name]).csv(common.raw_csv(name)))


raw = {k: read(k).cache() for k in SCHEMAS}   # cache: each table is reused many times

# ---------------------------------------------------------------------------
# 2. PROFILING helpers
# ---------------------------------------------------------------------------
PROFILE = {}   # name -> {"rows", "nulls", "issues", "decision", ...}; goes to metrics + md


def null_counts(df):
    """Null count for every column in one pass over the data."""
    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
    return {c: int(row[c] or 0) for c in df.columns}


def key_stats(df, key_cols):
    """(distinct keys, number of key values that appear more than once, surplus rows)."""
    total = df.count()
    grouped = df.groupBy(*key_cols).count()
    distinct = grouped.count()
    dup_keys = grouped.filter(F.col("count") > 1).count()
    return distinct, dup_keys, total - distinct


def profile(name, df, key_cols=None):
    p = {"rows": df.count(), "nulls": null_counts(df), "issues": [], "decision": ""}
    if key_cols:
        d, dk, extra = key_stats(df, key_cols)
        p.update(key=",".join(key_cols), distinct_keys=d, duplicate_keys=dk, surplus_rows=extra)
        if dk:
            p["issues"].append(f"{dk:,} duplicate key values ({extra:,} surplus rows) on {key_cols}")
    PROFILE[name] = p
    return p


p_orders = profile("orders", raw["olist_orders_dataset"], ["order_id"])
p_items = profile("order_items", raw["olist_order_items_dataset"], ["order_id", "order_item_id"])
p_cust = profile("customers", raw["olist_customers_dataset"], ["customer_id"])
p_sell = profile("sellers", raw["olist_sellers_dataset"], ["seller_id"])
p_prod = profile("products", raw["olist_products_dataset"], ["product_id"])
p_pay = profile("payments", raw["olist_order_payments_dataset"], ["order_id", "payment_sequential"])
p_rev = profile("reviews", raw["olist_order_reviews_dataset"], ["review_id"])
p_geo = profile("geolocation", raw["olist_geolocation_dataset"])
p_tr = profile("category_translation", raw["product_category_name_translation"],
               ["product_category_name"])

p_sell["issues"].append("no issues found: keys unique, no nulls")
p_tr["issues"].append("no issues found; 2 product categories are missing from it (see products)")
p_tr["decision"] = "Lookup only: left-joined onto products, not written separately."
p_cust.setdefault("decision", "")
# ---- table-specific profiling -------------------------------------------------
orders_raw = raw["olist_orders_dataset"]
status_dist = {r["order_status"]: r["count"] for r in
               orders_raw.groupBy("order_status").count().orderBy(F.desc("count")).collect()}
rng = orders_raw.agg(F.min("order_purchase_timestamp").alias("mn"),
                     F.max("order_purchase_timestamp").alias("mx")).first()
p_orders["status_distribution"] = status_dist
p_orders["purchase_min"], p_orders["purchase_max"] = str(rng["mn"]), str(rng["mx"])
# monthly counts reveal sparse months at both ends of the range
month_counts = {r["m"]: r["n"] for r in orders_raw.groupBy(
    F.date_format("order_purchase_timestamp", "yyyy-MM").alias("m")).agg(F.count("*").alias("n"))
    .orderBy("m").collect()}
p_orders["orders_per_month"] = month_counts
n_deliv_status = status_dist.get("delivered", 0)
deliv_no_date = orders_raw.filter((F.col("order_status") == "delivered") &
                                  F.col("order_delivered_customer_date").isNull()).count()
date_without_deliv = orders_raw.filter((F.col("order_status") != "delivered") &
                                       F.col("order_delivered_customer_date").isNotNull()).count()
p_orders["delivered_status_missing_delivery_date"] = deliv_no_date
p_orders["non_delivered_with_delivery_date"] = date_without_deliv
deliv_before_purchase = orders_raw.filter(F.col("order_delivered_customer_date") <
                                          F.col("order_purchase_timestamp")).count()
p_orders["delivered_before_purchase"] = deliv_before_purchase
p_orders["issues"] += [
    f"{deliv_no_date} orders with status 'delivered' have no delivery date",
    f"{date_without_deliv} orders have a delivery date but a status other than 'delivered'",
    f"{deliv_before_purchase} orders have delivery date earlier than purchase date",
    f"{status_dist.get('canceled', 0)} canceled and {status_dist.get('unavailable', 0)} unavailable orders",
]

items_raw = raw["olist_order_items_dataset"]
p_items["orphan_orders"] = items_raw.join(orders_raw, "order_id", "left_anti").count()
p_items["nonpositive_price"] = items_raw.filter(F.col("price") <= 0).count()
p_items["negative_freight"] = items_raw.filter(F.col("freight_value") < 0).count()
p_items["issues"] += [f"{p_items['orphan_orders']} items whose order_id is not in orders",
                      f"{p_items['nonpositive_price']} items with price <= 0",
                      f"{p_items['negative_freight']} items with negative freight"]
p_orders["orders_without_items"] = orders_raw.join(items_raw, "order_id", "left_anti").count()
p_orders["issues"].append(f"{p_orders['orders_without_items']} orders have no order_items row")

p_cust["unique_customers"] = raw["olist_customers_dataset"].select("customer_unique_id").distinct().count()
p_cust["states"] = raw["olist_customers_dataset"].select("customer_state").distinct().count()
p_cust["issues"].append(f"customer_id is per-order; {p_cust['unique_customers']:,} real people "
                        f"(customer_unique_id) behind {p_cust['rows']:,} customer_ids")

pay_raw = raw["olist_order_payments_dataset"]
p_pay["payment_types"] = {r["payment_type"]: r["count"] for r in
                          pay_raw.groupBy("payment_type").count().orderBy(F.desc("count")).collect()}
p_pay["zero_value"] = pay_raw.filter(F.col("payment_value") == 0).count()
p_pay["not_defined"] = pay_raw.filter(F.col("payment_type") == "not_defined").count()
p_pay["issues"] += [f"{p_pay['not_defined']} rows with payment_type 'not_defined'",
                    f"{p_pay['zero_value']} rows with payment_value = 0"]

prod_raw = raw["olist_products_dataset"]
tr_raw = raw["product_category_name_translation"]
untranslated = (prod_raw.filter(F.col("product_category_name").isNotNull())
                .join(tr_raw, "product_category_name", "left_anti")
                .select("product_category_name").distinct().collect())
p_prod["untranslated_categories"] = sorted(r[0] for r in untranslated)
p_prod["issues"] += [
    f"{p_prod['nulls']['product_category_name']} products have no category name",
    f"categories missing from the translation table: {p_prod['untranslated_categories']}",
    "source columns product_name_lenght / product_description_lenght are misspelled "
    "(renamed to *_length)"]

rev_raw = raw["olist_order_reviews_dataset"]
score_dist = {str(r["review_score"]): r["count"] for r in
              rev_raw.groupBy("review_score").count().orderBy("review_score").collect()}
p_rev["score_distribution"] = score_dist
p_rev["bad_scores"] = rev_raw.filter(F.col("review_score").isNull() |
                                     ~F.col("review_score").between(1, 5)).count()
p_rev["orders_with_multiple_reviews"] = (rev_raw.groupBy("order_id").count()
                                         .filter("count > 1").count())
p_rev["distinct_orders_reviewed"] = rev_raw.select("order_id").distinct().count()
p_rev["issues"] += [
    f"{p_rev['bad_scores']} rows with a missing/out-of-range score (0 means the multiline parse is sane)",
    f"{p_rev['orders_with_multiple_reviews']:,} orders have more than one review",
    f"comment title null in {p_rev['nulls']['review_comment_title']:,} rows, "
    f"comment message null in {p_rev['nulls']['review_comment_message']:,} rows"]

geo_raw = raw["olist_geolocation_dataset"]
LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX = -34.0, 6.0, -74.0, -34.0
in_brazil = (F.col("geolocation_lat").between(LAT_MIN, LAT_MAX) &
             F.col("geolocation_lng").between(LNG_MIN, LNG_MAX))
p_geo["outside_brazil"] = geo_raw.filter(~in_brazil | F.col("geolocation_lat").isNull()
                                         | F.col("geolocation_lng").isNull()).count()
p_geo["distinct_zip_prefixes"] = geo_raw.select("geolocation_zip_code_prefix").distinct().count()
p_geo["exact_duplicate_rows"] = p_geo["rows"] - geo_raw.distinct().count()
p_geo["issues"] += [
    f"{p_geo['outside_brazil']:,} points outside the Brazil box "
    f"(lat {LAT_MIN}..{LAT_MAX}, lng {LNG_MIN}..{LNG_MAX})",
    f"{p_geo['exact_duplicate_rows']:,} exact duplicate rows; "
    f"{p_geo['distinct_zip_prefixes']:,} zip prefixes share {p_geo['rows']:,} points"]

# ---------------------------------------------------------------------------
# 3. CLEANING + WRITING PARQUET
# ---------------------------------------------------------------------------
OUTPUT_COUNTS = {}


def write(df, name):
    """Overwrite a Parquet table and record its row count (read back from disk)."""
    df.write.mode("overwrite").parquet(common.processed(name))
    OUTPUT_COUNTS[name] = spark.read.parquet(common.processed(name)).count()


# ---- orders: keep ALL orders, add derived columns -------------------------------
o = orders_raw
purchase_date = F.to_date("order_purchase_timestamp")
deliv_date = F.to_date("order_delivered_customer_date")
est_date = F.to_date("order_estimated_delivery_date")
orders = (o.withColumn("purchase_ts", F.col("order_purchase_timestamp"))
          .withColumn("purchase_month", F.date_format("purchase_ts", "yyyy-MM"))
          .withColumn("purchase_dow", F.date_format("purchase_ts", "E"))  # Mon..Sun
          # Spark dayofweek: 1=Sun..7=Sat  ->  1=Mon..7=Sun
          .withColumn("purchase_dow_num", ((F.dayofweek("purchase_ts") + 5) % 7) + 1)
          .withColumn("purchase_hour", F.hour("purchase_ts"))
          .withColumn("delivery_days", F.datediff(deliv_date, purchase_date))
          .withColumn("estimated_days", F.datediff(est_date, purchase_date))
          .withColumn("delay_days", F.datediff(deliv_date, est_date))
          # late = delivered on a calendar day after the estimated day; null if not delivered
          .withColumn("is_late", F.when(F.col("delay_days").isNull(), F.lit(None).cast("boolean"))
                      .otherwise(F.col("delay_days") > 0))
          .withColumn("is_delivered", F.col("order_status") == "delivered"))
write(orders, "orders")
late_row = orders.agg(F.sum(F.col("is_late").cast("int")).alias("late"),
                      F.count("delay_days").alias("with_delivery")).first()
p_orders["late_orders"], p_orders["orders_with_delivery_date"] = int(late_row["late"]), int(late_row["with_delivery"])
p_orders["late_rate_pct"] = round(100 * late_row["late"] / late_row["with_delivery"], 2)
p_orders["decision"] = ("Kept all rows (canceled/unavailable orders are real behaviour for demand "
                        "analysis; analyses filter is_delivered when they need fulfilment). "
                        "Delivery fields stay null when missing instead of being guessed.")

# ---- order_items / customers / sellers / payments -------------------------------
order_ids = orders_raw.select("order_id")
items = (items_raw.dropDuplicates(["order_id", "order_item_id"])
         .join(order_ids, "order_id", "left_semi"))    # drop orphans (none expected)
write(items, "order_items")
p_items["decision"] = ("Removed duplicate (order_id, order_item_id) pairs and orphans if any; "
                       "kept all prices (none <= 0).")

customers = (raw["olist_customers_dataset"].dropDuplicates(["customer_id"])
             .withColumn("customer_city", F.lower(F.trim("customer_city")))
             .withColumn("customer_state", F.upper(F.trim("customer_state"))))
write(customers, "customers")
p_cust["decision"] = "Trimmed/lower-cased city, upper-cased state; zip kept as string; no rows dropped."

sellers = (raw["olist_sellers_dataset"].dropDuplicates(["seller_id"])
           .withColumn("seller_city", F.lower(F.trim("seller_city")))
           .withColumn("seller_state", F.upper(F.trim("seller_state"))))
write(sellers, "sellers")
p_sell["decision"] = "Same text normalisation as customers; no rows dropped."

payments = pay_raw.dropDuplicates(["order_id", "payment_sequential"])
write(payments, "payments")
p_pay["decision"] = ("Kept all rows incl. not_defined / zero-value (small, and they belong to "
                     "real orders); an order may have several payment rows, so sum per order.")

# ---- products + English category -----------------------------------------------
products = (prod_raw.dropDuplicates(["product_id"])
            .withColumnRenamed("product_name_lenght", "product_name_length")
            .withColumnRenamed("product_description_lenght", "product_description_length")
            .join(tr_raw, "product_category_name", "left")        # left join keeps all products
            # translation if available; Portuguese name if untranslated; "unknown" if no category
            .withColumn("category_en",
                        F.coalesce(F.col("product_category_name_english"),
                                   F.col("product_category_name"), F.lit("unknown")))
            .drop("product_category_name_english"))
write(products, "products")
p_prod["category_unknown"] = products.filter(F.col("category_en") == "unknown").count()
p_prod["distinct_categories"] = products.select("category_en").distinct().count()
p_prod["decision"] = ("Left-joined translation (no product lost). Missing category -> 'unknown'; "
                      "the untranslated Portuguese names keep their original name rather than "
                      "being hidden in 'unknown'. Typos in column names fixed.")

# ---- reviews: ONE row per order (latest answer wins) ----------------------------
w = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last(),
                                           F.col("review_id"))
reviews = (rev_raw.filter(F.col("review_score").between(1, 5))
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "review_comment_title"))
write(reviews, "reviews")
p_rev["decision"] = ("Kept the latest review per order (by review_answer_timestamp) so joins to "
                     "orders do not duplicate rows; dropped the mostly-empty title column, kept "
                     "the comment message.")

# ---- geolocation: filter outliers then ONE row per zip prefix --------------------
geo_ok = geo_raw.filter(in_brazil).withColumn("geolocation_city", F.lower(F.trim("geolocation_city")))
# most frequent (state, city) per zip via row_number over the counts
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
write(geo_by_zip, "geo_by_zip")
p_geo["rows_after_filter"] = geo_ok.count()
p_geo["zip_prefixes_after_filter"] = OUTPUT_COUNTS["geo_by_zip"]
cust_zip_missing = (customers.select("customer_zip_code_prefix").distinct()
                    .join(geo_by_zip, F.col("customer_zip_code_prefix") == F.col("zip_prefix"), "left_anti").count())
p_geo["customer_zips_without_geo"] = cust_zip_missing
p_geo["decision"] = (f"Dropped out-of-Brazil points, then averaged lat/lng per zip prefix "
                     f"({p_geo['rows_after_filter']:,} points left -> {OUTPUT_COUNTS['geo_by_zip']:,} zip rows; "
                     f"{p_geo['distinct_zip_prefixes'] - OUTPUT_COUNTS['geo_by_zip']} prefixes had only outlier points). "
                     f"{cust_zip_missing} distinct customer zip prefixes have no geo match.")

# ---- order_lines: denormalised fact table, one row per order item ---------------
# every join is a LEFT join from order_items onto a table that is unique on its join key,
# so the row count cannot grow (no fan-out).
o_cols = orders.select("order_id", "customer_id", "order_status", "purchase_ts", "purchase_month",
                       "purchase_dow", "purchase_dow_num", "purchase_hour", "delivery_days",
                       "estimated_days", "delay_days", "is_late", "is_delivered")
c_cols = customers.select("customer_id", "customer_unique_id",
                          F.col("customer_state").alias("customer_state"),
                          F.col("customer_city").alias("customer_city"),
                          F.col("customer_zip_code_prefix").alias("customer_zip"))
s_cols = sellers.select("seller_id", "seller_state", "seller_city",
                        F.col("seller_zip_code_prefix").alias("seller_zip"))
p_cols = products.select("product_id", F.col("category_en"))
r_cols = reviews.select("order_id", F.col("review_score"))

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

# 5. LAZY EVALUATION demo: nothing above has executed yet for order_lines.
print("\n=== explain() on order_lines BEFORE any action (lazy: only a plan, no job run) ===")
order_lines.explain()
print("=== now an action (write) triggers the actual computation ===\n")
write(order_lines, "order_lines")

n_items, n_lines = OUTPUT_COUNTS["order_items"], OUTPUT_COUNTS["order_lines"]
print(f"order_items rows = {n_items:,}; order_lines rows = {n_lines:,}; equal = {n_items == n_lines}")
assert n_items == n_lines, "join fan-out detected in order_lines"
ol_nulls = null_counts(spark.read.parquet(common.processed("order_lines")))
ol_issues = [f"{c}: {n:,} nulls" for c, n in ol_nulls.items() if n]
PROFILE["order_lines"] = {
    "rows": n_lines, "nulls": ol_nulls, "issues": ["no join fan-out: rows equal order_items"] + ol_issues,
    "decision": "Derived table: order_items LEFT JOIN orders, customers, sellers, products, "
                "reviews (each unique on its key). Null review_score = order never reviewed; "
                "null delivery fields = not delivered."}
PROFILE["order_lines"]["order_items_rows"] = n_items

# ---------------------------------------------------------------------------
# 4. SAVE METRICS + MARKDOWN REPORT
# ---------------------------------------------------------------------------
for k, v in OUTPUT_COUNTS.items():
    PROFILE.setdefault(k, {})
metrics = {"profile": PROFILE, "parquet_row_counts": OUTPUT_COUNTS,
           "geo_bounds": {"lat": [LAT_MIN, LAT_MAX], "lng": [LNG_MIN, LNG_MAX]}}
common.save_metrics("data_quality", metrics)

md = ["# Data quality report (Olist, Phase 1)", "",
      "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle). All numbers are computed by "
      "`analysis/p1_prepare.py` and stored in `outputs/metrics.json` (section `data_quality`).", ""]
summary_rows = [("orders", "orders"), ("order_items", "order_items"), ("customers", "customers"),
                ("sellers", "sellers"), ("products", "products"), ("payments", "payments"),
                ("reviews", "reviews"), ("geolocation", "geo_by_zip"),
                ("category_translation", None), ("order_lines", "order_lines")]
md += ["## Summary", "", "| Dataset | Raw rows | Parquet rows written |", "|---|---:|---:|"]
for name, out in summary_rows:
    raw_rows = PROFILE[name].get("rows", "-") if name != "order_lines" else "(derived)"
    outv = f"{OUTPUT_COUNTS[out]:,}" if out else "(not written; used for join)"
    md.append(f"| {name} | {raw_rows if isinstance(raw_rows, str) else format(raw_rows, ',')} | {outv} |")
md.append("")

for name, p in PROFILE.items():
    if "rows" not in p:
        continue
    md += [f"## {name}", "", f"- Rows (raw): **{p['rows']:,}**"]
    if "key" in p:
        md.append(f"- Key `{p['key']}`: {p['distinct_keys']:,} distinct, "
                  f"{p['duplicate_keys']:,} duplicated key values")
    nz = {c: n for c, n in p["nulls"].items() if n}
    md.append("- Nulls per column: " + (", ".join(f"`{c}`={n:,}" for c, n in nz.items()) if nz
                                         else "none"))
    for extra in ("status_distribution", "score_distribution", "payment_types"):
        if extra in p:
            md.append(f"- {extra.replace('_', ' ')}: " + ", ".join(f"{k}={v:,}" for k, v in p[extra].items()))
    if "purchase_min" in p:
        md.append(f"- Purchase date range: {p['purchase_min']} to {p['purchase_max']}")
        md.append(f"- Late deliveries: {p['late_orders']:,} of {p['orders_with_delivery_date']:,} "
                  f"orders with a delivery date ({p['late_rate_pct']}%)")
        sparse = {m: n for m, n in p["orders_per_month"].items() if n < 100}
        md.append(f"- Months with fewer than 100 orders (partial coverage): "
                  + (", ".join(f"{m}={n}" for m, n in sparse.items()) or "none"))
    md += ["", "**Issues found**", ""] + [f"- {i}" for i in p["issues"]] + ["", f"**Decision:** {p['decision']}", ""]

md += ["## Row-count check", "",
       f"`order_items` = {n_items:,} rows and `order_lines` = {n_lines:,} rows "
       f"({'equal, no join fan-out' if n_items == n_lines else 'MISMATCH'})."]
OUT_MD.write_text("\n".join(md) + "\n")

print("\nParquet row counts:", OUTPUT_COUNTS)
print(f"Done in {time.time() - T0:.1f}s")
spark.stop()
