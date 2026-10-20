"""Q4 - Do late deliveries lower review scores?  (run from fa2/: python analysis/q4_delivery_reviews.py)

Spark steps: orders (delivered, is_late known) JOIN reviews -> on-time vs late stats ->
delay buckets -> per-state late rate. Delivery metrics are at ORDER level (orders table).
delay_days = delivered date - estimated date (positive = late, negative = early).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
from pyspark.sql import functions as F

SOURCE = "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)"
common.FIGURES.mkdir(exist_ok=True)
spark = common.get_spark("q4-delivery")

# ---- 1. Delivered orders with a known delivery outcome ---------------------------------
orders = (spark.read.parquet(common.processed("orders"))
          .filter((F.col("order_status") == "delivered") & F.col("is_late").isNotNull()))
customers = spark.read.parquet(common.processed("customers")).select("customer_id", "customer_state")
reviews = spark.read.parquet(common.processed("reviews")).select("order_id", "review_score")

# Inner join: orders without a review cannot say anything about satisfaction
# (reviews has one row per order, so no fan-out)
delivered = orders.join(customers, "customer_id").cache()
n_delivered = delivered.count()
scored = delivered.join(reviews, "order_id").cache()
n_scored = scored.count()
print(f"Delivered orders with delivery outcome: {n_delivered}; with a review score: {n_scored}")

# ---- 2. On-time vs late ----------------------------------------------------------------
by_late = (scored.groupBy("is_late")
           .agg(F.count("*").alias("orders"),
                F.round(F.avg("review_score"), 3).alias("avg_score"),
                F.round(F.avg((F.col("review_score") == 1).cast("int")) * 100, 2).alias("pct_1star")))
print("=== On-time (False) vs late (True) ===")
by_late.show()
late_rows = {r["is_late"]: r for r in by_late.collect()}

# ---- 3. Delay buckets ------------------------------------------------------------------
d = F.col("delay_days")
bucket = (F.when(d < -7, "Early >7d").when(d < 0, "Early 1-7d").when(d == 0, "On time (0)")
          .when(d <= 3, "Late 1-3d").when(d <= 7, "Late 4-7d").when(d <= 14, "Late 8-14d")
          .otherwise("Late >14d"))
order_of = {"Early >7d": 1, "Early 1-7d": 2, "On time (0)": 3, "Late 1-3d": 4,
            "Late 4-7d": 5, "Late 8-14d": 6, "Late >14d": 7}
buckets = (scored.withColumn("bucket", bucket).groupBy("bucket")
           .agg(F.count("*").alias("orders"),
                F.round(F.avg("review_score"), 3).alias("avg_score"),
                F.round(F.avg((F.col("review_score") == 1).cast("int")) * 100, 2).alias("pct_1star")))
bpd = buckets.toPandas()
bpd["ord"] = bpd.bucket.map(order_of)
bpd = bpd.sort_values("ord").reset_index(drop=True)
print("=== Avg score by delay bucket ===")
print(bpd.drop(columns="ord").to_string(index=False))

# ---- 4. Correlation delay_days vs review_score (Spark stat) ----------------------------
corr = scored.stat.corr("delay_days", "review_score")
print("Pearson correlation delay_days vs review_score:", corr)

# ---- 5. Late rate and delivery days per state (>= 500 delivered orders) -----------------
nat_late_rate = delivered.agg(F.avg(F.col("is_late").cast("int"))).first()[0] * 100
states = (delivered.groupBy("customer_state")
          .agg(F.count("*").alias("delivered_orders"),
               F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("late_rate_pct"),
               F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"))
          .filter(F.col("delivered_orders") >= 500).orderBy(F.desc("late_rate_pct")))
print("=== Late rate by state (>=500 delivered orders) ===")
states.show(30)
spd = states.toPandas()

# ---- 6. Charts -------------------------------------------------------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

common.set_style()
on, late = late_rows[False], late_rows[True]

# 6a. average score by delay bucket
fig, ax = plt.subplots(figsize=(10, 5.8), facecolor="white")
colors = [common.ALERT if b.startswith("Late") else common.PRIMARY for b in bpd.bucket]
bars = ax.bar(bpd.bucket, bpd.avg_score, color=colors)
for b, (s, n) in zip(bars, zip(bpd.avg_score, bpd.orders)):
    ax.text(b.get_x() + b.get_width() / 2, s + 0.04, f"{s:.2f}", ha="center", fontweight="bold")
    ax.text(b.get_x() + b.get_width() / 2, 0.1, f"n={n:,}", ha="center", color="white", fontsize=8)
ax.set_ylim(0, 5)
ax.set_ylabel("Average review score (1-5 stars)")
ax.set_xlabel("Delivery vs estimated date (days)")
ax.set_title(f"Late deliveries drag reviews down: {on['avg_score']:.2f} stars on time vs "
             f"{late['avg_score']:.2f} when late", loc="left", fontsize=13)
ax.tick_params(axis="x", labelsize=9)
ax.annotate(f"1-star reviews: {on['pct_1star']:.1f}% on time\nvs {late['pct_1star']:.1f}% late",
            xy=(5, bpd.avg_score[bpd.ord == 5].iloc[0]), xytext=(2.3, 4.55),
            fontsize=10, color=common.ALERT, ha="center",
            arrowprops=dict(arrowstyle="->", color=common.ALERT))
fig.text(0.01, -0.02, f"{SOURCE}. Delivered orders with a review.", fontsize=8, color=common.GREY)
fig.savefig(common.FIGURES / "q4_late_vs_score.png")
plt.close(fig)

# 6b. late rate by state (horizontal bars, worst at top)
sp = spd.sort_values("late_rate_pct")                 # barh draws first row at the bottom
fig, ax = plt.subplots(figsize=(9, 8), facecolor="white")
cols = [common.ALERT if v > nat_late_rate else common.PRIMARY for v in sp.late_rate_pct]
ax.barh(sp.customer_state, sp.late_rate_pct, color=cols)
for y, v in enumerate(sp.late_rate_pct):
    ax.text(v + 0.2, y, f"{v:.1f}%", va="center", fontsize=8)
ax.axvline(nat_late_rate, color=common.GREY, ls="--")
ax.text(nat_late_rate + 0.2, -0.9, f"National average {nat_late_rate:.1f}%", color=common.GREY, fontsize=9)
ax.set_xlabel("Share of delivered orders arriving after the estimated date (%)")
ax.set_ylabel("Customer state")
worst = spd.iloc[0]
ax.set_title(f"Late deliveries are concentrated: {worst.customer_state} runs {worst.late_rate_pct:.1f}% late "
             f"vs {nat_late_rate:.1f}% nationally", loc="left", fontsize=12)
ax.set_ylim(-1.3, len(sp) - 0.4)
fig.text(0.01, -0.01, f"{SOURCE}. States with >= 500 delivered orders.", fontsize=8, color=common.GREY)
fig.savefig(common.FIGURES / "q4_late_rate_by_state.png")
plt.close(fig)

# ---- 7. Metrics ------------------------------------------------------------------------
metrics = {
    "delivered_orders": int(n_delivered), "delivered_orders_with_review": int(n_scored),
    "on_time_orders": int(on["orders"]), "late_orders": int(late["orders"]),
    "avg_score_on_time": float(on["avg_score"]), "avg_score_late": float(late["avg_score"]),
    "score_gap": round(float(on["avg_score"] - late["avg_score"]), 3),
    "pct_1star_on_time": float(on["pct_1star"]), "pct_1star_late": float(late["pct_1star"]),
    "national_late_rate_pct": round(float(nat_late_rate), 2),
    "corr_delay_days_vs_score": round(float(corr), 4),
    "delay_buckets": bpd.drop(columns="ord").to_dict("records"),
    "n_states_ge_500": int(len(spd)),
    "worst_state_late_rate": {"state": worst.customer_state, "late_rate_pct": float(worst.late_rate_pct),
                              "delivered_orders": int(worst.delivered_orders),
                              "avg_delivery_days": float(worst.avg_delivery_days)},
    "best_state_late_rate": {"state": spd.iloc[-1].customer_state,
                             "late_rate_pct": float(spd.iloc[-1].late_rate_pct),
                             "avg_delivery_days": float(spd.iloc[-1].avg_delivery_days)},
    "states_late_rate": spd.to_dict("records"),
}
import time
for attempt in range(5):                     # shared metrics.json: retry on decode errors
    try:
        common.save_metrics("q4_delivery_reviews", metrics)
        break
    except ValueError:
        time.sleep(1)
print("Metrics saved.")
spark.stop()
