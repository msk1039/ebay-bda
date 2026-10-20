"""Q6: Which product categories earn the most? (groupBy + rank window + partitioned window)"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = common.get_spark("q6_categories")
lines = spark.read.parquet(common.processed("order_lines"))
# Valid orders only; GMV = sum(price), freight excluded
valid = lines.filter(~F.col("order_status").isin("canceled", "unavailable")).cache()

# Per-category KPIs
cat = (valid.groupBy("category_en")
       .agg(F.round(F.sum("price"), 2).alias("gmv"),
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("review_score"), 2).alias("avg_review"))
       .cache())

# Global rank window (no partition -> one ordered list; fine for ~70 rows)
w = Window.orderBy(F.desc("gmv"))
ranked = cat.withColumn("rank", F.rank().over(w))
top10 = ranked.filter("rank <= 10").orderBy("rank")
top10.show(truncate=False)

total_gmv = cat.agg(F.sum("gmv")).first()[0]
top10_pd = top10.toPandas()
top10_share = 100 * top10_pd["gmv"].sum() / total_gmv
top10_pd["share_pct"] = 100 * top10_pd["gmv"] / total_gmv
print(f"Total GMV {total_gmv:,.2f} BRL; top-10 share {top10_share:.2f}%")

# Partitioned window: rank categories WITHIN each customer_state
state_orders = valid.groupBy("customer_state").agg(F.countDistinct("order_id").alias("orders"))
big5 = [r.customer_state for r in state_orders.orderBy(F.desc("orders")).limit(5).collect()]
sc = (valid.filter(F.col("customer_state").isin(big5))
      .groupBy("customer_state", "category_en").agg(F.round(F.sum("price"), 2).alias("gmv")))
ws = Window.partitionBy("customer_state").orderBy(F.desc("gmv"))
top_per_state = (sc.withColumn("rank", F.rank().over(ws)).filter("rank = 1")
                 .orderBy(F.desc("gmv")))
top_per_state.show(truncate=False)
tps = top_per_state.toPandas()

# ---- Chart ----
import matplotlib.pyplot as plt
common.set_style()
d = top10_pd.iloc[::-1]   # reverse so rank 1 is at the top
fig, ax = plt.subplots(figsize=(10, 6.2), facecolor="white")
colors = [common.PRIMARY if i == 9 else common.ACCENT for i in range(10)]
bars = ax.barh(d["category_en"], d["gmv"] / 1e6, color=colors)
for b, s in zip(bars, d["share_pct"]):
    ax.text(b.get_width() + 0.03, b.get_y() + b.get_height() / 2, f"{b.get_width():.2f}M ({s:.1f}%)",
            va="center", fontsize=10)
ax.set_xlim(0, d["gmv"].max() / 1e6 * 1.22)
ax.set_xlabel("Revenue / GMV (million BRL, excl. freight)")
ax.set_ylabel("Product category")
ax.set_title(f"Top 10 categories earn {top10_share:.0f}% of GMV; "
             f"{top10_pd.category_en[0].replace('_',' ')} leads", loc="left")
fig.text(0.01, -0.02, "Labels: GMV and share of total GMV of valid orders. "
         "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)", fontsize=8.5, color="dimgray")
fig.savefig(common.FIGURES / "q6_top_categories.png"); plt.close(fig)

common.save_metrics("q6_categories", {
    "total_gmv_brl": float(total_gmv), "n_categories": int(cat.count()),
    "top10_share_of_gmv_pct": round(top10_share, 2),
    "top10": [dict(rank=int(r.rank), category=r.category_en, gmv=float(r.gmv), orders=int(r.orders),
                   items=int(r.items), avg_price=float(r.avg_price),
                   avg_review=None if r.avg_review != r.avg_review else float(r.avg_review),
                   share_pct=round(float(r.share_pct), 2)) for r in top10_pd.itertuples()],
    "top_category_per_state_top5": [dict(state=r.customer_state, category=r.category_en, gmv=float(r.gmv))
                                    for r in tps.itertuples()],
})
spark.stop()
