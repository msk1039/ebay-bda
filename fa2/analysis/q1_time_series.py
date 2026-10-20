"""Q1: How did orders and revenue grow month by month?

Spark steps: filter valid orders -> groupBy month -> window lag() for MoM growth,
repeat the monthly totals with Spark SQL (and assert they match), then plot.
Run from fa2/:  python analysis/q1_time_series.py
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
from pyspark.sql import Window
from pyspark.sql import functions as F

THIN_MONTHS = ["2016-09", "2016-12", "2018-09", "2018-10"]   # < 100 orders each
WIN_START, WIN_END = "2017-01", "2018-08"                     # analysis window


def safe_save_metrics(section, values, tries=10):
    """save_metrics does read-modify-write on a shared file; retry if another agent is mid-write."""
    for _ in range(tries):
        try:
            common.save_metrics(section, values)
            return
        except json.JSONDecodeError:
            time.sleep(0.5)
    raise RuntimeError("could not write metrics.json")


spark = common.get_spark("q1_time_series")

# ---- 1. Load the denormalised fact table and keep valid orders only ----
lines = spark.read.parquet(common.processed("order_lines"))
valid = lines.filter(~F.col("order_status").isin("canceled", "unavailable"))

# ---- 2. DataFrame API: monthly orders, GMV (price only, no freight), average order value ----
monthly = (valid.groupBy("purchase_month")
           .agg(F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("price"), 2).alias("gmv"))
           .withColumn("aov", F.round(F.col("gmv") / F.col("orders"), 2)))

# ---- 3. Window function: previous month's value via lag(), then growth % ----
w = Window.orderBy("purchase_month")
monthly = (monthly
           .withColumn("orders_prev", F.lag("orders").over(w))
           .withColumn("gmv_prev", F.lag("gmv").over(w))
           .withColumn("orders_mom_pct", F.round((F.col("orders") / F.col("orders_prev") - 1) * 100, 1))
           .withColumn("gmv_mom_pct", F.round((F.col("gmv") / F.col("gmv_prev") - 1) * 100, 1))
           .orderBy("purchase_month"))
monthly.cache()
print("Monthly orders / GMV / AOV with MoM growth (all months; thin months not reliable):")
monthly.drop("orders_prev", "gmv_prev").show(30, truncate=False)

# ---- 4. Same monthly totals with Spark SQL, and check they agree ----
valid.createOrReplaceTempView("valid_lines")
sql_monthly = spark.sql("""
    SELECT purchase_month,
           COUNT(DISTINCT order_id)  AS orders,
           ROUND(SUM(price), 2)      AS gmv
    FROM valid_lines
    GROUP BY purchase_month
    ORDER BY purchase_month
""")
sql_rows = [(r.purchase_month, r.orders, round(r.gmv, 2)) for r in sql_monthly.collect()]
df_rows = [(r.purchase_month, r.orders, round(r.gmv, 2)) for r in monthly.select("purchase_month", "orders", "gmv").collect()]
assert sql_rows == df_rows, "Spark SQL and DataFrame results differ"
print("Spark SQL monthly totals equal DataFrame totals: OK")

# ---- 5. Restrict to the analysis window (drops thin months) ----
win = monthly.filter((F.col("purchase_month") >= WIN_START) & (F.col("purchase_month") <= WIN_END))
pdf = win.toPandas()                      # tiny: 20 rows
peak = pdf.loc[pdf["orders"].idxmax()]
peak_gmv = pdf.loc[pdf["gmv"].idxmax()]

# ---- 6. Daily orders (window months only) for the daily peak and 7-day average ----
daily = (valid.withColumn("d", F.to_date("purchase_ts"))
         .filter((F.col("purchase_month") >= WIN_START) & (F.col("purchase_month") <= WIN_END))
         .groupBy("d").agg(F.countDistinct("order_id").alias("orders")).orderBy("d"))
dpdf = daily.toPandas()
dpeak = dpdf.loc[dpdf["orders"].idxmax()]

# ---- 7. Year-over-year: Jan-Aug 2018 vs Jan-Aug 2017 ----
def ytd(year):
    m = pdf[(pdf["purchase_month"] >= f"{year}-01") & (pdf["purchase_month"] <= f"{year}-08")]
    return int(m["orders"].sum()), float(m["gmv"].sum())
o17, g17 = ytd(2017)
o18, g18 = ytd(2018)
yoy_orders = (o18 / o17 - 1) * 100
yoy_gmv = (g18 / g17 - 1) * 100

# Jan 2017's lag() points at thin Dec 2016, so its MoM growth is meaningless: blank it out
pdf.loc[pdf.index[0], ["orders_mom_pct", "gmv_mom_pct"]] = float("nan")

# Growth first -> last month of window
first, last = pdf.iloc[0], pdf.iloc[-1]
avg_mom_orders = float(pdf["orders_mom_pct"].iloc[1:].mean())
avg_mom_gmv = float(pdf["gmv_mom_pct"].iloc[1:].mean())

# Nov 2017 vs Oct 2017 and Dec 2017 (does the spike fade?)
def row(m): return pdf[pdf["purchase_month"] == m].iloc[0]
nov, dec = row("2017-11"), row("2017-12")

print(f"Peak month by orders: {peak.purchase_month} ({int(peak.orders)} orders, BRL {peak.gmv:,.2f})")
print(f"Peak day: {dpeak.d} ({int(dpeak.orders)} orders)")
print(f"YoY Jan-Aug 2018 vs 2017: orders {yoy_orders:+.1f}%, GMV {yoy_gmv:+.1f}%")

# ---- 8. Chart: two stacked panels sharing the month axis ----
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

common.set_style()
months = pd.to_datetime(pdf["purchase_month"] + "-01")
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), sharex=True, facecolor="white")

ax1.plot(months, pdf["orders"], color=common.PRIMARY, lw=2.5, marker="o")
ax1.set_ylabel("Orders per month (count)")
ax1.set_ylim(0, pdf["orders"].max() * 1.2)
ax2.plot(months, pdf["gmv"] / 1e3, color=common.ACCENT, lw=2.5, marker="o")
ax2.set_ylabel("Revenue / GMV per month (BRL thousand)")
ax2.set_ylim(0, pdf["gmv"].max() / 1e3 * 1.2)

# Annotate the peak directly on each panel
pm = pd.to_datetime(peak.purchase_month + "-01")
ax1.scatter([pm], [peak.orders], s=140, color=common.ALERT, zorder=5)
ax1.annotate(f"Peak {pd.Timestamp(pm):%b %Y}\n{int(peak.orders):,} orders",
             (pm, peak.orders), xytext=(-110, -5), textcoords="offset points",
             ha="right", va="center", color=common.ALERT, fontweight="bold",
             arrowprops=dict(arrowstyle="->", color=common.ALERT))
gm = pd.to_datetime(peak_gmv.purchase_month + "-01")
ax2.scatter([gm], [peak_gmv.gmv / 1e3], s=140, color=common.ALERT, zorder=5)
ax2.annotate(f"Peak {pd.Timestamp(gm):%b %Y}\nBRL {peak_gmv.gmv/1e3:,.0f}k",
             (gm, peak_gmv.gmv / 1e3), xytext=(-110, -5), textcoords="offset points",
             ha="right", va="center", color=common.ALERT, fontweight="bold",
             arrowprops=dict(arrowstyle="->", color=common.ALERT))
ax2.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b\n%Y"))
ax2.set_xlabel("Purchase month")

fig.suptitle(f"Orders rose {last.orders/first.orders:.1f}x from Jan 2017 to Aug 2018, peaked in "
             f"{pd.Timestamp(pm):%b %Y} (Black Friday), then plateaued", fontsize=14, fontweight="bold", y=0.98)
fig.text(0.01, -0.01,
         "Valid orders only (excl. canceled/unavailable); revenue = item price excl. freight. "
         "Window Jan 2017-Aug 2018: months with <100 orders\n(Sep 2016, Dec 2016, Sep 2018, Oct 2018; extract ends there) excluded.   "
         "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)",
         fontsize=8.5, color=common.GREY, ha="left", va="top")
fig.tight_layout(rect=(0, 0, 1, 0.96))
common.FIGURES.mkdir(exist_ok=True)
fig.savefig(common.FIGURES / "q1_monthly_trend.png", facecolor="white")
plt.close(fig)

# ---- 9. Save every quoted number ----
tbl = {r.purchase_month: {"orders": int(r.orders), "gmv_brl": float(r.gmv), "aov_brl": float(r.aov),
                          "orders_mom_pct": None if pd.isna(r.orders_mom_pct) else float(r.orders_mom_pct),
                          "gmv_mom_pct": None if pd.isna(r.gmv_mom_pct) else float(r.gmv_mom_pct)}
       for r in pdf.itertuples()}
safe_save_metrics("q1_time_series", {
    "window": f"{WIN_START} to {WIN_END}",
    "excluded_thin_months": THIN_MONTHS,
    "monthly": tbl,
    "peak_month_orders": peak.purchase_month,
    "peak_month_orders_count": int(peak.orders),
    "peak_month_orders_gmv_brl": float(peak.gmv),
    "peak_month_gmv": peak_gmv.purchase_month,
    "peak_month_gmv_brl": float(peak_gmv.gmv),
    "nov2017_mom_orders_pct": float(nov.orders_mom_pct),
    "nov2017_mom_gmv_pct": float(nov.gmv_mom_pct),
    "dec2017_mom_orders_pct": float(dec.orders_mom_pct),
    "peak_day": str(dpeak.d),
    "peak_day_orders": int(dpeak.orders),
    "orders_jan2017": int(first.orders), "orders_aug2018": int(last.orders),
    "gmv_jan2017_brl": float(first.gmv), "gmv_aug2018_brl": float(last.gmv),
    "orders_growth_multiple_jan17_to_aug18": round(float(last.orders / first.orders), 2),
    "avg_mom_orders_pct": round(avg_mom_orders, 1),
    "avg_mom_gmv_pct": round(avg_mom_gmv, 1),
    "ytd_jan_aug_2017_orders": o17, "ytd_jan_aug_2018_orders": o18,
    "ytd_jan_aug_2017_gmv_brl": g17, "ytd_jan_aug_2018_gmv_brl": g18,
    "yoy_orders_pct": round(yoy_orders, 1),
    "yoy_gmv_pct": round(yoy_gmv, 1),
    "aov_overall_window_brl": round(float(pdf["gmv"].sum() / pdf["orders"].sum()), 2),
    "spark_sql_matches_dataframe": True,
})
spark.stop()
