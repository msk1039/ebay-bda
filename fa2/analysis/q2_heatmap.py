"""Q2: Which weekdays and hours do buyers purchase most?

Spark steps: distinct orders per (weekday, hour) -> pivot hours into 24 columns -> 7x24 matrix,
converted to pandas only at the end for the seaborn heatmap.
Run from fa2/:  python analysis/q2_heatmap.py
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
from pyspark.sql import functions as F

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def safe_save_metrics(section, values, tries=10):
    """Retry if another agent is writing metrics.json at the same moment."""
    for _ in range(tries):
        try:
            common.save_metrics(section, values)
            return
        except json.JSONDecodeError:
            time.sleep(0.5)
    raise RuntimeError("could not write metrics.json")


spark = common.get_spark("q2_heatmap")

# ---- 1. One row per order (an order has several item lines), valid orders only ----
# Time-of-day is analysed over the whole dataset (patterns, not trends), so thin months stay in.
orders = (spark.read.parquet(common.processed("order_lines"))
          .filter(~F.col("order_status").isin("canceled", "unavailable"))
          .select("order_id", "purchase_dow_num", "purchase_hour").distinct())
orders.cache()
total_orders = orders.count()

# ---- 2. Count orders per weekday and hour, then pivot hours into columns ----
counts = orders.groupBy("purchase_dow_num", "purchase_hour").agg(F.count("*").alias("orders"))
matrix = (counts.groupBy("purchase_dow_num")
          .pivot("purchase_hour", list(range(24)))     # listing the values avoids an extra pass
          .sum("orders").na.fill(0)
          .orderBy("purchase_dow_num"))
print("Weekday x hour matrix (orders):")
matrix.show(7, truncate=False)

# ---- 3. Only now go to pandas (7 rows x 24 columns) ----
m = matrix.toPandas().set_index("purchase_dow_num")
m.columns = [int(c) for c in m.columns]
m.index = DAYS
assert int(m.values.sum()) == total_orders

# ---- 4. Summary numbers ----
day_tot = m.sum(axis=1)
hour_tot = m.sum(axis=0)
busiest_day = day_tot.idxmax()
quietest_day = day_tot.idxmin()
busiest_hour = int(hour_tot.idxmax())
peak_cell_day = m.stack().idxmax()[0]
peak_cell_hour = int(m.stack().idxmax()[1])
peak_cell_val = int(m.values.max())
quiet_hours = [int(h) for h in hour_tot.nsmallest(5).index]
weekend_share = day_tot[["Sat", "Sun"]].sum() / total_orders * 100
weekday_share = 100 - weekend_share
day_share = float(hour_tot.loc[10:21].sum() / total_orders * 100)   # 10:00-21:59
night_share = float(hour_tot.loc[0:5].sum() / total_orders * 100)   # 00:00-05:59
# Busiest 3-hour band
band = max(range(0, 22), key=lambda h: hour_tot.loc[h:h + 2].sum())
band_share = float(hour_tot.loc[band:band + 2].sum() / total_orders * 100)
# Average daily orders on a weekday (Mon-Fri) vs a weekend day, using each weekday's total / 5 or 2
avg_wd = float(day_tot[DAYS[:5]].sum() / 5)
avg_we = float(day_tot[["Sat", "Sun"]].sum() / 2)
print(day_tot.to_string())
print(f"Busiest day {busiest_day}; busiest hour {busiest_hour}:00; peak cell {peak_cell_day} {peak_cell_hour}:00 = {peak_cell_val}")

# ---- 5. Heatmap ----
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle

common.set_style()
fig, ax = plt.subplots(figsize=(13, 6.2), facecolor="white")
sns.heatmap(m, cmap="Blues", ax=ax, linewidths=0.6, linecolor="white",
            cbar_kws={"label": "Orders (count, Sep 2016-Oct 2018)"})
ax.set_xticklabels([f"{h:02d}" for h in range(24)], rotation=0)
ax.set_yticklabels(DAYS, rotation=0)
ax.set_xlabel("Hour of day purchased (00-23, Brasilia time)")
ax.set_ylabel("Weekday")
ax.grid(False)
# Highlight the peak cell with a red outline + label
r, c = DAYS.index(peak_cell_day), peak_cell_hour
ax.add_patch(Rectangle((c, r), 1, 1, fill=False, edgecolor=common.ALERT, lw=3))
ax.annotate(f"Peak: {peak_cell_day} {peak_cell_hour:02d}:00\n{peak_cell_val} orders",
            (c, r + 0.5), xytext=(2.4, r + 0.5), textcoords="data",
            color=common.ALERT, fontweight="bold", ha="left", va="center",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=common.ALERT),
            arrowprops=dict(arrowstyle="->", color=common.ALERT))
ax.set_title(f"Buyers order on weekdays, mostly 10:00-22:00; busiest slot is {peak_cell_day} {peak_cell_hour:02d}:00",
             loc="left")
fig.text(0.01, -0.02,
         "Valid orders only (excl. canceled/unavailable), one count per order, all months in the dataset.   "
         "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)",
         fontsize=8.5, color=common.GREY, ha="left")
fig.tight_layout()
common.FIGURES.mkdir(exist_ok=True)
fig.savefig(common.FIGURES / "q2_weekday_hour_heatmap.png", facecolor="white")
plt.close(fig)

# ---- 6. Save numbers ----
safe_save_metrics("q2_heatmap", {
    "total_valid_orders": int(total_orders),
    "orders_by_weekday": {d: int(v) for d, v in day_tot.items()},
    "orders_by_hour": {str(h): int(v) for h, v in hour_tot.items()},
    "busiest_weekday": busiest_day,
    "busiest_weekday_share_pct": round(float(day_tot.max() / total_orders * 100), 1),
    "quietest_weekday": quietest_day,
    "quietest_weekday_share_pct": round(float(day_tot.min() / total_orders * 100), 1),
    "busiest_hour": busiest_hour,
    "busiest_hour_orders": int(hour_tot.max()),
    "busiest_3h_band": f"{band:02d}:00-{band + 3:02d}:00",
    "busiest_3h_band_share_pct": round(band_share, 1),
    "peak_cell": {"weekday": peak_cell_day, "hour": peak_cell_hour, "orders": peak_cell_val},
    "quietest_5_hours": sorted(quiet_hours),
    "night_00_06_share_pct": round(night_share, 1),
    "weekend_share_pct": round(float(weekend_share), 1),
    "weekday_share_pct": round(float(weekday_share), 1),
    "avg_orders_per_weekday_mon_fri": round(avg_wd, 1),
    "avg_orders_per_weekend_day": round(avg_we, 1),
    "share_10_to_22_pct": round(day_share, 1),
})
spark.stop()
