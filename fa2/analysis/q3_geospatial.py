"""Q3 - Where are buyers and revenue concentrated?  (run from fa2/: python analysis/q3_geospatial.py)

Spark steps: filter valid orders -> aggregate per customer zip prefix and per state ->
join zip aggregates to geo_by_zip (lat/lng) -> small results go to pandas for plotting.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
from pyspark.sql import functions as F

SOURCE = "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)"
common.FIGURES.mkdir(exist_ok=True)
spark = common.get_spark("q3-geo")

# ---- 1. Load and keep only valid orders (not canceled / unavailable) -------------------
lines = (spark.read.parquet(common.processed("order_lines"))
         .filter(~F.col("order_status").isin("canceled", "unavailable"))
         .cache())                                   # reused by several aggregations
sellers = spark.read.parquet(common.processed("sellers")).cache()
geo = spark.read.parquet(common.processed("geo_by_zip"))

# ---- 2. Aggregate per customer zip prefix: distinct orders and GMV (price only) --------
zip_agg = (lines.groupBy("customer_zip")
           .agg(F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("price"), 2).alias("gmv")))

# Left join keeps zips without coordinates so we can count how many orders are lost
zip_geo = (zip_agg.join(geo.select("zip_prefix", "lat", "lng"),
                        zip_agg.customer_zip == geo.zip_prefix, "left").cache())
total_orders = lines.select("order_id").distinct().count()
unmapped_orders = zip_geo.filter(F.col("lat").isNull()).agg(F.sum("orders")).first()[0] or 0
unmapped_zips = zip_geo.filter(F.col("lat").isNull()).count()
print(f"Orders that could not be geolocated: {unmapped_orders} of {total_orders} "
      f"({unmapped_zips} zip prefixes)")
# NB: an order has one customer zip, so summing zip-level distinct orders is exact.

# ---- 3. Aggregate per state ------------------------------------------------------------
state_agg = (lines.groupBy("customer_state")
             .agg(F.countDistinct("order_id").alias("orders"),
                  F.round(F.sum("price"), 2).alias("gmv"),
                  F.countDistinct("customer_unique_id").alias("customers")))
national_gmv = lines.agg(F.sum("price")).first()[0]
national_orders = total_orders
state_agg = (state_agg
             .withColumn("gmv_share_pct", F.round(F.col("gmv") / national_gmv * 100, 2))
             .withColumn("orders_share_pct", F.round(F.col("orders") / national_orders * 100, 2))
             .withColumn("gmv_per_customer", F.round(F.col("gmv") / F.col("customers"), 2)))
# sellers per state (from the sellers table, all registered sellers)
sellers_state = sellers.groupBy("seller_state").agg(F.count("*").alias("sellers"))
state_agg = (state_agg.join(sellers_state, state_agg.customer_state == sellers_state.seller_state, "left")
             .drop("seller_state").fillna(0, subset=["sellers"])
             .orderBy(F.desc("gmv")).cache())
print("=== Top 10 states by GMV ===")
state_agg.show(10, truncate=False)

# ---- 4. Top cities (by distinct orders) ------------------------------------------------
city_agg = (lines.groupBy("customer_city", "customer_state")
            .agg(F.countDistinct("order_id").alias("orders"), F.round(F.sum("price"), 2).alias("gmv"))
            .orderBy(F.desc("orders")))
print("=== Top 10 cities by orders ===")
city_agg.show(10, truncate=False)

# ---- 5. Seller concentration -----------------------------------------------------------
n_sellers = sellers.count()
sellers_sp = sellers.filter(F.col("seller_state") == "SP").count()
print(f"Sellers in SP: {sellers_sp} of {n_sellers}")

# ---- 6. Collect small results to pandas ------------------------------------------------
states_pd = state_agg.toPandas().sort_values("gmv", ascending=False)
cities_pd = city_agg.limit(10).toPandas()
points = zip_geo.filter(F.col("lat").isNotNull()).toPandas()
print("Mapped zip prefixes:", len(points))

top3 = states_pd.head(3)
top3_by_orders = states_pd.sort_values("orders", ascending=False).head(3)
sp = states_pd[states_pd.customer_state == "SP"].iloc[0]
top3_orders_share = float(top3_by_orders.orders.sum() / national_orders * 100)
top3_gmv_share = float(top3.gmv.sum() / national_gmv * 100)

# ---- 7a. Interactive folium map: circle per zip prefix, radius ~ sqrt(GMV) --------------
import folium
m = folium.Map(location=[-14.5, -51.0], zoom_start=4, tiles="OpenStreetMap")
# Round coords to 2 decimals and keep only zip prefixes with >=3 orders to keep HTML small
pts = points[points.orders >= 3].copy()
pts["lat"], pts["lng"] = pts.lat.round(2), pts.lng.round(2)
for r in pts.itertuples():
    folium.CircleMarker(
        location=[r.lat, r.lng],
        radius=min(3 + (r.gmv ** 0.5) / 25, 20),
        color=common.PRIMARY, weight=0.5, fill=True, fill_color=common.ALERT, fill_opacity=0.5,
        tooltip=f"zip {r.customer_zip}: {r.orders} orders, BRL {r.gmv:,.0f} GMV",
    ).add_to(m)
m.get_root().html.add_child(folium.Element(
    f"<div style='position:fixed;bottom:8px;left:8px;z-index:9999;background:white;padding:4px 8px;"
    f"font:12px Arial'>Buyer zip prefixes (circle size = GMV, BRL). {SOURCE}</div>"))
html_path = common.FIGURES / "q3_buyer_map.html"
m.save(str(html_path))
html_mb = html_path.stat().st_size / 1e6
print(f"Saved {html_path} ({html_mb:.1f} MB, {len(pts)} markers)")

# ---- 7b. Static PNG: scatter on lng/lat, colour+size = orders (log) ---------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import numpy as np

common.set_style()
fig, ax = plt.subplots(figsize=(9, 9.5), facecolor="white")
ax.set_facecolor("white")
p = points.sort_values("orders")                      # draw big points on top
sc = ax.scatter(p.lng, p.lat, c=p.orders, s=8 + 6 * np.log1p(p.orders), cmap="YlOrRd",
                norm=LogNorm(vmin=1, vmax=p.orders.max()), alpha=0.75, edgecolor="none")
cb = fig.colorbar(sc, ax=ax, shrink=0.55, pad=0.03)
cb.set_label("Orders per zip prefix (log scale)")
ax.set_aspect(1 / np.cos(np.radians(15)))             # lng/lat scaled so Brazil is not stretched
ax.set_xlabel("Longitude (degrees)")
ax.set_ylabel("Latitude (degrees)")
ax.set_title(f"Buyers cluster on the south-east coast: SP alone is {sp.orders_share_pct:.0f}% of orders",
             loc="left")
# Annotate top 6 cities at their mean zip location (manual offsets avoid overlaps)
# label positions (lng, lat) chosen by hand in empty map areas so labels do not overlap
label_pos = {"sao paulo": (-70, -25), "rio de janeiro": (-47, -31), "belo horizonte": (-62, -14),
             "brasilia": (-70, -6), "curitiba": (-70, -30), "campinas": (-70, -20)}
zip_city = lines.select("customer_zip", "customer_city").distinct().toPandas()
for c in cities_pd.head(6).itertuples():
    sub = points.merge(zip_city, on="customer_zip")
    sub = sub[sub.customer_city == c.customer_city]
    x, y = np.average(sub.lng, weights=sub.orders), np.average(sub.lat, weights=sub.orders)
    ax.annotate(f"{c.customer_city.title()} ({c.orders:,})", (x, y), xytext=label_pos[c.customer_city],
                fontsize=9, fontweight="bold", color=common.PRIMARY,
                arrowprops=dict(arrowstyle="-", color=common.GREY, lw=0.8),
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=common.GREY, lw=0.5))
ax.set_xlim(-76, -30)
ax.annotate(f"{SOURCE}. Orders excl. canceled/unavailable; "
            f"{unmapped_orders:,} orders ({unmapped_orders / total_orders * 100:.1f}%) have no coordinates.",
            xy=(0, 0), xycoords="axes fraction", xytext=(0, -42), textcoords="offset points",
            fontsize=8, color=common.GREY)
fig.savefig(common.FIGURES / "q3_buyer_map.png")
plt.close(fig)

# ---- 8. Metrics ------------------------------------------------------------------------
common_metrics = {
    "valid_orders": int(total_orders),
    "national_gmv_brl": round(float(national_gmv), 2),
    "orders_not_geolocated": int(unmapped_orders),
    "orders_not_geolocated_pct": round(unmapped_orders / total_orders * 100, 2),
    "zip_prefixes_not_geolocated": int(unmapped_zips),
    "zip_prefixes_mapped": int(len(points)),
    "map_markers": int(len(pts)),
    "map_html_mb": round(html_mb, 2),
    "top3_states_by_gmv": list(top3.customer_state),
    "top3_states_gmv_share_pct": round(top3_gmv_share, 2),
    "top3_states_by_orders": list(top3_by_orders.customer_state),
    "top3_states_orders_share_pct": round(top3_orders_share, 2),
    "sp_orders": int(sp.orders), "sp_orders_share_pct": float(sp.orders_share_pct),
    "sp_gmv_share_pct": float(sp.gmv_share_pct), "sp_gmv_per_customer_brl": float(sp.gmv_per_customer),
    "top_cities_by_orders": [{"city": r.customer_city, "state": r.customer_state, "orders": int(r.orders),
                              "orders_share_pct": round(r.orders / total_orders * 100, 2)}
                             for r in cities_pd.head(5).itertuples()],
    "n_sellers": int(n_sellers), "sellers_in_sp": int(sellers_sp),
    "sellers_in_sp_pct": round(sellers_sp / n_sellers * 100, 2),
    "states_top10": states_pd.head(10)[["customer_state", "orders", "gmv", "gmv_share_pct", "customers",
                                         "sellers", "gmv_per_customer"]].to_dict("records"),
}
for attempt in range(5):                     # metrics.json is shared with other agents: retry
    try:
        common.save_metrics("q3_geospatial", common_metrics)
        break
    except ValueError:                      # JSONDecodeError while another agent is writing
        import time; time.sleep(1)
print("Metrics saved.")
spark.stop()
