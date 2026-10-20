"""Q5: Which seller states ship to which buyer states? (Spark aggregation + networkx map-network)"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import common
import pyspark.sql.functions as F

spark = common.get_spark("q5_network")
lines = spark.read.parquet(common.processed("order_lines"))

# Shared definition: valid orders = not canceled/unavailable
valid = lines.filter(~F.col("order_status").isin("canceled", "unavailable")).cache()

# One row per (seller_state -> customer_state) flow.
# countDistinct(order_id): an order with sellers in 2 states counts once in each flow.
flows = (valid.groupBy("seller_state", "customer_state")
         .agg(F.countDistinct("order_id").alias("orders"),
              F.round(F.sum("price"), 2).alias("gmv"),
              F.round(F.avg("freight_value"), 2).alias("avg_freight"),
              F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"))
         .cache())
flows.orderBy(F.desc("orders")).show(10)

# Intra- vs inter-state split (flow-level orders)
flows = flows.withColumn("intra", F.col("seller_state") == F.col("customer_state"))
split = (flows.groupBy("intra").agg(F.sum("orders").alias("orders"), F.sum("gmv").alias("gmv")).toPandas()
         .set_index("intra"))
tot_orders = int(split["orders"].sum())
intra_orders = int(split.loc[True, "orders"]); inter_orders = int(split.loc[False, "orders"])

# Average freight per ITEM, intra vs inter (computed on item rows, not on flow averages)
fr = (valid.groupBy((F.col("seller_state") == F.col("customer_state")).alias("same"))
      .agg(F.avg("freight_value").alias("f")).toPandas().set_index("same")["f"])
fr_intra, fr_inter = float(fr[True]), float(fr[False])

# Out-degree: number of distinct buyer states each seller state serves; and share of all flow orders
out = (flows.groupBy("seller_state")
       .agg(F.countDistinct("customer_state").alias("out_degree"), F.sum("orders").alias("out_orders"))
       .withColumn("out_share_pct", F.round(100 * F.col("out_orders") / tot_orders, 2))
       .orderBy(F.desc("out_orders")))
out.show(10)
out_pd = out.toPandas()

# Geographic centroid per state = mean lat/lng of zip prefixes in geo_by_zip
cent = (spark.read.parquet(common.processed("geo_by_zip")).groupBy("state")
        .agg(F.avg("lat").alias("lat"), F.avg("lng").alias("lng")).toPandas().set_index("state"))

flows_pd = flows.toPandas()
top10 = flows_pd.sort_values("orders", ascending=False).head(10)
top10_orders_pct = 100 * top10["orders"].sum() / tot_orders

# ---------------- Network graph ----------------
import networkx as nx
import matplotlib.pyplot as plt
common.set_style()
THRESH = 500   # draw only inter-state flows with >= 500 orders (keeps the map readable)
inter = flows_pd[~flows_pd["intra"]]
shown = inter[inter["orders"] >= THRESH]
as_seller = flows_pd.groupby("seller_state")["orders"].sum()
as_buyer = flows_pd.groupby("customer_state")["orders"].sum()
size = as_seller.add(as_buyer, fill_value=0)

G = nx.DiGraph()
for s in cent.index:
    if s in size.index:
        G.add_node(s, pos=(cent.loc[s, "lng"], cent.loc[s, "lat"]))
for r in shown.itertuples():
    G.add_edge(r.seller_state, r.customer_state, w=r.orders)
pos = nx.get_node_attributes(G, "pos")
NS = [40 + 6 * size[n] ** 0.5 for n in G]   # sqrt scaling so SP does not swallow its neighbours

fig, ax = plt.subplots(figsize=(11, 10), facecolor="white")
ax.set_facecolor("white")
nx.draw_networkx_nodes(G, pos, ax=ax, node_size=NS,
                       node_color=[common.ALERT if n == "SP" else common.ACCENT for n in G],
                       alpha=0.7, edgecolors="white")
widths = [0.4 + G[u][v]["w"] / 900 for u, v in G.edges]
nx.draw_networkx_edges(G, pos, ax=ax, width=widths, edge_color=common.PRIMARY, alpha=0.4,
                       arrows=True, arrowsize=12, connectionstyle="arc3,rad=0.12",
                       node_size=NS)
nx.draw_networkx_labels(G, pos, ax=ax, font_size=9, font_weight="bold",
                        labels={n: n for n in G}, font_color="black",
                        bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.7))
ax.set_xlabel("Longitude (degrees)"); ax.set_ylabel("Latitude (degrees)")
ax.grid(alpha=0.2); ax.tick_params(labelbottom=True, labelleft=True)
ax.set_title(f"SP is the hub: {100*intra_orders/tot_orders:.0f}% of orders stay in-state, "
             f"and most cross-state flows start in SP", loc="left")
fig.text(0.01, -0.01,
         f"Arrows go seller state -> buyer state; width ~ orders; node size ~ orders as seller + buyer. "
         f"Only inter-state flows with >= {THRESH} orders drawn ({len(shown)} edges).\n"
         "Nodes placed at mean zip-prefix coordinates per state. "
         "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)", fontsize=8.5, color="dimgray")
fig.savefig(common.FIGURES / "q5_state_flow_network.png"); plt.close(fig)

sp_out = shown[shown.seller_state == "SP"]["orders"].sum() / shown["orders"].sum()
common.save_metrics("q5_network", {
    "flow_orders_total": tot_orders,
    "intra_state_orders": intra_orders, "inter_state_orders": inter_orders,
    "intra_state_share_pct": round(100 * intra_orders / tot_orders, 2),
    "inter_state_share_pct": round(100 * inter_orders / tot_orders, 2),
    "avg_freight_intra_brl": round(fr_intra, 2), "avg_freight_inter_brl": round(fr_inter, 2),
    "top10_flows": [dict(seller_state=r.seller_state, customer_state=r.customer_state, orders=int(r.orders),
                         gmv=float(r.gmv), avg_freight=float(r.avg_freight),
                         avg_delivery_days=float(r.avg_delivery_days)) for r in top10.itertuples()],
    "top10_flows_share_of_orders_pct": round(top10_orders_pct, 2),
    "seller_state_out_degree": {r.seller_state: [int(r.out_degree), float(r.out_share_pct)] for r in out_pd.itertuples()},
    "seller_state_out_degree_note": "state -> [number of buyer states served, % of all flow orders]",
    "edge_threshold_orders": THRESH, "edges_drawn": int(len(shown)),
    "sp_share_of_drawn_edge_orders_pct": round(100 * sp_out, 2),
    "note": "flow_orders counts an order once per (seller_state, customer_state) pair; multi-seller orders can count in more than one flow",
})
spark.stop()
