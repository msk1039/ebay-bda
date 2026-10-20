# FA2 Plan: eBay-style Marketplace Analytics with Apache Spark (Olist data)

Course: Big Data Analytics, FA2 (Units 3 and 4). Case study continued from FA1: **eBay**.
Final deliverable: **one executed Jupyter notebook** (`notebooks/fa2_olist_spark.ipynb`). No slides.

## Problem statement
eBay's business is a marketplace connecting independent sellers to buyers; its growth depends on
knowing **when** and **where** demand happens and **how well it is fulfilled**. eBay does not
publish its transaction data, so we use the **Olist Brazilian e-commerce marketplace dataset**
(2016-2018, ~100k orders, 9 linked tables) as a public stand-in with the same marketplace model.
Using Apache Spark we answer:

| # | Question | Spark work | Visualization |
|---|---|---|---|
| Q1 | How did orders and revenue grow month by month? | join orders+items, groupBy month, window lag for MoM growth | Time-series line (advanced) |
| Q2 | Which weekdays and hours do buyers purchase most? | date_format/hour, groupBy, pivot | Heatmap (advanced) |
| Q3 | Where are buyers and revenue concentrated? | aggregate 1M geolocation rows per zip prefix, join customers | Geospatial map (advanced) |
| Q4 | Do late deliveries lower review scores? | datediff, late flag, join reviews | Bar charts (score on-time vs late; late rate by state) |
| Q5 | Which seller states ship to which buyer states? | 4-table join, groupBy (seller_state, customer_state) | Network graph (advanced) |
| Q6 | Which product categories earn the most? | join translation, groupBy, rank window | Horizontal bar |

## Spark features to demonstrate
Explicit schema on read, Parquet write/read, filter/select/withColumn, joins, groupBy/agg, pivot,
window functions (lag, rank), Spark SQL temp views, cache/repartition, `explain()` for lazy
evaluation (transformations vs actions), actions (count/show/take/collect/toPandas), one RDD
example (map/reduceByKey) contrasted with the DataFrame API.

## Folder contract
```
fa2/
  src/common.py            get_spark(), paths, palette, set_style(), save_metrics()
  data/processed/*.parquet cleaned tables (Phase 1)
  analysis/q*.py           one script per question (Phase 2)
  figures/                 PNG charts (+ .html for interactive map / network)
  outputs/metrics.json     every number quoted anywhere; written via save_metrics()
  outputs/data_quality.md  Phase 1 profiling report
  notebooks/fa2_olist_spark.ipynb   final executed notebook (Phase 3)
```

## Rules for every agent
- Use only `/Users/meow/pccoe/btech/BDA/fa1/.venv` (`source .venv/bin/activate`). Do not pip-install outside it.
- Spark via `common.get_spark()` (local[2], 1.5 GB) so up to 3 sessions can run at once on 8 GB RAM. Always `spark.stop()`.
- Never modify `../datasets/`. Never invent numbers; every figure and claim comes from computed data and goes into metrics.json.
- Heavy lifting in Spark; pandas only for the final small aggregated result handed to a plotting library.
- Code must be simple and commented so students can explain it in a viva.
- Charts: white background, palette from common.py, title stating the takeaway, labelled axes with units (BRL = Brazilian real), source caption "Source: Olist Brazilian E-Commerce Public Dataset (Kaggle)".

## Phases
1. Data engineer: profile + clean -> Parquet + data_quality.md
2. (parallel) 2a time (Q1, Q2) | 2b geo + logistics (Q3, Q4) | 2c network + categories + RDD demo (Q5, Q6)
3. Notebook builder: one narrative notebook, executed top to bottom
4. Insight writer: insights, recommendations, Unit 3/4 concept mapping, viva Q&A (into notebook markdown)
5. Independent QA: fresh re-run, number cross-check, rubric coverage

## Rubric coverage
Problem (1) -> statement + FA1 link + stand-in justification. Spark (3) -> phases 1-3. Analysis (2) -> Q1-Q6 metrics tables.
Visualization (2) -> 6 charts, 4 advanced. Insights (2) -> phase 4.
