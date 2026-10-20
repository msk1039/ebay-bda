# eBay and Big Data: FA1 and FA2 (Big Data Analytics, PCCoE)

Group coursework for Big Data Analytics (B.Tech, PCCoE). The case study is **eBay**.

- **FA1 (Units 1–2):** a mind-map presentation on how eBay moved from SQL warehouses to Hadoop (HDFS, YARN, MapReduce) and NoSQL (HBase, Cassandra, MongoDB).
- **FA2 (Units 3–4):** an Apache Spark (PySpark) analysis with Big Data visualizations. eBay publishes no transaction data, so we use the **Olist Brazilian marketplace dataset** (about 100k orders, 2016–2018, same seller-to-buyer model) as a public stand-in.

## What's in the repo

```text
ebay-bigdata-fa1.pptx           FA1 presentation (10 slides, speaker notes inside)
reports/                        FA1 and FA2 reports (.docx) + generator scripts in reports/source/
notes/                          Study notes for Units 1-4 and the FA1 presentation guide (start at notes/00-START-HERE.md)
fa2/
  notebooks/fa2_olist_spark.ipynb        FA2 notebook to present (short, guided: dataset -> Spark -> findings)
  notebooks/backup/fa2_olist_spark_FULL.ipynb   Full analysis (6 questions, 7 charts, RDD demo)
  analysis/*.py                 Standalone Spark scripts (data prep + one per question)
  src/common.py                 Shared Spark session, paths, chart style
  figures/                      Charts (PNG) and the interactive buyer map (HTML)
  outputs/                      metrics.json (all numbers), data_quality.md, insights.md, viva_qa.md
datasets/                       Not in Git; see datasets/README.md
```

## FA2 in one paragraph
**Data:** 9 linked tables (orders, items, customers, sellers, products, payments, reviews, geolocation, category names).
**Spark:** load CSVs into DataFrames, clean them with transformations, join everything into one 112,650-row table, use `groupBy`/`agg`, window functions and Spark SQL, and show lazy evaluation with `explain()` and `cache()`.
**Findings:**
- Orders grew about 8× from January 2017 to August 2018, peaking at Black Friday in November 2017.
- São Paulo alone placed about 42% of orders.
- Late deliveries average 2.27 stars against 4.29 on time.

## Run it on your machine

**Requirements:**
- Python 3.10 or newer.
- Java 17 or 21, which Spark needs.
- Git.

```bash
git clone <this-repo-url>
cd <repo-folder>
bash setup.sh            # macOS / Linux   (Windows: setup.bat)
```

The setup script creates `.venv`, installs `requirements.txt` and registers the Jupyter kernel **Python (fa2-olist)**. To reproduce the original machine exactly (Python 3.14), use `bash setup.sh --lock`, which installs from `requirements-lock.txt`.

Then:
1. Download the dataset into `datasets/olist-dataset/` (see [datasets/README.md](datasets/README.md)).
2. Start Jupyter:
   ```bash
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   jupyter lab fa2/notebooks/fa2_olist_spark.ipynb
   ```
3. Pick the kernel **Python (fa2-olist)**, then choose **Run → Restart Kernel and Run All Cells**. It takes a few minutes; the notebook is saved with its outputs, so you can also just read it.

The standalone scripts need the cleaned Parquet data first:
```bash
cd fa2
python analysis/p1_prepare.py      # builds fa2/data/processed/ (ignored by Git)
python analysis/q1_time_series.py  # ... q2_heatmap.py, q3_geospatial.py, q4_delivery_reviews.py, q5_network.py, q6_categories.py, rdd_demo.py
```

### Windows note
Spark on Windows needs Hadoop's `winutils.exe` to **write** files (the Parquet output in `p1_prepare.py`). The main notebook only reads CSVs and should work without it. If a write fails, install winutils for Hadoop 3 and set `HADOOP_HOME`, or use WSL.

## Data source
Brazilian E-Commerce Public Dataset by Olist, on Kaggle, licensed CC BY-NC-SA 4.0: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
