"""Shared helpers for FA2: Spark session, paths, chart style, metrics store."""
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]          # .../fa1/fa2
RAW = ROOT.parent / "datasets" / "olist-dataset"    # original Kaggle CSVs (read-only)
PROCESSED = ROOT / "data" / "processed"             # cleaned Parquet tables
FIGURES = ROOT / "figures"
METRICS = ROOT / "outputs" / "metrics.json"

# Brand-neutral, readable palette (one primary, one accent, one alert)
PRIMARY = "#1F4E79"
ACCENT = "#2E75B6"
ALERT = "#C0392B"
GREY = "#7F7F7F"


def get_spark(app_name="fa2-olist", cores=2, memory="1500m"):
    """Local Spark session. Kept small so several sessions can run on an 8 GB laptop."""
    os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def raw_csv(name):
    return str(RAW / f"{name}.csv")


def processed(name):
    return str(PROCESSED / name)


def set_style():
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="whitegrid", font="Arial")
    plt.rcParams.update({
        "figure.dpi": 110,
        "savefig.dpi": 160,
        "savefig.bbox": "tight",
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 12,
    })


def save_metrics(section, values):
    """Merge one section's numbers into outputs/metrics.json (the single source of truth)."""
    METRICS.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(METRICS.read_text()) if METRICS.exists() else {}
    data[section] = values
    METRICS.write_text(json.dumps(data, indent=2, default=str))
