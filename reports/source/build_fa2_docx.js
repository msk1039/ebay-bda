// Content + generator for the FA2 report (docx-js). Style: see STYLE.md.
// Run: NODE_PATH=/opt/homebrew/lib/node_modules node build_fa2_docx.js
const fs = require("fs");
const path = require("path");
const D = require("docx");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun, Header, Footer,
  AlignmentType, HeadingLevel, WidthType, ShadingType, BorderStyle, LevelFormat, PageNumber,
  TableOfContents, ExternalHyperlink, PageBreak, VerticalAlign,
} = D;

const FA2 = "/Users/meow/pccoe/btech/BDA/fa1/fa2";
const OUT = "/Users/meow/pccoe/btech/BDA/fa1/reports/FA2_Marketplace_Spark_Report.docx";
const NAVY = "1F4E79", BLUE = "2E75B6", GREY = "7F7F7F", ALT = "EAF1F8";
const W = 9026;

// ---------- helpers ----------
function runs(text, opts = {}) {
  // inline markup: **bold**, `code`
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0, m;
  const base = { font: "Calibri", ...opts };
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(new TextRun({ ...base, text: text.slice(last, m.index) }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new TextRun({ ...base, text: t.slice(2, -2), bold: true }));
    else out.push(new TextRun({ ...base, text: t.slice(1, -1), font: "Consolas", size: (opts.size || 22) - 2 }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ ...base, text: text.slice(last) }));
  return out;
}
const body = [];
const P = (t, o = {}) => body.push(new Paragraph({ children: runs(t, o.run), alignment: o.align, keepNext: o.keepNext, spacing: o.spacing, style: o.style }));
const H1 = (t, pb) => body.push(new Paragraph({ heading: HeadingLevel.HEADING_1, pageBreakBefore: !!pb, children: [new TextRun(t)] }));
const H2 = (t) => body.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] }));
const H3 = (t) => body.push(new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(t)] }));
const bullets = (arr, ref = "bul") => arr.forEach((t) => body.push(new Paragraph({ numbering: { reference: ref, level: 0 }, children: runs(t), spacing: { after: 60 } })));
let numRefCount = 0;
const numbered = (arr) => { const ref = "dec" + (numRefCount++); numRefs.push(ref); arr.forEach((t) => body.push(new Paragraph({ numbering: { reference: ref, level: 0 }, children: runs(t), spacing: { after: 60 } }))); };
const numRefs = [];

const border = { style: BorderStyle.SINGLE, size: 4, color: "BFBFBF" };
const borders = { top: border, bottom: border, left: border, right: border };
let tableNo = 0, figNo = 0;
function caption(text) {
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 60, after: 160 },
    children: [new TextRun({ text, italics: true, size: 19, color: GREY, font: "Calibri" })] });
}
function tcap(text) {
  tableNo++;
  body.push(new Paragraph({ keepNext: true, alignment: AlignmentType.LEFT, spacing: { before: 120, after: 60 },
    children: [new TextRun({ text: `Table ${tableNo}: ${text}`, italics: true, size: 19, color: GREY, font: "Calibri" })] }));
}
function table(cap, widths, header, rows, aligns = []) {
  if (cap) tcap(cap);
  const sum = widths.reduce((a, b) => a + b, 0);
  if (sum !== W) widths[widths.length - 1] += W - sum;
  const cell = (t, i, isHead, shade) => new TableCell({
    width: { size: widths[i], type: WidthType.DXA }, borders,
    margins: { top: 80, bottom: 80, left: 120, right: 120 }, verticalAlign: VerticalAlign.CENTER,
    shading: isHead ? { type: ShadingType.CLEAR, fill: NAVY, color: "auto" } : { type: ShadingType.CLEAR, fill: shade ? ALT : "FFFFFF", color: "auto" },
    children: [new Paragraph({ alignment: aligns[i] === "r" ? AlignmentType.RIGHT : AlignmentType.LEFT, spacing: { after: 0, line: 252 },
      children: runs(String(t), isHead ? { bold: true, color: "FFFFFF", size: 20 } : { size: 20 }) })],
  });
  const trs = [new TableRow({ tableHeader: true, cantSplit: true, children: header.map((h, i) => cell(h, i, true)) })];
  rows.forEach((r, ri) => trs.push(new TableRow({ cantSplit: true, children: r.map((c, i) => cell(c, i, false, ri % 2 === 1)) })));
  body.push(new Table({ width: { size: W, type: WidthType.DXA }, columnWidths: widths, rows: trs }));
  body.push(new Paragraph({ spacing: { after: 120 }, children: [] }));
}
function code(lines, cap) {
  const ps = lines.map((l) => new Paragraph({ spacing: { after: 0, line: 240 }, children: [new TextRun({ text: l === "" ? " " : l, font: "Consolas", size: 18 })] }));
  const nb = { style: BorderStyle.SINGLE, size: 4, color: "D9D9D9" };
  if (cap) tcap(cap);
  body.push(new Table({ width: { size: W, type: WidthType.DXA }, columnWidths: [W], rows: [new TableRow({ cantSplit: true, children: [new TableCell({
    width: { size: W, type: WidthType.DXA }, borders: { top: nb, bottom: nb, left: nb, right: nb },
    shading: { type: ShadingType.CLEAR, fill: "F2F2F2", color: "auto" }, margins: { top: 100, bottom: 100, left: 160, right: 160 }, children: ps })] })] }));
  body.push(new Paragraph({ spacing: { after: 120 }, children: [] }));
}
function pngSize(f) { const b = fs.readFileSync(f); return [b.readUInt32BE(16), b.readUInt32BE(20)]; }
function fig(file, cap, maxW = 576) {
  const f = path.join(FA2, "figures", file);
  const [w, h] = pngSize(f);
  const width = Math.min(maxW, 576), height = Math.round((width * h) / w);
  figNo++;
  body.push(new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 120, after: 0 },
    children: [new ImageRun({ type: "png", data: fs.readFileSync(f), transformation: { width, height },
      altText: { title: file, description: cap, name: file } })] }));
  body.push(caption(`Figure ${figNo}: ${cap}`));
}
const link = (text, url) => new ExternalHyperlink({ link: url, children: [new TextRun({ text, style: "Hyperlink", color: "0563C1", underline: {}, font: "Calibri" })] });
function refPara(prefix, text, url) {
  body.push(new Paragraph({ numbering: { reference: "refs", level: 0 }, spacing: { after: 80 }, children: [...runs(prefix), ...(url ? [link(text, url)] : [])] }));
}

// ---------- title page ----------
const title = [];
const tp = (text, o) => title.push(new Paragraph({ alignment: AlignmentType.CENTER, spacing: o.spacing || { after: 200 },
  children: [new TextRun({ text, font: "Calibri", ...o.run })] }));
tp("Big Data Analytics (B.Tech) — Pimpri Chinchwad College of Engineering (PCCoE)", { spacing: { before: 1800, after: 400 }, run: { size: 24, color: GREY } });
tp("Formative Assessment 2 (FA2): Problem-based Solving using Apache Spark and Big Data Visualization", { spacing: { after: 600 }, run: { size: 28, bold: true, color: NAVY } });
tp("eBay-style Marketplace Analytics with Apache Spark", { spacing: { after: 300 }, run: { size: 52, bold: true, color: NAVY } });
tp("When, where and how well does marketplace demand get fulfilled? — Olist marketplace data as a public stand-in for eBay", { spacing: { after: 1200 }, run: { size: 28, color: BLUE } });
tp("Group members: [add names]", { run: { size: 24 } });
tp("Academic year: [add]", { run: { size: 24 } });
tp("Case study: eBay (continued from FA1)", { spacing: { before: 400 }, run: { size: 24 } });

// ---------- TOC page ----------
const tocPage = [
  new Paragraph({ children: [new TextRun({ text: "Contents", bold: true, size: 32, color: NAVY, font: "Calibri" })], spacing: { after: 200 } }),
  new TableOfContents("Contents", { hyperlink: true, headingStyleRange: "1-2" }),
  new Paragraph({ spacing: { before: 200 }, children: [new TextRun({ text: "Right-click → Update Field in Word to refresh page numbers.", italics: true, size: 20, color: GREY, font: "Calibri" })] }),
];

// =====================================================================
// CONTENT
// =====================================================================
H1("Abstract", true);
P("In FA1 we studied eBay's Big Data architecture: SQL warehouses (Teradata, Greenplum), Hadoop (HDFS, YARN, MapReduce) and NoSQL stores (HBase, Cassandra, MongoDB). FA2 continues that case study by applying Apache Spark to a marketplace problem: when, where and how well is buyer demand fulfilled? eBay publishes no transaction data, so we use the Olist Brazilian E-Commerce Public Dataset (nine linked tables, 2016-2018, 99,441 orders) as a public stand-in with the same seller-to-buyer model. Using PySpark in local mode we ingest the data with explicit schemas, clean it, build a denormalised table of 112,650 order lines, store it as Parquet and answer six questions with joins, aggregations, pivots, window functions, Spark SQL and an RDD comparison. Seven visualizations, four of them advanced (time series, heatmap, geospatial map, network graph), show that orders grew 8.16 times between January 2017 and August 2018 with a Black Friday peak, that three states generate 63.35% of revenue, and that late deliveries cut average review scores from 4.29 to 2.27 stars. We turn these findings into six recommendations and describe how the same code would scale on eBay's stack.");

H1("1 Introduction");
H2("1.1 From FA1 to FA2");
P("FA1 described how eBay stores and processes data at very large scale: relational data warehouses for structured reporting, a Hadoop cluster (HDFS for storage, YARN for resource management, MapReduce for batch processing) for raw and semi-structured data, and NoSQL stores (HBase, Cassandra, MongoDB) for low-latency serving. FA2 asks what an analyst would actually do on top of such a platform. Apache Spark is the natural answer: it runs on YARN, reads from HDFS, keeps intermediate data in memory and offers one API for SQL, DataFrames, windows and low-level RDDs.");
H2("1.2 Problem statement");
P("eBay is a marketplace that connects independent sellers to buyers. Its growth depends on knowing **when** demand happens, **where** buyers and revenue are concentrated, and **how well** orders are fulfilled, because late or failed fulfilment damages the ratings that buyers rely on. The problem we solve is therefore: using Apache Spark, find the time pattern, geographic pattern and fulfilment quality of marketplace demand, and turn the findings into recommendations for the platform and its sellers.");
H2("1.3 Objectives");
bullets([
  "Select a suitable public dataset and justify it as a stand-in for eBay.",
  "Implement ingestion, cleaning and analysis in PySpark using both transformations and actions, and demonstrate core Spark concepts (lazy evaluation, joins, windows, Spark SQL, caching, partitioning, Parquet, RDDs).",
  "Present the results with at least three meaningful visualizations, including advanced ones.",
  "Interpret the results and give actionable recommendations, linked back to eBay's FA1 architecture.",
]);
H2("1.4 Analytical questions");
P("We framed six questions. Each is answered with a specific Spark technique and a matching chart; four of the charts are advanced types.");
table("The six analytical questions, Spark techniques and visualizations", [560, 3000, 2900, 2566],
  ["#", "Question", "Spark technique", "Visualization"],
  [
    ["Q1", "How did orders and revenue grow month by month?", "Join, groupBy month, window `lag` for month-on-month growth", "Time-series line charts (**advanced**)"],
    ["Q2", "Which weekdays and hours do buyers purchase most?", "Date functions, groupBy, `pivot` into a 7 x 24 matrix", "Weekday-by-hour heatmap (**advanced**)"],
    ["Q3", "Where are buyers and revenue concentrated?", "Aggregate by zip prefix and state, join to geolocation", "Geospatial map, static and interactive (**advanced**)"],
    ["Q4", "Do late deliveries lower review scores?", "`datediff`-style delay features, join reviews, bucketing", "Bar charts (score by delay; late rate by state)"],
    ["Q5", "Which seller states ship to which buyer states?", "Multi-table join, groupBy (seller state, buyer state)", "Network graph on geographic coordinates (**advanced**)"],
    ["Q6", "Which product categories earn the most?", "Join to translation table, global and partitioned `rank` windows", "Horizontal bar chart"],
  ]);

H1("2 Dataset");
H2("2.1 Why a stand-in for eBay");
P("eBay does not publish transaction-level data. The Olist Brazilian E-Commerce Public Dataset (Kaggle) comes from a marketplace with the same structure as eBay: independent sellers list products and buyers order them through the platform, with payments, shipping and reviews recorded per order. It is public, has about 100 thousand orders across nine linked tables (which makes joins meaningful) and contains delivery dates and review scores, which let us study fulfilment quality. The patterns we find are Brazilian patterns for 2016-2018, so they illustrate the method rather than describe eBay itself.");
H2("2.2 The nine files");
table("Olist source files and raw row counts", [3200, 1500, 4326],
  ["File (table)", "Rows", "Content"],
  [
    ["orders", "99,441", "One row per order: status, purchase, approval, delivery and estimated dates"],
    ["order_items", "112,650", "One row per item: product, seller, price, freight"],
    ["customers", "99,441", "Customer id (per order), unique person id, city, state, zip prefix"],
    ["sellers", "3,095", "Seller id, city, state, zip prefix"],
    ["products", "32,951", "Category, dimensions, weight"],
    ["payments", "103,886", "Payment type and value per order"],
    ["reviews", "99,224", "Review score (1-5) and comments"],
    ["geolocation", "1,000,163", "Latitude and longitude points per zip prefix"],
    ["category_translation", "71", "Portuguese to English category names"],
  ], ["l", "r", "l"]);
H2("2.3 Data quality findings and cleaning decisions");
P("Phase 1 of the pipeline profiles every table (row counts, null counts, duplicate keys, impossible values) and records the results. The main findings and our decisions are summarised below.");
table("Data quality findings and decisions", [1500, 4000, 3526],
  ["Table", "Finding", "Decision"],
  [
    ["orders", "625 canceled and 609 unavailable orders; 8 'delivered' orders without a delivery date; 6 orders with a delivery date but a different status; 775 orders have no item row", "Kept all rows; filter valid orders per analysis; delivery fields stay null instead of being guessed"],
    ["order_items", "No duplicate keys, no orphans, no price at or below 0, no negative freight", "Deduplicate and drop orphans if any (none found)"],
    ["customers", "customer_id is per order: 96,096 real people behind 99,441 ids", "Trim and case-normalise city and state; zip kept as string; no rows dropped"],
    ["products", "610 products without a category; 2 categories missing from the translation table; misspelled column names", "Left join so no product is lost; missing category becomes 'unknown'; column names fixed"],
    ["payments", "3 rows with type 'not_defined'; 9 rows with value 0", "Kept (they belong to real orders); sum per order"],
    ["reviews", "789 duplicated review_id values (814 surplus rows); 547 orders with more than one review", "Kept the latest review per order, giving 98,673 rows; dropped the mostly empty title column"],
    ["geolocation", "42 points outside Brazil; 261,831 exact duplicate rows; 1,000,163 points for 19,015 zip prefixes", "Dropped outliers, averaged latitude and longitude per zip prefix: 19,010 rows"],
    ["order_lines", "Derived table; 942 order lines without a review score and 2,454 without delivery fields", "Row count equals order_items (112,650), so no join fan-out"],
  ]);
H2("2.4 Shared definitions");
bullets([
  "**Valid orders:** all orders except those with status canceled or unavailable.",
  "**GMV (gross merchandise value):** the sum of item price, excluding freight, in Brazilian reais (BRL).",
  "**Analysis window for trends:** January 2017 to August 2018. Months with fewer than 100 orders (2016-09 with 4, 2016-12 with 1, 2018-09 with 16 and 2018-10 with 4) are excluded because growth percentages on such small bases are meaningless.",
  "**Late delivery:** an order delivered after its estimated delivery date; delay is the delivered date minus the estimated date in days.",
]);

H1("3 Methodology and Spark implementation");
H2("3.1 Environment");
table("Software environment", [3000, 6026], ["Component", "Version / setting"],
  [
    ["Apache Spark (PySpark)", "4.2.0, local mode with 2 cores, 1.5 GB driver memory, 8 shuffle partitions"],
    ["Java", "17"],
    ["Python", "3.14"],
    ["Interface", "JupyterLab; kernel \"Python (fa2-olist)\" in the project virtual environment"],
    ["Plotting", "matplotlib and seaborn (static), folium (interactive map), NetworkX (network graph)"],
    ["Storage format", "Parquet for cleaned tables"],
  ]);
H2("3.2 Pipeline");
numbered([
  "**Ingest.** Each of the nine CSV files is read with an explicit `StructType` schema (identifiers and zip codes as strings, so \"01037\" keeps its leading zero; timestamps and decimals typed). Reviews are read with multi-line quoting because comments span lines.",
  "**Clean and engineer features.** Normalise text, fix column names, deduplicate reviews, aggregate geolocation to one point per zip prefix, and derive purchase month, weekday, hour, delivery days, delay days and a late flag.",
  "**Denormalise.** Left-join order_items to orders, customers, sellers, products and reviews into one `order_lines` table. Every joined table is unique on its join key, so the result has exactly 112,650 rows (asserted in the code).",
  "**Persist.** Write the cleaned tables as Parquet, so analyses read them quickly and never touch the raw CSVs again.",
  "**Analyse.** One script per question reads `order_lines` from Parquet, aggregates in Spark and hands only the small result (at most a few thousand rows) to pandas for plotting. Every number is saved to a metrics file that this report quotes.",
]);
code([
  "def read(name):",
  "    return (spark.read.option(\"header\", True)",
  "            .option(\"multiLine\", True)",
  "            .option(\"quote\", '\"').option(\"escape\", '\"')",
  "            .schema(SCHEMAS[name])          # explicit StructType, no inference pass",
  "            .csv(common.raw_csv(name)))",
  "",
  "SCHEMAS[\"olist_orders_dataset\"] = StructType([",
  "    S(\"order_id\"), S(\"customer_id\"), S(\"order_status\"),",
  "    S(\"order_purchase_timestamp\", TS), S(\"order_approved_at\", TS), ...])",
], "Reading a CSV with an explicit schema (p1_prepare.py)");
code([
  "# every join is a LEFT join onto a table unique on its key: no row fan-out",
  "order_lines = (items.join(o_cols, \"order_id\", \"left\")",
  "               .join(c_cols, \"customer_id\", \"left\")",
  "               .join(s_cols, \"seller_id\", \"left\")",
  "               .join(p_cols, \"product_id\", \"left\")",
  "               .join(r_cols, \"order_id\", \"left\"))",
  "order_lines.explain()     # lazy: prints the plan, runs no job",
  "write(order_lines, \"order_lines\")   # action: triggers the work",
  "assert n_items == n_lines   # 112,650 == 112,650",
], "Building the denormalised order_lines table (p1_prepare.py)");
H2("3.3 Spark concepts used");
table("Spark concepts, where they are used and why", [2200, 3400, 3426],
  ["Concept", "Where used", "Why"],
  [
    ["Transformations vs actions", "`filter`, `select`, `join`, `groupBy` build a plan; `count`, `show`, `take`, `toPandas`, `write` execute it", "Nothing runs until an action, so Spark can optimise the whole chain"],
    ["Lazy evaluation, `explain()`, Catalyst", "`explain()` on `order_lines` before it is written", "Shows the optimised physical plan (scans, projections, joins) without running a job"],
    ["Joins and shuffles", "Five-table join for `order_lines`; reviews and geolocation joins; Q5 four-table join", "Combining normalised tables; joins and `groupBy` shuffle data across partitions, the most expensive step"],
    ["`groupBy` / `agg`", "All six questions (counts, distinct counts, sums, averages)", "Aggregation of 112,650 lines into a few rows for plotting"],
    ["`pivot`", "Q2: hours become 24 columns of a weekday matrix", "Turns long data into the matrix a heatmap needs"],
    ["Window functions", "`lag` for month-on-month growth (Q1); global `rank` and `rank` partitioned by state (Q6)", "Row-level calculation using neighbouring rows without collapsing the data"],
    ["Spark SQL temp views", "Q1 monthly totals re-computed in SQL on a temp view", "Asserted equal to the DataFrame result: both APIs share one engine"],
    ["`cache()`", "Raw tables and fact tables reused by several actions", "Avoids recomputing the chain from disk on each action"],
    ["Partitions and `repartition`", "RDD demo: 2 partitions, then `repartition(4)` gives 4", "Partitions are the unit of parallelism; repartitioning shuffles data"],
    ["Parquet", "Cleaned tables written and re-read by every analysis", "Columnar, compressed, schema stored with the data"],
    ["RDD `map` / `reduceByKey`", "Orders per state and items per category", "Low-level contrast with the DataFrame API (section 3.4)"],
  ]);
code([
  "w = Window.orderBy(\"purchase_month\")",
  "monthly = (valid.groupBy(\"purchase_month\")",
  "    .agg(F.countDistinct(\"order_id\").alias(\"orders\"),",
  "         F.round(F.sum(\"price\"), 2).alias(\"gmv\"))",
  "    .withColumn(\"orders_prev\", F.lag(\"orders\").over(w))",
  "    .withColumn(\"orders_mom_pct\",",
  "        F.round((F.col(\"orders\") / F.col(\"orders_prev\") - 1) * 100, 1)))",
], "Q1: monthly aggregation and window lag (q1_time_series.py)");
code([
  "counts = orders.groupBy(\"purchase_dow_num\", \"purchase_hour\")",
  "               .agg(F.count(\"*\").alias(\"orders\"))",
  "matrix = (counts.groupBy(\"purchase_dow_num\")",
  "          .pivot(\"purchase_hour\", list(range(24)))  # listed values: no extra pass",
  "          .sum(\"orders\").na.fill(0)",
  "          .orderBy(\"purchase_dow_num\"))",
  "m = matrix.toPandas()      # only 7 rows x 24 columns leave Spark",
], "Q2: pivot to a weekday-by-hour matrix (q2_heatmap.py)");
H2("3.4 RDD versus DataFrame");
P("To contrast the two APIs we answered the same question (orders per buyer state) both ways. The DataFrame version is one declarative `groupBy().count()`, planned by the optimiser. The RDD version spells out `map` to (key, 1) pairs and `reduceByKey`, which combines within each partition before the shuffle. Both returned identical results (SP 41,746 orders first, then RJ 12,852, MG 11,635, RS 5,466 and PR 5,045).");
P("A practical finding: calling `df.rdd` on the full joined table converts every row, with all its columns and timestamps, into Python objects, and the job took about 74 s. Selecting the single needed column before `.rdd` cut the RDD runtime to about 1.2 s (1.16 s measured, against 0.41 s for the DataFrame version). The lesson is that moving from the DataFrame API to RDDs is not free and should be done on as little data as possible.");
code([
  "rdd_res = (df.select(\"customer_state\").rdd      # one column only: ~74 s -> ~1.2 s",
  "           .map(lambda r: (r.customer_state, 1))",
  "           .reduceByKey(add)                      # combine inside partitions first",
  "           .sortBy(lambda kv: (-kv[1], kv[0]))",
  "           .take(5))                              # action: runs the job",
  "assert [(r[0], r[1]) for r in df_res] == rdd_res  # same answer as DataFrame",
], "RDD map / reduceByKey (rdd_demo.py)");

H1("4 Results and analysis");
H2("4.1 Q1: How did orders and revenue grow?");
P("We filtered valid orders, grouped by purchase month and computed distinct orders, GMV and average order value, then used `lag` over a window ordered by month to obtain month-on-month (MoM) growth. The same monthly totals were recomputed in Spark SQL on a temp view and matched the DataFrame result exactly. Over the window of 20 months, average order value stayed near BRL 137.37, so revenue growth came from more orders rather than bigger baskets.");
table("Selected months (valid orders; GMV excludes freight)", [1600, 1500, 2100, 1900, 1926],
  ["Month", "Orders", "GMV (BRL)", "Orders MoM", "GMV MoM"],
  [
    ["2017-01", "787", "120,098.27", "n/a (first month)", "n/a"],
    ["2017-10", "4,547", "660,179.62", "+7.6%", "+6.2%"],
    ["2017-11", "7,421", "1,003,862.14", "+63.2%", "+52.1%"],
    ["2017-12", "5,618", "742,183.79", "-24.3%", "-26.1%"],
    ["2018-08", "6,421", "848,860.10", "+3.0%", "-3.3%"],
  ], ["l", "r", "r", "r", "r"]);
fig("q1_monthly_trend.png", "Orders rose 8.2x from Jan 2017 to Aug 2018, peaked in Nov 2017, then plateaued.", 470);
P("Monthly orders grew from 787 to 6,421, a multiple of 8.16, with an average MoM growth of 15.6% in orders and 14.2% in GMV. Comparing January-August 2018 with the same months of 2017, orders rose from 22,562 to 53,530 (+137.3%) and GMV from BRL 3,080,850.81 to BRL 7,341,037.41 (+138.3%). November 2017 was the peak, and the single busiest day was 2017-11-24 with 1,166 orders, consistent with Black Friday although the data does not prove the cause. After the peak, orders fell 24.3% in December and settled on a plateau of roughly 6,100 to 7,200 orders a month in 2018.");

H2("4.2 Q2: Which weekdays and hours do buyers purchase most?");
P("Distinct valid orders were counted per (weekday, hour) and the hours pivoted into 24 columns, giving a 7 x 24 matrix (98,199 orders) that was passed to the heatmap.");
table("Orders by weekday (valid orders)", [1100, 1100, 1100, 1100, 1100, 1100, 1100],
  ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
  [["15,978", "15,758", "15,350", "14,571", "13,948", "10,763", "11,831"]], ["r", "r", "r", "r", "r", "r", "r"]);
fig("q2_weekday_hour_heatmap.png", "Buyers shop on weekdays between mid-morning and evening; nights are quiet.");
P("Monday is the busiest weekday (16.3% of orders) and Saturday the quietest (11.0%); weekdays together carry 77.0% of orders and the weekend 23.0%, an average of 15,121 orders per weekday against 11,297 per weekend day. The 14:00-17:00 band alone holds 19.8% of orders, hours 10:00-22:00 hold 75.7%, and the quietest period, 00:00-06:00, only 4.8%. The single busiest cell is Tuesday at 14:00 with 1,106 orders, and the busiest hour overall is 16:00 with 6,594 orders.");

H2("4.3 Q3: Where are buyers and revenue concentrated?");
P("Orders and GMV were aggregated per customer zip prefix and per state. The zip aggregates were left-joined to the averaged geolocation table (so unmatched zips could be counted rather than silently lost) and only the 14,807 mapped prefixes were handed to the plotting libraries. Of 98,199 valid orders, 274 (0.28%, from 157 zip prefixes) could not be placed on the map.");
table("Top five buyer states by GMV (valid orders)", [1000, 1300, 1900, 1400, 2026, 1400],
  ["State", "Orders", "GMV (BRL)", "GMV share", "GMV per customer (BRL)", "Sellers"],
  [
    ["SP", "41,125", "5,163,867.22", "38.27%", "129.92", "1,849"],
    ["RJ", "12,697", "1,811,623.42", "13.43%", "148.00", "171"],
    ["MG", "11,496", "1,573,508.20", "11.66%", "141.32", "244"],
    ["RS", "5,415", "742,559.78", "5.50%", "141.90", "129"],
    ["PR", "4,982", "676,883.06", "5.02%", "140.32", "349"],
  ], ["l", "r", "r", "r", "r", "r"]);
fig("q3_buyer_map.png", "Orders cluster along the south-east coast, around Sao Paulo and Rio de Janeiro.", 470);
P("SP, RJ and MG together give 63.35% of national GMV (BRL 13,494,400.74) and 66.52% of orders. SP alone has 41,125 orders (41.88% of orders), and the city of Sao Paulo alone 15,291 orders (15.57%), ahead of Rio de Janeiro (6,800; 6.92%) and Belo Horizonte (2,737; 2.79%). Buyers in RJ spend more per customer than those in SP (BRL 148.00 against 129.92). Supply is even more concentrated than demand: 1,849 of 3,095 sellers (59.74%) are in SP. The notebook also contains an interactive folium map (`figures/q3_buyer_map.html`, 9,758 circle markers, 7.17 MB) with zoom and tooltips; the static figure above is the same data on a log colour scale.");

H2("4.4 Q4: Do late deliveries lower review scores?");
P("Delivery metrics were computed at order level: delay = delivered date minus estimated date, an order is late if delay is positive, and the delivered orders were joined to the latest review per order. Of 96,470 delivered orders, 95,824 have a review.");
table("Review scores for on-time and late orders", [3000, 2000, 2000, 2026],
  ["Group", "Orders", "Average score", "1-star reviews"],
  [["On time", "89,443", "4.29", "6.62%"], ["Late", "6,381", "2.27", "53.77%"]], ["l", "r", "r", "r"]);
fig("q4_late_vs_score.png", "Late deliveries drag reviews down: 4.29 stars on time versus 2.27 when late.", 500);
table("Review score by delivery delay bucket", [2400, 1800, 2400, 2426],
  ["Delay bucket", "Orders", "Average score", "1-star share"],
  [
    ["Early more than 7 days", "70,929", "4.317", "6.47%"],
    ["Early 1-7 days", "17,234", "4.201", "7.07%"],
    ["On time (0 days)", "1,280", "4.034", "8.52%"],
    ["Late 1-3 days", "1,852", "3.291", "25.11%"],
    ["Late 4-7 days", "1,748", "2.103", "58.58%"],
    ["Late 8-14 days", "1,446", "1.671", "70.61%"],
    ["Late more than 14 days", "1,335", "1.724", "68.99%"],
  ], ["l", "r", "r", "r"]);
fig("q4_late_rate_by_state.png", "Late deliveries are far more common in the north-east and Rio than in the south-east core.", 460);
P("The national late rate is 6.77%, but it varies strongly by buyer state (among the 17 states with at least 500 delivered orders): Maranhao (MA) reaches 17.43% with 21.5 days average delivery, Ceara (CE) 13.76% and Bahia (BA) 12.16%, while SP has 4.49% (8.7 days) and PR the lowest at 4.04%. Scores fall steeply as delays grow and flatten after a week late. The overall correlation between delay in days and review score is -0.27, so this is an association: late orders may also differ in product or seller.");

H2("4.5 Q5: Which seller states ship to which buyer states?");
P("We joined order_lines (which carries seller and buyer state) and counted orders per (seller state, buyer state) pair, together with GMV, average freight and average delivery days, giving 98,715 flows. An order with items from several seller states is counted once in each pair. The resulting edge list was drawn with NetworkX, with each state placed at the mean coordinates of its zip prefixes.");
table("Top five seller-to-buyer flows", [1700, 1700, 1500, 1400, 1300, 1426],
  ["Seller state", "Buyer state", "Orders", "GMV (BRL)", "Avg freight (BRL)", "Avg delivery days"],
  [
    ["SP", "SP", "31,293", "3,585,867.19", "13.20", "7.86"],
    ["SP", "RJ", "8,421", "1,099,866.81", "20.46", "16.03"],
    ["SP", "MG", "7,565", "939,581.74", "20.25", "12.23"],
    ["SP", "RS", "3,661", "449,317.64", "20.70", "15.95"],
    ["SP", "PR", "3,167", "400,412.64", "20.50", "12.72"],
  ], ["l", "l", "r", "r", "r", "r"]);
fig("q5_state_flow_network.png", "SP is the hub: 36% of orders stay in-state, and most cross-state flows start in SP.", 450);
P("Only 35.82% of flows (35,356 orders) stay inside one state; 64.18% (63,359) cross state lines. SP sellers originate 70.76% of all flows, and the ten largest flows account for 66.9% of orders. Distance costs money and time: average freight is BRL 23.69 for inter-state orders against BRL 13.45 intra-state, and SP to BA takes 19.68 days (2,380 orders) against 7.86 days for SP to SP. In the figure only inter-state flows with at least 500 orders are drawn (24 edges), of which SP-origin edges carry 73.13% of the drawn order volume.");

H2("4.6 Q6: Which product categories earn the most?");
P("Valid order lines were grouped by English category name, ranked by GMV with a global `rank` window (acceptable because only 74 categories exist) and, separately, ranked within each buyer state with a partitioned window to find the leading category per state.");
table("Top ten categories by GMV", [600, 2800, 1700, 1000, 1000, 1000, 926],
  ["Rank", "Category", "GMV (BRL)", "Share", "Orders", "Avg price", "Avg review"],
  [
    ["1", "health_beauty", "1,255,695.13", "9.31%", "8,800", "130.34", "4.15"],
    ["2", "watches_gifts", "1,198,185.21", "8.88%", "5,604", "200.70", "4.03"],
    ["3", "bed_bath_table", "1,035,964.06", "7.68%", "9,399", "93.36", "3.90"],
    ["4", "sports_leisure", "979,740.92", "7.26%", "7,673", "114.06", "4.12"],
    ["5", "computers_accessories", "904,322.02", "6.70%", "6,654", "116.22", "3.95"],
    ["6", "furniture_decor", "727,465.05", "5.39%", "6,425", "87.67", "3.92"],
    ["7", "housewares", "626,825.80", "4.65%", "5,847", "90.65", "4.07"],
    ["8", "cool_stuff", "620,770.49", "4.60%", "3,616", "164.27", "4.16"],
    ["9", "auto", "586,585.73", "4.35%", "3,872", "139.53", "4.08"],
    ["10", "garden_tools", "481,009.94", "3.56%", "3,505", "111.14", "4.06"],
  ], ["r", "l", "r", "r", "r", "r", "r"]);
fig("q6_top_categories.png", "Ten of 74 categories earn 62.37% of all GMV.", 500);
P("The top 10 of 74 categories earn 62.37% of GMV, led by health_beauty (9.31%) and watches_gifts (8.88%). Volume and value differ: bed_bath_table has the most orders among the top three (9,399) but a low average price (BRL 93.36), whereas watches_gifts earns nearly as much from far fewer orders at BRL 200.70 per item. Review scores also differ (bed_bath_table 3.90, cool_stuff 4.16), and the leading category varies by state: bed_bath_table in SP and RS, watches_gifts in RJ and PR, health_beauty in MG.");

H1("5 Visualization techniques");
table("Charts used", [1900, 1800, 3926, 1400],
  ["Chart", "Type", "Why chosen", "Advanced?"],
  [
    ["Monthly orders and GMV (Fig. 1)", "Time-series line, two panels", "Growth and a single spike are read best along a time axis; two panels avoid a dual axis", "Yes"],
    ["Weekday-by-hour (Fig. 2)", "Heatmap", "Two categorical dimensions and a count: colour shows intensity in 168 cells", "Yes"],
    ["Buyer map (Fig. 3, folium map)", "Geospatial scatter / circle map", "The question is about place; position, size and colour show concentration", "Yes"],
    ["Score by delay bucket (Fig. 4)", "Bar chart", "Compares a mean across ordered buckets; colour separates on-time from late", "No"],
    ["Late rate by state (Fig. 5)", "Horizontal bar chart", "Ranks 17 states on one measure", "No"],
    ["State flows (Fig. 6)", "Network graph on map coordinates", "Seller-to-buyer relationships are edges between states; geography keeps it readable", "Yes"],
    ["Top categories (Fig. 7)", "Horizontal bar chart", "Ranked long category names fit best on a horizontal axis", "No"],
  ]);
P("Design choices applied to every figure:");
bullets([
  "**Takeaway titles.** Each title states the finding (for example \"Late deliveries drag reviews down\") instead of naming the chart.",
  "**Annotations.** Peaks and key contrasts are marked directly on the chart (the Nov 2017 peak, the 1-star shares, the largest cities) so the reader does not need the table.",
  "**Log colour scale on the map.** Orders per zip prefix range from one to thousands; a linear scale would show only Sao Paulo and Rio. A log scale keeps small clusters visible.",
  "**Geographic node placement in the network.** States sit at their mean zip-prefix coordinates rather than a force-directed layout, so the graph doubles as a map and distance is visible.",
  "**Edge threshold.** Only inter-state flows with at least 500 orders are drawn (24 edges); including all flows would hide the structure behind clutter. The table reports the full counts.",
  "**Consistent palette and honesty.** One navy and blue palette with red for the alert or peak, labelled axes with units (BRL), and a source note on each figure; bar charts start at zero.",
]);

H1("6 Insights and recommendations");
H2("6.1 Key findings");
numbered([
  "**Q1: Growth was steep and Black Friday was the biggest spike.** Monthly orders rose from 787 (Jan 2017) to 6,421 (Aug 2018), about 8.16 times. November 2017 peaked at 7,421 orders (BRL 1,003,862.14), up 63.2% on October, with a single-day peak on 2017-11-24 (1,166 orders), then fell 24.3% in December. A marketplace must provision capacity and promotions for a few extreme days, not the average month. Black Friday is a plausible cause, not proven.",
  "**Q2: Buyers shop on weekdays, in the afternoon and evening.** Monday is busiest (16.3% of orders) and Saturday quietest (11.0%); 14:00-17:00 alone holds 19.8% of orders and the single busiest slot is Tuesday 14:00 (1,106 orders). Campaigns and push notifications should land just before these windows, and heavy batch jobs belong overnight (00:00-06:00 is only 4.8% of orders).",
  "**Q3: Demand is concentrated in a few states.** SP, RJ and MG together give 63.35% of GMV; SP alone has 41.88% of orders and the city of Sao Paulo 15.57%. RJ buyers spend more per customer (BRL 148.00) than SP (BRL 129.92). Growth is mostly about deepening the south-east, while the remaining states are a smaller, harder-to-serve opportunity.",
  "**Q4: Late delivery goes with much worse reviews.** On-time orders average 4.29 stars versus 2.27 for late ones, and 53.77% of late orders get 1 star versus 6.62% on time. The national late rate is 6.77% but MA reaches 17.43% (average 21.5 days) while SP is 4.49%. Delivery promises directly shape seller ratings and repeat trust. This is correlation: late orders may also differ in product or seller, and the overall delay-versus-score correlation is only -0.27.",
  "**Q5: SP is the supply hub, so most orders cross state lines.** 64.18% of order flows are inter-state and only 35.82% stay in-state; SP sellers originate 70.76% of all flows. Average freight is BRL 23.69 inter-state versus BRL 13.45 intra-state, and SP to BA takes 19.68 days versus 7.86 for SP to SP. Distance costs buyers money and time, so geographic seller mix matters.",
  "**Q6: A few categories earn most of the money.** The top 10 of 74 categories earn 62.37% of GMV; health_beauty leads (9.31%), then watches_gifts (8.88%). Review scores differ (bed_bath_table 3.90, cool_stuff 4.16), so merchandising should focus on the top 10 while monitoring quality, since big categories are not automatically the best rated.",
]);
H2("6.2 Connecting the findings");
bullets([
  "**Distance chain.** 59.74% of sellers (1,849 of 3,095) are in SP (Q3), so most orders ship from SP (Q5). Far-away buyers wait longer (BA 19.3 days, MA 21.5, CE 21.2) and are late more often (MA 17.43%, CE 13.76%, BA 12.16%) (Q4), and late orders score about 2 stars lower.",
  "**Supply gap.** BA has only 19 sellers for 3,344 orders, so almost everything must be shipped in from elsewhere (Q3, Q5).",
  "**Peak load meets fulfilment.** The November 2017 surge (Q1) hit a network where inter-state shipping is already slow (Q5). Whether the surge itself raised lateness is not tested here.",
]);
H2("6.3 Recommendations");
table("Recommendations with supporting evidence", [2500, 2500, 1500, 2526],
  ["Recommendation", "Evidence (numbers)", "Who acts", "Expected impact"],
  [
    ["Plan capacity, stock and promotions before Black Friday", "Nov 2017: 7,421 orders, +63.2% MoM, then -24.3% in Dec", "Marketplace, sellers", "Fewer stock-outs and slowdowns at peak; smoother December"],
    ["Time campaigns and notifications to 14:00-17:00 on weekdays and Sunday evenings", "14:00-17:00 = 19.8% of orders; Mon 16.3% vs Sat 11.0%", "Marketplace", "Better response per message; batch jobs moved to quiet hours"],
    ["Recruit sellers or open fulfilment hubs outside SP, starting with BA, CE, MA and RJ", "SP sellers = 70.76% of flows; inter-state freight BRL 23.69 vs 13.45", "Marketplace, logistics", "Shorter shipping, lower freight, fewer late orders"],
    ["Show realistic delivery estimates and send proactive delay messages in high-late-rate states", "MA 17.43%, CE 13.76%, BA 12.16% late vs SP 4.49%", "Marketplace, logistics", "Fewer unpleasant surprises, so fewer 1-star reviews"],
    ["Track on-time delivery as a seller-quality metric", "Late orders: 2.27 stars vs 4.29; 1-star 53.77% vs 6.62%", "Marketplace, sellers", "Ratings better reflect seller service; early warning on weak carriers"],
    ["Prioritise the top-10 categories but watch their review scores", "Top 10 = 62.37% of GMV; bed_bath_table 3.90 stars", "Marketplace, sellers", "Revenue focus without letting quality slip in large categories"],
  ]);

H1("7 Scaling to eBay");
P("The analysis ran on a laptop, but each design choice maps onto the architecture we described in FA1.");
bullets([
  "**Storage and compute.** At eBay scale the same PySpark code would run on a YARN cluster reading from HDFS instead of local CSVs. Only the paths and the cluster size change.",
  "**Parquet.** Columnar files let Spark read only the needed columns, which suits wide event tables far better than CSV. Partitioning the data by date would also let queries skip irrelevant files.",
  "**Streaming.** The late-delivery logic (promised date versus now) could run in Spark Structured Streaming to raise real-time delay alerts.",
  "**Serving.** Results such as per-state late rates or seller scores could be pushed to HBase or Cassandra for low-latency lookups on the website.",
  "**Warehouse and MapReduce.** The roles of Teradata or Greenplum (curated SQL reporting) and MapReduce (batch jobs) are covered by Spark SQL and DataFrames, which are shorter to write and keep intermediate data in memory.",
]);

H1("8 Limitations");
bullets([
  "Olist is a stand-in: Brazilian data from 2016-2018, so patterns such as the SP hub and the Black Friday spike will not transfer exactly to eBay.",
  "All links shown are correlations, not proven causes.",
  "Thin months (2016-09, 2016-12, 2018-09, 2018-10) were excluded from the Q1 trend.",
  "Q5 counts a multi-seller order once per seller-state and buyer-state pair, and delivery metrics cover delivered orders only.",
  "Spark ran in local mode on one machine; timings are informational and say nothing about cluster performance.",
]);

H1("9 Conclusion");
P("Using Apache Spark we turned nine raw Olist files into a clean, denormalised Parquet table of 112,650 order lines and answered six questions about marketplace demand and fulfilment. Demand grew 8.16 times in 20 months, peaks sharply around Black Friday, follows a clear weekday and afternoon rhythm and is concentrated in the south-east, while fulfilment from a single hub (SP) produces slow, late deliveries elsewhere that cost review scores. Seven visualizations, four of them advanced, made these patterns visible, and six evidence-backed recommendations follow from them. The same code and design carry over directly to eBay's YARN, HDFS and HBase stack described in FA1.");

H1("References");
refPara("Olist. Brazilian E-Commerce Public Dataset by Olist. Kaggle. ", "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce", "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce");
refPara("Apache Spark documentation. ", "https://spark.apache.org/docs/latest/", "https://spark.apache.org/docs/latest/");
refPara("PySpark API documentation. ", "https://spark.apache.org/docs/latest/api/python/", "https://spark.apache.org/docs/latest/api/python/");
refPara("folium: Python data, Leaflet.js maps. ", "https://python-visualization.github.io/folium/", "https://python-visualization.github.io/folium/");
refPara("NetworkX: network analysis in Python. ", "https://networkx.org/", "https://networkx.org/");
refPara("matplotlib and seaborn, Python plotting libraries. ", "https://matplotlib.org/", "https://matplotlib.org/");
refPara("Seaborn: statistical data visualization. ", "https://seaborn.pydata.org/", "https://seaborn.pydata.org/");
refPara("Group report, FA1: \"eBay and Big Data\", Big Data Analytics (B.Tech), PCCoE.");

// ---------- Appendices ----------
H1("Appendix A: Rubric coverage", true);
table("Coverage of the FA2 rubric", [3000, 900, 5126],
  ["Criterion", "Marks", "Where addressed"],
  [
    ["Problem identification", "1", "Section 1 (FA1 link, problem statement, objectives, Q1-Q6); Section 2.1 (why Olist stands in for eBay); notebook introduction"],
    ["Spark implementation", "3", "Section 3 (explicit schemas, pipeline, concepts table, code excerpts, RDD comparison); `analysis/*.py` scripts and the executed notebook"],
    ["Data analysis and results", "2", "Section 2.3 (data quality) and Section 4 (Q1-Q6 results with tables); every number traced to the metrics file"],
    ["Visualization techniques", "2", "Section 4 (7 figures) and Section 5 (4 advanced: time series, heatmap, geospatial map, network graph); interactive folium map in the notebook"],
    ["Insights, interpretation, recommendations", "2", "Sections 6 to 8 (findings, connections, six recommendations, scaling to eBay, limitations)"],
    ["Total", "10", ""],
  ], ["l", "r", "l"]);

H1("Appendix B: How to run");
numbered([
  "Open a terminal in the project folder and activate the virtual environment: `source .venv/bin/activate`.",
  "Start JupyterLab: `jupyter lab`.",
  "Open `fa2/notebooks/fa2_olist_spark.ipynb` and select the kernel \"Python (fa2-olist)\".",
  "Choose Run All. The notebook runs top to bottom in about 50 seconds on a laptop.",
]);
code([
  "fa2/",
  "  PLAN.md                         project plan",
  "  src/common.py                   Spark session, paths, palette, metrics helper",
  "  data/processed/*.parquet        cleaned tables",
  "  analysis/                       p1_prepare.py, q1_time_series.py ... q6_categories.py,",
  "                                  rdd_demo.py",
  "  figures/                        PNG charts and q3_buyer_map.html",
  "  outputs/                        metrics.json, data_quality.md, insights.md, viva_qa.md",
  "  notebooks/fa2_olist_spark.ipynb final executed notebook",
], "Folder layout");

H1("Appendix C: Viva questions and answers");
const viva = [
  ["Why Spark instead of MapReduce?", "MapReduce writes intermediate results to disk between every stage, while Spark keeps data in memory where it can, so multi-step analysis (joins, aggregations, iterative work) is much faster. Spark also gives DataFrames, SQL and window functions, so the code is shorter and easier to read."],
  ["What are transformations and actions, and what is lazy evaluation?", "Transformations (filter, select, join, groupBy) only build a plan; nothing runs. Actions (count, show, collect, toPandas, write) trigger execution, and Spark optimises the whole plan first. We used `explain()` to show the plan before any action."],
  ["Why did you store data as Parquet?", "Parquet is columnar and compressed, so Spark reads only the columns a query needs and keeps the schema with the data. CSV has to be parsed in full each time. Our cleaned tables (for example order_lines, 112,650 rows) are written once and re-read fast."],
  ["What does caching do?", "`cache()` keeps a DataFrame in memory after it is first computed, so later actions reuse it instead of recomputing the whole chain from disk. It helps when the same table feeds several queries, like our joined order_lines table, but it is wasted if used only once."],
  ["RDD versus DataFrame: what did your demo show?", "A DataFrame has a schema and goes through Spark's optimiser, so it is usually faster and shorter. In our demo, selecting a single column before calling `.rdd` cut the runtime from about 74 s to about 1.2 s, because `df.rdd` converts every row into Python objects, so fewer columns means far less conversion. Both approaches gave the same top-5 states (SP first with 41,746 orders)."],
  ["What is a shuffle and which operations cause it?", "A shuffle moves data across the network so that rows with the same key end up on the same partition. `groupBy`, joins and `repartition` cause it, and it is usually the most expensive step in a Spark job, so we avoid unnecessary ones."],
  ["Why use window functions?", "They compute a value for each row using related rows without collapsing the data. We used `lag` to get month-over-month growth (for example November 2017 orders +63.2%) and `rank` to order categories by revenue."],
  ["Why Olist as a stand-in for eBay?", "eBay does not publish transaction data, and Olist has the same marketplace model: independent sellers selling to buyers. It is public, has about 100k orders over nine linked tables (good for joins), and includes delivery dates and reviews."],
  ["How did you choose each advanced visualization?", "We matched the chart to the question: a time-series line for growth over time (Q1), a weekday-by-hour heatmap for two-dimensional patterns (Q2), a map for geographic concentration (Q3), and a network graph for seller-state to buyer-state flows (Q5). Simple bars were enough for the late-vs-score comparison (Q4) and the top categories (Q6)."],
  ["How would you scale this to eBay volumes?", "Run the same PySpark code on a YARN cluster reading Parquet from HDFS, partition data by date, and avoid collecting large results to the driver. Use Spark Structured Streaming for real-time alerts, and serve results from HBase or Cassandra."],
  ["Why do late orders score 2.27 but on-time orders 4.29?", "Late orders average 2.27 stars against 4.29 for on-time ones, a gap of 2.02, and 53.77% of late orders get 1 star versus 6.62%. This is a correlation: lateness probably drives dissatisfaction, but product and seller differences may also play a part, and the overall delay-vs-score correlation is only -0.27."],
  ["Why did you drop some months and clean the data?", "Months with only a handful of orders (2016-09, 2016-12, 2018-09, 2018-10) would make growth percentages meaningless, so Q1 uses 2017-01 to 2018-08. In cleaning we kept the latest review per order to avoid duplicate rows in joins, and averaged 1,000,163 geolocation points into 19,010 zip-prefix rows."],
];
viva.forEach(([q, a], i) => {
  body.push(new Paragraph({ keepNext: true, spacing: { before: 120, after: 40 }, children: [new TextRun({ text: `${i + 1}. ${q}`, bold: true, color: NAVY, font: "Calibri" })] }));
  P(a);
});

// ---------- document ----------
const hdr = new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ text: "FA2 — Marketplace Analytics with Apache Spark", size: 18, color: GREY, font: "Calibri" })] })] });
const ftr = new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [
  new TextRun({ text: "Page ", size: 18, color: GREY, font: "Calibri" }), new TextRun({ children: [PageNumber.CURRENT], size: 18, color: GREY, font: "Calibri" }),
  new TextRun({ text: " of ", size: 18, color: GREY, font: "Calibri" }), new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, color: GREY, font: "Calibri" })] })] });
const page = { size: { width: 11906, height: 16838 }, margin: { top: 1440, bottom: 1440, left: 1440, right: 1440 } };
const decLevel = (ref) => ({ reference: ref, levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 360 } } } }] });

const doc = new Document({
  creator: "PCCoE BDA group", title: "eBay-style Marketplace Analytics with Apache Spark (FA2)",
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: "Calibri", size: 22 }, paragraph: { spacing: { line: 276, after: 120 } } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 32, bold: true, color: NAVY, font: "Calibri" }, paragraph: { spacing: { before: 360, after: 120 }, outlineLevel: 0, keepNext: true } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 26, bold: true, color: BLUE, font: "Calibri" }, paragraph: { spacing: { before: 240, after: 100 }, outlineLevel: 1, keepNext: true } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 23, bold: true, color: NAVY, font: "Calibri" }, paragraph: { spacing: { before: 180, after: 80 }, outlineLevel: 2, keepNext: true } },
    ],
  },
  numbering: { config: [
    { reference: "bul", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } }] },
    { reference: "refs", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "[%1]", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 540 } } } }] },
    ...numRefs.map(decLevel),
  ] },
  sections: [
    { properties: { page }, children: title },
    { properties: { page }, headers: { default: hdr }, footers: { default: ftr }, children: [...tocPage, ...body] },
  ],
});
Packer.toBuffer(doc).then((b) => { fs.writeFileSync(OUT, b); console.log("wrote", OUT, b.length); });
