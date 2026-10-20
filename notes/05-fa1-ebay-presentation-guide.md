# FA1 eBay presentation guide (read this tonight)

You present tomorrow. You do not need to be an expert. You need to (1) understand the story, (2) know the 10 slides, (3) deliver a 2-minute talk, and (4) answer basic viva questions in your own words.

Sources used: the deck (slide text and speaker notes) and the FA1 report. All eBay facts here come from those two files only. They are **historical (2009 to 2014)**. Always say "reported" or "around", and never say "eBay uses X today".

Depth notes: `01-unit1-big-data-basics.md` (Unit 1), `02-unit2-hadoop-and-nosql.md` (Unit 2).

---

## 1. The story in 60 seconds

eBay is an online marketplace. Every search, click, bid, order and listing photo creates data. By about 2013 it had 120M+ active users, 300M+ searches a day and 350M+ items for sale.

Until about 2010, eBay analysed this data in big **SQL warehouses**: Teradata and Greenplum. In 2009 one warehouse took in 150 billion new log records a day. These systems **scale up** (buy a bigger, costlier machine) and need a **fixed schema** (the columns must be decided first). Logs, clicks and images do not fit that well. eBay even packed `name=value` text into one SQL column to hold logs.

So eBay added **Hadoop**. Hadoop **scales out**: you add cheap machines. It keeps 3 copies of every block of data, so a dead machine loses nothing. eBay's first large Hadoop cluster took in 8 to 10 TB of new data a day. eBay did not throw SQL away. In 2010 it ran Teradata (10+ PB) beside Hadoop.

Hadoop has three parts: **HDFS stores** data, **YARN schedules** the work, **MapReduce processes** it (for example, building eBay's search indexes). For the live website, eBay added **NoSQL** databases: HBase, Cassandra and MongoDB, each for a different job.

**Thesis (say this one sentence):** *eBay's data outgrew SQL-only warehouses, so it added Hadoop for scale-out storage and processing, and NoSQL databases to serve its live features.*

---

## 2. Words you must know before reading the slides

Skim these first. Every one appears in the deck.

| Term | Plain meaning | Depth |
| --- | --- | --- |
| **TB / PB** | Terabyte = 1,000 GB. Petabyte = 1,000 TB. "8-10 TB a day" is big; "hundreds of PB" is enormous. | 01 |
| **Data warehouse** | A big database built for analysis and reports, not for running the live site. | 01 |
| **Teradata** | A commercial SQL data warehouse. eBay kept it beside Hadoop (10+ PB in 2010). | 01 |
| **Greenplum** | Another SQL warehouse. eBay's held 6.5 PB and 17 trillion records (2009), later replaced by Teradata, mainly over reliability concerns. | 01 |
| **RDBMS / SQL** | Relational database: data in tables with fixed columns, queried with SQL. | 01 |
| **Schema** | The fixed structure (tables, columns, types) you must define before loading data. | 01, 02 |
| **Scale up** | Make one machine bigger. Costly and has a ceiling. | 01 |
| **Scale out** | Add more ordinary machines. Cheaper, and survives failures. | 01 |
| **Commodity hardware** | Ordinary, cheap servers (not special expensive ones). | 02 |
| **Node** | One machine in a cluster. | 02 |
| **Cluster** | A group of nodes working together as one system. | 02 |
| **Distributed system** | Data and work spread over many machines that tolerate some failing. | 01, 02 |
| **HDFS** | Hadoop Distributed File System: stores big files across many nodes. | 02 |
| **Block** | A chunk a file is cut into. In the deck: 128 to 256 MB. | 02 |
| **Replication** | Keeping copies of each block on different machines. Default 3. | 02 |
| **NameNode** | The HDFS "index": knows which blocks make up which file and where they are. | 02 |
| **DataNode** | The machine that actually holds blocks. | 02 |
| **YARN** | Hadoop 2's resource manager: decides who gets CPU and memory. | 02 |
| **Resource Manager** | The YARN boss: assigns cluster resources. | 02 |
| **Node Manager** | The YARN agent on each machine: runs the tasks. | 02 |
| **MapReduce** | Process big data in two steps: Map splits work and emits key-value pairs; Reduce combines them. | 02 |
| **Hive** | SQL-like queries on Hadoop data (Hive was created at Facebook). | 02 |
| **Spark / Storm / Tez** | Other engines that run on Hadoop 2 beside MapReduce (Spark is the Unit 3 topic). | 02, 03 |
| **NoSQL** | Databases without the fixed relational schema, built to spread across many nodes. | 02 |
| **HBase** | Wide-column NoSQL database that sits on HDFS. Fast row lookups; MapReduce can bulk-load it. | 02 |
| **Cassandra** | Wide-column NoSQL with no master; very fast writes. | 02 |
| **MongoDB** | Document NoSQL (JSON-like documents); flexible schema. | 02 |
| **OLAP / Kylin** | OLAP = fast summary queries (totals, group-bys) over huge data. Kylin = eBay's OLAP engine on Hadoop and HBase, open-sourced 2014. | 02 |
| **Cassini** | eBay's search engine. Its indexes are built with MapReduce and stored in HBase. | 02 |
| **Clickstream** | The trail of clicks and page views users leave. | 01 |
| **Log record** | One line a system writes when something happens (a search, a click). | 01 |

---

## 3. Slide by slide

**Reminder from the notes:** the 2-minute talk uses **slides 2, 7 and 9**. The other slides are backup to open during the viva.

### Slide 1: Title

- **On the slide:** "eBay moved from SQL warehouses to Hadoop and NoSQL to handle Big Data". Big Data Analytics, FA1 mind-map case study, Units 1 and 2. `[Group members]`, PCCoE B.Tech, October 2026.
- **Plain words:** the headline is the whole argument.
- **Numbers:** none.
- **Syllabus topic:** the FA1 brief (Unit 1 Introduction to Big Data, Unit 2 Hadoop Architecture).
- **What to say:** "Good morning. We are [names]. Our case is eBay, an online marketplace, and how its data moved from SQL warehouses to Hadoop and NoSQL."
- **If they ask...**
  - *Why eBay, not Facebook?* The syllabus case is Facebook. We chose eBay because its engineers published detailed accounts of both the old SQL systems and the new Hadoop/NoSQL ones. We replaced the "Facebook Case Study" leaf with "eBay Case Study".
  - *Which units?* Unit 1 (Big Data basics) and Unit 2 (Hadoop and NoSQL).

### Slide 2: The question and eBay's scale (TALK SLIDE)

- **On the slide:** two questions: how did eBay use Hadoop and NoSQL where relational databases fell short? Which Hadoop components store and process the data, and which NoSQL databases serve it? Three big numbers.
- **Plain words:** the question we answer, plus proof eBay is huge.
- **Numbers:**

| Number | Meaning | Source |
| --- | --- | --- |
| 120M+ | active users | Hadoop and HBase @ eBay (c. 2013) |
| 300M+ | searches a day | same |
| 350M+ | items for sale | same |

  Notes also use 150 billion log records a day (2009, Monash 2009).
- **Syllabus topic:** Importance of Big Data; Applications.
- **What to say:** "eBay had over 120 million active users, 300 million searches a day and 350 million items. In 2009 one log warehouse took in 150 billion new records a day. One database could not keep up."
- **If they ask...**
  - *Are these numbers current?* No. They are from about 2013 (reported in eBay's own conference slides). We did not check today's figures.
  - *Why does Big Data matter?* Because the value is in the analysis: search ranking, recommendations and fraud detection depend on it.
  - *What is the industry application?* E-commerce: search, recommendations, fraud detection.

### Slide 3: The 5 Vs

- **On the slide:** table of the 5 Vs with eBay evidence. "So what: Volume and Variety strain a fixed-schema SQL warehouse most."
- **Plain words:** eBay's data is big (Volume), fast (Velocity), mixed (Variety), sometimes dirty or fake (Veracity), and worth money (Value).
- **Numbers:**

| Number | Meaning | Source |
| --- | --- | --- |
| 8-10 TB a day | new data into eBay's first large Hadoop cluster | "Hadoop: The Power of the Elephant" (eBay blog, undated; the report places it at 2010) |
| 150 billion/day | new log records (2009) | Monash (2009) |
| 400M+/day | Cassandra writes | Patel (2012), Cassandra at eBay |

- **Syllabus topic:** 5 Vs; Characteristics.
- **What to say (backup):** "Volume is how much: 8 to 10 TB a day. Velocity is how fast: 150 billion log records a day. Variety is formats: logs, clicks, orders, images. Veracity is trust: fraud signals. Value is benefit: search and recommendations."
- **If they ask...**
  - *Define each V.* Volume = how much; Velocity = how fast it arrives and must be processed; Variety = many formats (structured, semi-structured, unstructured); Veracity = how trustworthy or clean; Value = business benefit.
  - *Which V hurts SQL most?* Volume and Variety.
  - *Veracity example?* Fraud-detection time series in Cassandra help flag fake activity.
  - *Is 150 billion records the same as 150 billion TB?* No. It is a record count (also reported as over 50 TB ingested, 2009), not a size.

### Slide 4: SQL warehouses vs Hadoop

- **On the slide:** left: Greenplum 6.5 PB and 17 trillion records; semi-structured logs packed into one text column; Greenplum replaced by Teradata over reliability. Right: add cheap nodes to scale out; 3 copies of every block; old data on 4x cheaper nodes. Bottom: "Not either/or: in 2010 eBay ran Teradata (10+ PB) beside Hadoop."
- **Plain words:** SQL warehouses worked but got expensive and awkward. Hadoop grew by adding cheap machines. But eBay used both.
- **Numbers:**

| Number | Meaning | Source |
| --- | --- | --- |
| 6.5 PB, 17 trillion records | Greenplum size (2009) | Monash (2009) |
| 10+ PB | Teradata size (2010) | Monash (2010) |
| 3 copies | HDFS replication | eBay Hadoop material / HDFS standard |
| 4x cheaper | ARCHIVE (storage-only) tier cost per GB vs normal nodes | "HDFS storage efficiency using tiered storage" (eBay, undated) |

- **Syllabus topic:** RDBMS vs Distributed Systems; Need for Hadoop.
- **What to say (backup):** "SQL warehouses scale up, need a fixed schema, and struggle with heavy joins. Hadoop scales out on cheap nodes. But it was not either/or. In 2010 eBay ran Teradata beside Hadoop."
- **If they ask...**
  - *Why can't an RDBMS handle Big Data?* (1) Scales up: costly with a ceiling. (2) Fixed schema: logs and images fit badly. (3) Heavy joins over huge tables are slow.
  - *Why did eBay keep Teradata?* Sources only say it ran both in 2010, and one analyst reported eBay then saw Hadoop as limited for joins. Do not claim more than that.
  - *Why was Greenplum replaced?* Mainly over reliability concerns (Monash 2010).
  - *Why is the ARCHIVE tier cheaper?* Storage-only machines (no compute), each 220 TB.

### Slide 5: HDFS, YARN, MapReduce

- **On the slide:** three columns. HDFS "stores": NameNode file map; DataNodes with 128-256 MB blocks; 3 copies; eBay hundreds of PB. YARN "schedules": Resource Manager, Node Manager; about 2x job throughput. MapReduce "processes": Map splits, Reduce combines; builds Cassini search indexes.
- **Plain words:** HDFS is the warehouse shelf, YARN is the traffic controller, MapReduce is the worker crew.
- **Numbers:**

| Number | Meaning | Source |
| --- | --- | --- |
| 128-256 MB | block size | Hadoop-2 @ eBay; tiered storage (n.d.). Textbook default is 128 MB |
| 3 | replicas per block | standard |
| hundreds of PB | size of eBay's Hadoop storage, Hadoop 2 era | eBay slides; deliberately a range because sources disagree |
| about 2x | job throughput after moving to Hadoop 2 | Hadoop-2 @ eBay (reported) |

- **Syllabus topic:** HDFS (NameNode, DataNode, replication), YARN (Resource Manager, Node Manager), MapReduce (Map, Reduce).
- **What to say (backup):** "HDFS splits files into big blocks on DataNodes, the NameNode keeps the map, and each block has 3 copies. YARN's Resource Manager hands out resources and Node Managers run tasks. MapReduce maps then reduces. eBay uses it to build its Cassini search indexes."
- **If they ask...**
  - *Role of HDFS?* Distributed storage; NameNode = map of blocks, DataNodes = blocks.
  - *Why replication?* A failed machine loses no data, and reads can use another copy.
  - *What is YARN?* The cluster's resource manager. It fixed Hadoop 1 limits (about 4-5 thousand nodes, about 40 thousand concurrent tasks, one failure killing all jobs, fixed map/reduce slots) and lets Spark, Storm and Tez share the cluster.
  - *MapReduce example?* Counting searches per query: Map emits (query, 1) for each search; Reduce adds the 1s per query.

### Slide 6: HBase, Cassandra, MongoDB

- **On the slide:** table. HBase: Cassini indexes, Kylin OLAP cubes; fast random reads/writes on HDFS, MapReduce bulk-loads it. Cassandra: social signals, fraud time series, order tracking; very fast writes, scalable counters. MongoDB: product metadata loaded daily from Hadoop; flexible schema. "Our pick: HBase for Hadoop-linked work, MongoDB for the product catalogue."
- **Plain words:** three databases, three jobs.
- **Numbers (not on slide, report only):** Cassandra 200 TB+, 400M+ writes and 100M+ reads a day (2012-2013, Patel 2012).
- **Syllabus topic:** NoSQL: HBase, Cassandra, MongoDB.
- **What to say (backup):** "HBase sits on HDFS, so MapReduce can bulk-load it; it holds the Cassini indexes. Cassandra is for fast writes like fraud signals and order tracking. MongoDB holds product data whose fields vary."
- **CAUTION (from the deck notes):** the **MongoDB row is unverified**. It comes from search summaries of an eBay talk that the group has not read in full. If asked, say: "That row is from summaries of an eBay talk, and we have flagged it for checking."
- **If they ask...**
  - *HBase vs MongoDB?* HBase: wide-column store on HDFS, row-key lookups and range scans, bulk load from MapReduce. MongoDB: document store, flexible schema, rich queries, own replication and sharding.
  - *Why HBase for Hadoop-linked work?* It runs on HDFS and MapReduce can feed it directly.
  - *What is Cassandra good at?* Very fast writes across many nodes, no master.
  - *Why MongoDB for the catalogue?* Product fields vary and change; a flexible schema avoids redesigning tables.

### Slide 7: The mind map (TALK SLIDE)

- **On the slide:** a mind map. Read it like this:
  - **Centre:** BIG DATA ANALYTICS.
  - **Left branch, Unit 1: Introduction to Big Data.** Leaves: Definition, Evolution, 5 Vs (Volume, Velocity, Variety, Veracity, Value), Characteristics, Importance, RDBMS vs Distributed Systems, Big Data Frameworks, Analytics Life Cycle, Applications.
  - **Right branch, Unit 2: Hadoop Architecture.** Leaves: Need for Hadoop, Distributed Systems, HDFS (NameNode, DataNode, Replication), YARN (Resource Manager, Node Manager), MapReduce (Map, Reduce), NoSQL (HBase, Cassandra, MongoDB), and **eBay Case Study** (SQL, then Hadoop, then NoSQL).
  - **eBay replaces Facebook:** the syllabus leaf "Facebook Case Study" is now "eBay Case Study".
  - **Orange dashed lines 1 to 7:** cross-links between Unit 1 and Unit 2 (explained on slide 8).
- **Plain words:** it is the syllabus tree, plus seven links showing how Unit 1 problems are answered by Unit 2 tools.
- **Numbers:** 7 cross-links (the brief asks for at least 5).
- **Syllabus topic:** the whole of Units 1 and 2.
- **What to say:** "Our mind map has Big Data Analytics in the centre, Unit 1 on the left and Unit 2 on the right. We replaced the Facebook case study with eBay. Seven orange links tie the units together, for example 5 Vs links to Need for Hadoop."
- **If they ask...**
  - *Which AI tool did you use?* The deck notes say to recreate the map in Napkin AI, XMind, Canva or Miro from the same text. Know which tool your group actually used and say it honestly. The slide is the reference version.
  - *Why seven links?* The brief asks for at least five meaningful ones; each pairs a Unit 1 concept with the Unit 2 component that answers it.
  - *Show me one chain.* 5 Vs, then Need for Hadoop, then Distributed Storage, then HDFS, then MapReduce, then Analytics (the brief's example).

### Slide 8: The seven cross-links

- **On the slide:** table of 7 links with eBay evidence.

| # | Link | Meaning in plain words | eBay evidence |
| --- | --- | --- | --- |
| 1 | 5 Vs, to Need for Hadoop | The Vs create the need | 8-10 TB a day outgrew SQL warehouses |
| 2 | Characteristics, to Distributed Systems | Big, fast, varied data needs many machines | Scale-out nodes survive growth and failures |
| 3 | RDBMS vs Distributed, to HDFS | HDFS is the distributed answer to RDBMS storage limits | Hadoop stored data beside Teradata |
| 4 | Big Data Frameworks, to YARN | YARN lets many frameworks share one cluster | Hadoop 2 runs Spark, Storm and Tez |
| 5 | Analytics Life Cycle, to MapReduce | MapReduce is the "process" step | Process stage builds search indexes |
| 6 | Applications, to NoSQL | Live features need fast databases | NoSQL stores serve live features |
| 7 | Applications, to eBay Case Study | eBay is the real application | Search, recommendations, fraud checks |

- **Numbers:** 8-10 TB a day (link 1; eBay blog, 2010 per report). Others have no figures. Sources on slide: Hadoop-2 @ eBay; Hadoop and HBase @ eBay (c. 2013); Patel (2012); Monash (2010).
- **Syllabus topic:** the "link Unit 1 to Unit 2" requirement of the brief.
- **What to say (backup):** "Each link pairs a Unit 1 concept with the Unit 2 component that answers it, and each has an eBay fact as proof."
- **If they ask...**
  - *Explain the analytics life cycle at eBay.* Collect (searches, clicks, orders, images), Store (HDFS), Process (MapReduce, Spark, Hive, scheduled by YARN), Serve (HBase, Cassandra, MongoDB), Act (search, recommendations, fraud checks, dashboards).
  - *Which framework link?* Link 4: Hadoop 2 with YARN runs Spark, Storm and Tez beside MapReduce.
  - *Is link 3 correct that HDFS "replaces" RDBMS?* No. eBay ran both; HDFS answers the storage limits but does not replace SQL.

### Slide 9: Conclusions (TALK SLIDE; stay here during questions)

- **On the slide:** three conclusions: (1) Big Data outgrew SQL-only warehouses; Volume, Velocity and Variety all appear. (2) Hadoop added scale-out storage and processing: HDFS stores, YARN schedules, MapReduce processes. (3) NoSQL serves live features: HBase, Cassandra, MongoDB each fit a different job. "Are there questions or feedback?"
- **Plain words:** the thesis, in three steps.
- **Numbers:** none.
- **Syllabus topic:** summary of Units 1 and 2 and Applications.
- **What to say:** "To conclude: Big Data outgrew SQL-only warehouses; Hadoop gave scale-out storage and processing; NoSQL serves the live features. We recommend HBase for Hadoop-linked work. Are there any questions or feedback?"
- **If they ask...** The notes list likely viva areas: 5 Vs (slide 3), why RDBMS cannot cope (slide 4), HDFS and replication, YARN, MapReduce example (slide 5), HBase vs MongoDB (slide 6), how eBay uses Hadoop (slides 2 and 5).

### Slide 10: References

- **On the slide:** 9 sources: Monash (2009) and (2010) on the DBMS2 blog; four eBay Tech Blog or Hadoop Summit pieces (Power of the Elephant, tiered storage, Hadoop-2 @ eBay, Hadoop and HBase @ eBay c. 2013); Patel (2012) Cassandra at eBay; eBay Kylin (2014); MongoDB at eBay.
- **Plain words:** where every fact came from.
- **Numbers:** dates 2009, 2010, 2012, c. 2013, 2014; several are "n.d." (no date shown on the page).
- **What to say:** "Our sources are eBay's own engineering talks and blog posts, 2009 to 2014, plus two analyst posts by Curt Monash."
- **If they ask...**
  - *Limitations of your sources?* They are old (2009-2014), we did not check eBay today; eBay's own slides give different node counts for the same years (for example 4,000+ vs 10,000 nodes in 2013-2014), so we show ranges; several pages are undated; the MongoDB source was found via search summaries and not read in full; the Hadoop-limited-for-joins comment is one analyst's report, not eBay's words.
  - *Why n.d.?* The page showed no date.

---

## 4. The 2-minute talk (about 285 words, roughly 1 minute 50 seconds at a calm pace)

The notes say "about 1.5 minutes spoken"; if you run long, drop the sentence marked with an asterisk. Practise out loud with a timer.

**[Slide 2] (about 55 seconds)**

"Good morning. We are [names], and our case is eBay, an online marketplace. eBay has over 120 million active users, 300 million searches a day and 350 million items. In 2009 one log warehouse took in 150 billion new records a day. That is Volume, Velocity and Variety together, so one database could not cope. Volume and Variety hurt a fixed-schema SQL warehouse the most, because logs, clicks and images do not fit neat tables.

Relational warehouses like Teradata and Greenplum scale up and need a fixed schema. Hadoop scales out on cheap nodes and keeps three copies of every block. HDFS stores the data. YARN schedules resources. MapReduce processes the data, for example building eBay's search indexes. eBay did not drop SQL: in 2010 it ran Teradata beside Hadoop, so the move was not either-or."

**[Slide 7] (about 30 seconds)**

"This is our mind map. Big Data Analytics is in the centre, Unit 1 on the left, Unit 2 on the right. We replaced the Facebook case with eBay. Seven orange links connect the units. For example, link one joins the 5 Vs to the need for Hadoop: 8 to 10 terabytes a day outgrew the SQL warehouses.*"

**[Slide 9] (about 40 seconds)**

"For NoSQL we recommend HBase for Hadoop-linked work, because it sits on HDFS and MapReduce can bulk-load it. eBay also uses Cassandra for fast writes and MongoDB for flexible product data. The industry application is e-commerce: search, recommendations and fraud detection.

To conclude: eBay's data outgrew SQL-only warehouses, Hadoop added scale-out storage and processing, and NoSQL serves the live features. Thank you. Are there any questions or feedback?"

**Who says what (pick your group size)**

| Group size | Speaker A | Speaker B | Speaker C | Speaker D |
| --- | --- | --- | --- | --- |
| 3 | Slide 2, first paragraph (intro, scale, why Big Data) | Slide 2, second paragraph and slide 7 (Hadoop, mind map) | Slide 9 (NoSQL, application, conclusion) | none |
| 4 | Intro, scale, why Big Data | Why Hadoop: HDFS, YARN, MapReduce | Slide 7 mind map and links | Slide 9 NoSQL pick, application, conclusion, questions |

All members should know all slides: the examiner picks who answers.

---

## 5. Numbers to memorise

| Number | What it means | Year / source |
| --- | --- | --- |
| 120M+ | active users | c. 2013, Hadoop and HBase @ eBay |
| 300M+ | searches a day | c. 2013, same |
| 350M+ | items for sale | c. 2013, same |
| 150 billion | new log records a day (Greenplum warehouse; over 50 TB ingested) | 2009, Monash |
| 6.5 PB / 17 trillion | Greenplum size / records | 2009, Monash |
| 10+ PB | Teradata size | 2010, Monash |
| 8-10 TB a day | new data into first large Hadoop cluster (Athena, live 2010) | eBay blog (undated; 2010 in report) |
| 128-256 MB | HDFS block size at eBay (textbook default 128 MB) | eBay slides |
| 3 | copies of each block | HDFS standard; eBay |
| 4x cheaper | ARCHIVE storage tier cost per GB | eBay tiered storage post (undated) |
| about 2x | job throughput after moving to Hadoop 2 | Hadoop-2 @ eBay (reported) |
| 400M+ writes a day | Cassandra writes (200 TB+, 100M+ reads a day) | 2012-2013, Patel (2012) |
| 7 | cross-links on the mind map (brief asks 5+) | our deck |
| 2007 | first 4-node Hadoop cluster at eBay | report timeline |

Rule: "hundreds of PB" is deliberately a range. If asked for an exact cluster size or node count, say sources differ (for example 4,000+ vs 10,000 nodes, 2013-2014).

---

## 6. Viva question bank (30 questions)

### A. The eight questions from the brief

1. **Explain the 5 Vs of Big Data.** Volume (how much), Velocity (how fast), Variety (formats), Veracity (trustworthiness), Value (benefit). eBay: 8-10 TB a day; 150 billion log records a day; logs, clicks, orders, images; fraud signals; search and recommendations.
2. **Why can't an RDBMS handle Big Data efficiently?** It scales up rather than out, needs a fixed schema, and struggles with heavy joins over huge tables. eBay packed name=value text into one SQL column to hold semi-structured logs.
3. **What is the role of HDFS?** Distributed storage. Files are split into 128-256 MB blocks held on DataNodes; the NameNode keeps the map of blocks.
4. **Why is replication needed?** Each block has 3 copies on different machines, so a failed node loses no data and reads can use another copy.
5. **What is YARN?** The cluster's resource manager. The Resource Manager assigns resources and Node Managers run tasks. It fixed Hadoop 1 limits and lets Spark, Storm and Tez share the cluster.
6. **Explain MapReduce with an example.** Counting searches per query: Map emits (query, 1) for each search; Reduce adds the 1s per query. eBay uses it to build search indexes.
7. **Difference between HBase and MongoDB?** HBase is a wide-column store on HDFS with row-key lookups and bulk loads from MapReduce. MongoDB is a document store with a flexible schema and rich queries.
8. **How does eBay use Hadoop (the brief asks this about Facebook)?** HDFS stores the data (hundreds of PB), YARN schedules jobs, MapReduce, Hive and Spark process it, and HBase, Cassandra and MongoDB serve results to the site.

### B. Theory on Units 1 and 2

9. **What is Big Data?** Data too large, too fast or too varied for a single conventional database to store and process efficiently.
10. **Is Big Data only about size?** No. Volume is one of five Vs. High speed (Velocity), mixed formats (Variety), doubtful quality (Veracity) and usefulness (Value) matter too. Eight GB of messy, fast-changing logs can be a Big Data problem.
11. **RDBMS vs Hadoop?** RDBMS: scale up, fixed schema on write, good for transactions and structured SQL. Hadoop: scale out on cheap nodes, files stored as they are and structure applied when read, built for huge batch processing, fault tolerant through replication.
12. **Walk me through an HDFS read.** The client asks the NameNode for the file's block locations. The NameNode replies with DataNodes holding each block (nearest first). The client reads blocks directly from those DataNodes. Data does not flow through the NameNode.
13. **Walk me through an HDFS write.** The client asks the NameNode to create the file. The file is cut into blocks. For each block the NameNode picks DataNodes; the client sends the block to the first, which passes it along a pipeline to the others until 3 copies exist. Acknowledgements flow back, and the NameNode records the block map.
14. **What happens if a DataNode fails?** DataNodes send regular heartbeats. If one goes silent, the NameNode notices, and re-creates the lost copies from surviving replicas on other nodes. No data is lost.
15. **What happens if the NameNode fails?** In Hadoop 1 it is a single point of failure: the file system is unavailable. Hadoop 2 adds a standby NameNode (HA) that can take over. (Detail in `02-unit2-hadoop-and-nosql.md`.)
16. **What does YARN do and why was it needed?** It separates resource management from processing. Hadoop 1 had limits (about 4-5 thousand nodes, about 40 thousand concurrent tasks, failures killed all jobs, fixed map/reduce slots). YARN fixes these, and other engines (Spark, Storm, Tez) share the cluster. eBay reported about 2x job throughput.
17. **Explain MapReduce word count.** Map: for each word in each line, emit (word, 1). Shuffle: group all pairs with the same word. Reduce: add the 1s to get the count per word. Same pattern as (query, 1) for eBay searches.
18. **Types of NoSQL databases?** Key-value (for example Redis), document (MongoDB), wide-column (HBase, Cassandra), graph (Neo4j). The syllabus covers HBase, Cassandra, MongoDB.
19. **What is the CAP theorem?** A distributed data store can fully guarantee only two of: Consistency (every read sees the latest write), Availability (every request gets an answer) and Partition tolerance (works despite network splits). Since partitions happen, the real choice is consistency vs availability. Commonly: HBase leans consistency; Cassandra leans availability (tunable).
20. **HBase vs Cassandra vs MongoDB in one line each?** HBase: wide-column on HDFS, strong per-row consistency, MapReduce bulk load. Cassandra: masterless wide-column, very fast writes. MongoDB: document store, flexible schema, rich queries.
21. **Why does Hadoop use commodity hardware?** Cheap machines let you scale out; failures are expected and handled by replication, so you do not pay for special reliable hardware.

### C. Case-specific

22. **Why eBay instead of Facebook?** The syllabus case is Facebook, but we chose eBay because its engineers published detailed, citable accounts of both its relational systems and its Hadoop and NoSQL systems. The brief allows a real-world application. We replaced the Facebook leaf and still cover the same topics.
23. **Why did eBay keep Teradata?** Our sources only show it ran Teradata (10+ PB) beside Hadoop in 2010, and one analyst's 2010 report said eBay then saw Hadoop as limited for joins. The change was not either/or. We do not have eBay's own reasons, so we do not claim more.
24. **Why HBase and not MongoDB for Hadoop-linked work?** HBase runs on HDFS, so MapReduce can bulk-load it and it shares the same storage; eBay's Cassini search and Kylin OLAP both use it. MongoDB has its own storage and suits the catalogue (varied product fields). Honest caveat: the MongoDB claim is unverified.
25. **How is eBay's Hadoop use like Facebook's?** Both used Hadoop/HDFS at very large scale for analysing huge logs, and both used HBase. Both also moved from SQL-only analysis to scale-out. (Facebook points from the syllabus, see section 7.)
26. **What are the limitations of your sources?** Data is 2009-2014 and not checked against today; eBay's own slides disagree on node counts, so we use ranges; some pages are undated; the MongoDB source was found through search summaries, not read in full; the "Hadoop limited for joins" remark is one analyst's report; group names still to be added.
27. **Which eBay facts are the strongest?** The ones with a named source and year: 150 billion log records a day (Monash 2009), Greenplum 6.5 PB (2009), Teradata 10+ PB (2010), 400M+ Cassandra writes a day (Patel 2012).

### D. Tricky

28. **Is the Secondary NameNode a backup of the NameNode?** No. It periodically merges the NameNode's edit log into the file-system image (checkpointing) so the log does not grow forever. It is not a hot standby. A real standby NameNode is the HA feature in Hadoop 2.
29. **Is Hadoop a database?** No. It is a framework: HDFS is a distributed file system, YARN a resource manager, MapReduce a processing model. It has no tables, indexes or transactions by itself. HBase and Hive build database-like layers on top.
30. **Did eBay replace SQL with Hadoop?** No. In 2010 it ran Teradata (10+ PB) beside Hadoop. Hadoop was added for scale-out storage and processing; NoSQL for live features.

---

## 7. Facebook case study in 5 lines

The syllabus case is Facebook. If asked, say only these well-known points, and hedge any number (we have no Facebook figures in our sources, so do not quote any).

1. Facebook generated huge volumes of user activity and log data that its SQL warehouse could not handle economically.
2. It adopted Hadoop and HDFS and ran very large HDFS clusters.
3. It **created Hive** at Facebook so analysts could query Hadoop data with SQL-like commands.
4. Logs were collected into the cluster with **Scribe**, Facebook's log-collection tool.
5. It used **HBase** for Facebook Messages (a write-heavy, huge, real-time workload).

| Aspect | eBay (our case) | Facebook (syllabus) |
| --- | --- | --- |
| Starting point | SQL warehouses: Teradata, Greenplum | Large SQL warehouse (reported) |
| Hadoop use | HDFS, YARN, MapReduce, Hive, Spark; ran beside Teradata | Large HDFS clusters; created Hive |
| NoSQL | HBase (Cassini, Kylin), Cassandra, MongoDB | HBase for Messages |

---

## 8. Last-night checklist

**Before you sleep**
- [ ] Fill in `[Group members]` on **slide 1** and **slide 9** (the footer) with real names. The report's author line also needs names.
- [ ] Decide who speaks which part (section 4) and confirm which slide each person controls.
- [ ] Say the talk aloud 3 times with a timer. Target under 2 minutes. Cut the starred sentence if long.
- [ ] Learn the thesis sentence by heart.
- [ ] Learn these cold: 5 Vs, why RDBMS fails (3 reasons), HDFS read/write, replication, YARN parts, word count, HBase vs MongoDB.
- [ ] Open the MongoDB at eBay slides and confirm the claims, if you have 10 minutes. If not, be ready to say the row is unverified.
- [ ] Know which AI tool made the mind map and what you asked it.
- [ ] Charge laptop, keep a PDF backup of the deck.

**If you do not know an answer**
- Do not invent a number. Say: "I am not sure of the exact figure. Our sources report it as around X, and they differ by year." or "That is not covered in our sources; here is what I do know..." and give the related concept.
- Bring it back to the slide: "On slide 5 we show..."
- If a teammate knows, say "My teammate may add to this."

**1-day study plan**

| Time | Task | File |
| --- | --- | --- |
| 45 min | Read section 1 and 2 of this guide, then skim all slides once | this file |
| 90 min | Unit 1: 5 Vs, RDBMS vs distributed, life cycle | `01-unit1-big-data-basics.md` |
| 2 hrs | Unit 2: HDFS, YARN, MapReduce, NoSQL, CAP | `02-unit2-hadoop-and-nosql.md` |
| 45 min | Slide-by-slide section; say each "What to say" aloud | this file |
| 30 min | Rehearse the 2-minute talk with your group | section 4 |
| 45 min | Viva bank: cover the answers, answer aloud, check | section 6 |
| 15 min | Numbers table and Facebook five lines, then sleep | sections 5 and 7 |

Unit 3 (`03-unit3-apache-spark.md`) and Unit 4 (`04-unit4-big-data-visualization.md`) are not needed for FA1; skip them tonight.
