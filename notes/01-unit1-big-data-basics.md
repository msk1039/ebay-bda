# Unit 1: Introduction to Big Data

How to use these notes: each topic follows the same shape (one line, explain it like I'm new, how it works, example, remember). The running example is **eBay** from the FA1 deck. Sibling files: `02-unit2-hadoop-and-nosql.md` (HDFS, YARN, MapReduce, NoSQL), `05-fa1-ebay-presentation-guide.md` (the talk).

eBay figures in these notes come from the FA1 deck and its sources (Monash 2009/2010; eBay Tech Blog and conference slides c. 2013; Patel 2012). They are company-reported, dated, and not always consistent with each other, so treat them as "around" numbers.

---

## Concepts of Big Data

### Definition of Big Data

**In one line:** Big Data is data so large, fast-arriving or varied that a single traditional database and a single machine cannot store, process or analyse it economically, so it needs distributed tools.

**Explain it like I'm new.** Imagine a school library with 5,000 books and one librarian. She can find any book, keep a register, and answer questions. Now imagine a library receiving 10 lakh new books every day, in many languages, some as photos of handwritten pages, and everyone wants answers in seconds. One librarian and one building will fail no matter how hard she works. You need many buildings, many librarians, and a different way to organise them. That is the Big Data situation.

"Big" is not a fixed number of terabytes. It is relative: data becomes Big Data when your existing tools (a single relational database server, Excel, one PC) stop coping. A commonly quoted textbook definition is Gartner's (Doug Laney's 3Vs, 2001, later reworded by Gartner): high-volume, high-velocity and/or high-variety information assets that demand cost-effective, innovative forms of information processing for better insight and decision-making.

**How it works (the core idea).**

```text
Too much data for one machine
        |
        v
Split the data into pieces  ->  spread pieces over many cheap machines (a cluster)
        |
        v
Send the computation to the pieces (each machine works on its own part)
        |
        v
Combine partial results  ->  answer
```

**Example (eBay).** eBay has 300M+ searches a day and, as reported in 2009, its log warehouse took in about 150 billion new records a day. No single database server can hold and query that, so eBay added Hadoop clusters beside its SQL warehouses.

**Remember**
- Big Data is defined by what tools can handle, not by one fixed size.
- The classic definition uses the Vs (3 originally, 5 in most syllabi).
- The solution pattern is always: split, spread, process in parallel, combine.
- Typical sources: social media, sensors/IoT, logs, transactions, clicks, images, video.

**Common confusion.** "Big Data just means a lot of data." Correction: volume is only one dimension. A modest amount of messy, fast, mixed-format data (say, live clickstream plus images plus text) can be a Big Data problem too.

---

### Evolution of Big Data era

**In one line:** Data handling moved from files to relational databases to warehouses, and then, once web companies outgrew those, to distributed systems (GFS/MapReduce, Hadoop, NoSQL, Spark) and now cloud, streaming and AI.

**Explain it like I'm new.** Each era solved the previous era's pain. Files were hard to query, so we got databases. Databases were bad at long-term analysis across the business, so we got warehouses. The web created data too big for any warehouse, so Google, then the open-source world, invented distributed storage and processing. Each step below is "the problem, and the answer".

**How it works (timeline).**

```text
1960s-70s   Flat files / file systems         -> hard to search, duplicate, inconsistent
1970s-80s   RDBMS + SQL                       -> Codd's relational model (1970); SQL; Oracle,
                                                 DB2; ANSI SQL standard 1986. Structured data,
                                                 ACID transactions
Late 80s-90s Data warehouses (OLAP, ETL)      -> Teradata etc.; integrate data from many
                                                 systems for reporting (Inmon, Kimball)
Late 90s-00s Web scale                        -> Google, Yahoo, Amazon, eBay outgrow single
                                                 servers; Laney's 3Vs (2001)
2003        Google File System (GFS) paper    -> distributed file system on cheap machines
2004        Google MapReduce paper            -> simple parallel processing model
2006        Hadoop (Doug Cutting, Yahoo)      -> open-source HDFS + MapReduce
2006-2009   NoSQL wave                        -> Google Bigtable (2006), Amazon Dynamo (2007);
                                                 HBase, Cassandra, MongoDB
2009        Spark starts at UC Berkeley       -> in-memory processing; Apache incubator 2013,
                                                 top-level Apache project 2014
2010s-now   Cloud, streaming, AI era          -> cloud object storage and managed clusters,
                                                 Kafka/Flink streaming, ML and generative AI
```

Notes on the timeline: dates are the commonly cited ones (papers 2003/2004; Hadoop 2006; Spark 2009 research project, Apache 2013-14). Kafka (from LinkedIn) and Flink arrived in the 2010s.

**Example (eBay).** eBay's own story follows the timeline: relational warehouses first (Greenplum, Teradata), then Hadoop for the data those could not hold economically, then NoSQL stores (HBase, Cassandra, MongoDB) for live features. In 2010 it ran Teradata (10+ PB) beside Hadoop; it did not abandon SQL.

**Remember**
- GFS 2003 and MapReduce 2004 are Google papers; Hadoop (2006) is the open-source implementation of those ideas.
- Spark: 2009 Berkeley, Apache 2013-14.
- RDBMS/SQL dates to the 1970s; the 1970s-80s is when it became commercial.
- Each era is a reaction to the limits of the previous one.

**Common confusion.** "Hadoop was invented by Google." Correction: Google published the ideas (GFS, MapReduce papers); Hadoop is an independent open-source project started by Doug Cutting (then at Yahoo).

---

### 5 Vs of Big Data

**In one line:** The 5 Vs are Volume (how much), Velocity (how fast), Variety (how many formats), Veracity (how trustworthy) and Value (how useful).

**Explain it like I'm new.** Think of a restaurant kitchen. Volume: how many orders arrive. Velocity: how quickly they arrive and must be served. Variety: vegetarian, vegan, allergy, custom orders, all different. Veracity: is the order slip legible and correct? Value: does serving all this actually earn money? A kitchen strained on several of these at once needs a different design, not just a bigger stove.

**How it works (each V in detail).**

**1. Volume** is the size of the data. Units go up by about 1,000 (or 1,024 in binary use) each step:

| Unit | Symbol | Roughly equals |
|---|---|---|
| Kilobyte | KB | a short text message / small email |
| Megabyte | MB | a photo or an MP3 song |
| Gigabyte | GB | a movie (HD) |
| Terabyte | TB | a laptop hard disk |
| Petabyte | PB | 1,000 TB; a large company's warehouse |
| Exabyte | EB | 1,000 PB |
| Zettabyte | ZB | 1,000 EB; global data is measured in ZB |

Order to memorise: KB -> MB -> GB -> TB -> PB -> EB -> ZB (each ~1,000x the last; 1 ZB = 10^21 bytes).

**2. Velocity** is the speed at which data is generated and must be processed. Two modes: **batch** (collect, then process later, e.g. nightly reports) and **real time / streaming** (process as it arrives, e.g. fraud checks while the payment happens).

**3. Variety** is the range of formats. Data comes in three types:

| Type | Meaning | Examples |
|---|---|---|
| **Structured** | Fixed schema, rows and columns | Orders table, bank transactions, student marks in an RDBMS |
| **Semi-structured** | Has tags/keys giving some structure but no rigid table | JSON, XML, CSV with variable fields, server logs, name=value text |
| **Unstructured** | No predefined model | Images, video, audio, emails, free-text reviews, PDFs |

Most new data (often estimated at the large majority) is semi- or unstructured, which is why a rigid-schema database struggles.

**4. Veracity** is the quality and trustworthiness: missing values, duplicates, bias, noise, fake data. Bad data gives bad decisions ("garbage in, garbage out"). Handling it is the cleaning/preparation stage.

**5. Value** is the business benefit actually extracted. Data that is stored but never analysed has no value. Value is the point of the whole exercise and is often called the most important V.

**Example (eBay) - slide 3 of the deck.**

| V | eBay evidence (from the deck) |
|---|---|
| Volume | 8-10 TB of new data a day into eBay's first large Hadoop cluster |
| Velocity | 150 billion new log records a day (2009); 400M+ Cassandra writes a day |
| Variety | Logs, clicks, orders, images; SQL column packed with name=value text |
| Veracity | Fraud-detection time series in Cassandra help flag fake activity |
| Value | Search, recommendations and dashboards turn data into sales |

Slide's "so what": Volume and Variety strain a fixed-schema SQL warehouse most.

**Remember**
- 5 Vs: Volume, Velocity, Variety, Veracity, Value.
- Variety has three kinds: structured, semi-structured, unstructured.
- Units: KB, MB, GB, TB, PB, EB, ZB.
- Original model (Laney, 2001) had only the first 3 Vs.
- Veracity is about trust, Value is about benefit; don't swap them.

**Common confusion.** "Velocity means the data is big." Correction: velocity is speed of arrival/processing; volume is size. 150 billion records a day is both high volume and high velocity, but they are different properties.

---

### Importance of Big Data Analytics

**In one line:** Big Data analytics is examining huge, varied datasets to find patterns and insight that support better decisions, and it matters because it turns raw data into revenue, savings and new services.

**Explain it like I'm new.** Collecting data is like collecting fuel; analytics is the engine. The same dataset can answer four increasingly useful questions, which gives the four types of analytics.

**How it works (the four types).**

```text
Descriptive    -> What happened?          (reports, dashboards)         easiest, hindsight
Diagnostic     -> Why did it happen?      (drill-down, root cause)
Predictive     -> What will happen?       (ML models, forecasting)
Prescriptive   -> What should we do?      (optimisation, recommendations) hardest, most value
```

| Type | Question | Typical technique | eBay-style example |
|---|---|---|---|
| Descriptive | What happened? | Aggregations, dashboards | Searches per day by category |
| Diagnostic | Why? | Drill-down, correlation | Why did conversions drop in one category last week? |
| Predictive | What will happen? | Regression, classification | Predict which listings will sell; flag likely fraud |
| Prescriptive | What should we do? | Optimisation, rules + ML | Recommend price or ranking changes; block a suspicious order |

**Business benefits:**
- Better, faster decisions based on evidence instead of guesswork.
- Personalisation: recommendations and targeted offers raise sales.
- Cost reduction: efficient operations, predictive maintenance, cheaper storage tiers.
- Risk and fraud reduction: detect abnormal behaviour early.
- New products and revenue: data-driven services.
- Better customer experience: understand behaviour and sentiment.

**Example (eBay).** Search, recommendations and fraud detection (deck slides 2, 8) are all analytics: counting searches (descriptive), spotting what changed (diagnostic), predicting fraud (predictive) and acting on it (prescriptive).

**Remember**
- Four types in order of value and difficulty: descriptive, diagnostic, predictive, prescriptive.
- Questions: what happened, why, what will happen, what to do.
- Analytics creates the "Value" V.

---

### Characteristics of Big Data

**In one line:** Characteristics are the defining properties of Big Data; textbooks list them as 3, 5, 7 or even 10 Vs, and the extra ones beyond the 5 Vs are variability, visualization, volatility and validity.

**Explain it like I'm new.** "Characteristics" and "the Vs" overlap heavily; many books use the words interchangeably. Different authors simply stop the list at different points. When a question says "characteristics", give the 5 Vs and then mention the extras to score fully. Always state which list you are using.

**How it works (the lists).**

| List | Vs included | Where it is used |
|---|---|---|
| **3 Vs** | Volume, Velocity, Variety | Original (Laney, 2001), Gartner |
| **5 Vs** | + Veracity, Value | Most Indian university syllabi, including this one |
| **7 Vs** | + Variability, Visualization | Common in textbooks and tutorials |
| **10 Vs** | + Validity, Volatility, Vulnerability (varies by source) | Some blogs and review papers |

Definitions of the extras:
- **Variability**: the meaning or flow of data changes over time, e.g. the word "sick" in slang vs medical text, or traffic that spikes at festival sales. Different from Variety (formats).
- **Visualization**: Big Data must be presented in charts, dashboards and maps people can understand (see Unit 4).
- **Validity**: is the data correct and appropriate for its intended use? Close to Veracity.
- **Volatility**: how long data stays valid and must be retained; some data is useful for seconds (live clicks), some for years (orders).
- **Vulnerability**: security and privacy risk of holding sensitive data (listed in some 10-V lists).

Other non-V characteristics often stated: data is generated continuously; usually distributed across many nodes; often schema-less; processed in parallel; needs scale-out hardware.

**Example (eBay).** Variability: traffic jumps at sale events or holidays. Volatility: live click data matters for minutes, but order history must be kept for years, which is why eBay moved old data to cheaper, 4x cheaper nodes (deck slide 4).

**Remember**
- 3, 5, 7 or 10 Vs depending on the book; the first five are the safe core.
- Variability is meaning/flow changing; Variety is formats.
- Validity is similar to Veracity; Volatility is data lifetime.
- State your list at the start of a viva answer.

**Common confusion.** "Variety and Variability are the same." Correction: Variety = different formats at one time; Variability = the same data changing in meaning or volume over time.

---

### Traditional RDBMS vs Distributed systems

**In one line:** A traditional RDBMS keeps structured data on one big server with a fixed schema and strict ACID guarantees, while a distributed system spreads data and work over many cheap machines (scale-out), is flexible with schema, and tolerates failures.

**Explain it like I'm new.** Define each term first.
- **RDBMS**: a database that stores data in tables with rows and columns and is queried with SQL (MySQL, Oracle, PostgreSQL, Teradata).
- **Distributed system**: many computers (nodes) working together as one system, connected by a network.
- **Scale-up (vertical)**: make one machine bigger (more CPU, RAM, disk). Expensive and has a ceiling. Like replacing a bus with a bigger bus.
- **Scale-out (horizontal)**: add more ordinary machines. Cheap per unit and nearly unlimited. Like adding more buses.
- **Schema-on-write**: you must define the table structure first; data is checked and shaped as it is written (RDBMS).
- **Schema-on-read**: store raw data as-is and apply structure only when you read/query it (Hadoop, data lakes).
- **ACID**: Atomicity (all or nothing), Consistency (rules always hold), Isolation (transactions don't interfere), Durability (committed data survives crashes). Safe, but costly to guarantee across many machines.
- **BASE**: Basically Available, Soft state, Eventually consistent. Accept brief inconsistency across copies to stay available and fast. Typical of many NoSQL systems.
- **CAP theorem**: in a distributed system holding replicated data, during a network **P**artition you can guarantee only one of **C**onsistency (every read sees the latest write) or **A**vailability (every request gets a response). Partitions cannot be ruled out in practice, so the real choice is C vs A. Rough guide: traditional RDBMS favour consistency; systems like Cassandra favour availability (tunable); HBase favours consistency.

**How it works.**

```text
Scale-up (RDBMS)                      Scale-out (distributed)

  +-----------+                       +----+ +----+ +----+ +----+
  |  one big  |   data grows ->       |node| |node| |node| |node| + more nodes
  |  server   |   buy bigger server   +----+ +----+ +----+ +----+
  +-----------+   (cost jumps, limit)   each holds a slice + copies of others
```

Distributed systems cope with failure by **replication** (several copies of each piece on different nodes) so one dead machine loses nothing.

**Big comparison table.**

| Aspect | Traditional RDBMS / SQL warehouse | Distributed system (Hadoop / NoSQL) |
|---|---|---|
| Scaling | Scale-up (bigger server) | Scale-out (more commodity nodes) |
| Hardware cost | High-end, costly servers/appliances | Cheap commodity hardware |
| Data types | Mainly structured | Structured, semi-structured, unstructured |
| Schema | Schema-on-write (fixed up front) | Schema-on-read (or flexible schema) |
| Consistency model | ACID | Often BASE (eventual consistency); varies by system |
| CAP leaning | Consistency | Varies: availability (Cassandra) or consistency (HBase) |
| Query language | SQL | MapReduce, Spark, HiveQL (SQL-like), NoSQL APIs |
| Fault tolerance | Needs backups/standby; node failure is serious | Built-in replication (e.g. HDFS keeps 3 copies); failure is normal |
| Joins and transactions | Strong, efficient | Heavy joins are slow; limited multi-row transactions |
| Best for | OLTP, banking, reports on moderate data | Huge data, logs, analytics, flexible data |
| Data movement | Data moves to the query | Computation moves to the data |
| Typical size | GBs to low TBs per server | TBs to PBs across a cluster |
| Cost of growth | Rises sharply | Roughly linear (add nodes) |

**Example (eBay) - slide 4 of the deck.**

| | SQL warehouses | Hadoop |
|---|---|---|
| eBay fact | Greenplum: 6.5 PB, 17 trillion records | Add cheap nodes to scale out |
| Variety | Semi-structured logs packed into one text column | Stores raw data, structure applied on read |
| Reliability | Greenplum replaced by Teradata over reliability (reported, Monash 2010) | 3 copies of every block |
| Cost | Costly appliances | Old data on 4x cheaper nodes |

The deck's key message: it is **not either/or**. In 2010 eBay ran Teradata (10+ PB) beside Hadoop; SQL stays good for joins and structured reporting.

**Remember**
- Scale-up = bigger machine; scale-out = more machines.
- Schema-on-write (RDBMS) vs schema-on-read (Hadoop).
- ACID (strict) vs BASE (relaxed, eventual consistency).
- CAP: under a partition choose consistency or availability.
- Three reasons RDBMS strains (viva answer): scale-up cost/ceiling, fixed schema, slow heavy joins at huge size.

**Common confusion.** "NoSQL means no SQL at all and RDBMS is obsolete." Correction: NoSQL is usually read as "not only SQL"; RDBMS is still the right tool for transactions and structured data, and companies like eBay run both.

---

## Big Data Ecosystem

### Introduction to Big Data frameworks

**In one line:** A Big Data framework is a set of tools that together ingest, store, process, query and coordinate data across a cluster; the main split is batch processing (large stored chunks, high latency) versus stream processing (continuous, low latency).

**Explain it like I'm new.** Think of a postal system. Collection boxes (ingestion), sorting warehouses (storage), sorting machines (processing), enquiry counter (query), and a control room (coordination). No single tool does everything, so the "ecosystem" is a stack of tools, each good at one layer.

**Batch vs stream.**

| Aspect | Batch processing | Stream processing |
|---|---|---|
| Data | Large bounded set stored first | Unbounded, arriving continuously |
| Latency | Minutes to hours | Milliseconds to seconds |
| Example | Nightly sales report, building search indexes | Live fraud alert, real-time click counts |
| Tools | Hadoop MapReduce, Spark (batch), Hive | Kafka (transport), Storm, Flink, Spark Streaming |
| Trade-off | High throughput, simple | Fast answers, more complex |

Many companies use a mix of both (often called lambda or similar architectures).

**How it works (layers and tools).**

```text
Sources -> INGESTION -> STORAGE -> PROCESSING -> QUERY/SERVE -> Users/Apps
 (logs,    Kafka,       HDFS,       MapReduce,    Hive, Pig,      dashboards,
 clicks,   Flume,       HBase,      Spark, Flink, Impala, SQL     search, ML
 orders)   Sqoop        Cassandra   Storm, Tez                    models
                     COORDINATION & RESOURCES: ZooKeeper, YARN
```

| Layer | Tool | One-line role |
|---|---|---|
| Ingestion | **Apache Kafka** | Distributed message log for real-time data streams |
| Ingestion | **Flume** | Collects and moves log data into Hadoop |
| Ingestion | **Sqoop** | Transfers bulk data between RDBMS and Hadoop |
| Storage | **HDFS** | Distributed file system; blocks replicated across nodes |
| Storage | **HBase** | Wide-column NoSQL database on HDFS for random read/write |
| Storage | **Cassandra / MongoDB** | NoSQL stores: fast-write wide-column / flexible documents |
| Processing | **Hadoop MapReduce** | Batch model: Map splits the work, Reduce combines results |
| Processing | **Apache Spark** | Fast in-memory batch, SQL, streaming and ML engine |
| Processing | **Apache Flink** | Stream-first engine for low-latency continuous processing |
| Processing | **Apache Storm** | Real-time stream processing (earlier generation) |
| Processing | **Tez** | Faster execution engine for Hive/Pig jobs on YARN |
| Query | **Hive** | SQL-like queries (HiveQL) over data in HDFS |
| Query | **Pig** | Scripting language (Pig Latin) for data-flow transformations |
| Coordination | **YARN** | Cluster resource manager; schedules jobs (Hadoop 2+) |
| Coordination | **ZooKeeper** | Coordination service: configuration, naming, leader election |

Which role is "the" job of each tool is standard textbook material; Units 2 and 3 cover HDFS/YARN/MapReduce and Spark in depth (`02-unit2-hadoop-and-nosql.md`, `03-unit3-apache-spark.md`).

**Example (eBay).** The deck maps eBay to this stack: HDFS stores (hundreds of PB, reported), YARN schedules, MapReduce/Hive/Spark process, and HBase, Cassandra and MongoDB serve live features. Hadoop 2 lets Spark, Storm and Tez share the cluster with MapReduce (deck slides 5 and 8, link 4).

**Remember**
- Batch = big, stored, slower; stream = continuous, fast.
- Five layers: ingestion, storage, processing, query, coordination.
- Hadoop = HDFS + YARN + MapReduce; the ecosystem is everything around it.
- Spark is faster than MapReduce mainly because it keeps data in memory.
- Kafka moves data; it does not analyse it.

**Common confusion.** "Hadoop and Spark are competitors." Correction: Spark commonly runs on top of Hadoop (HDFS storage, YARN scheduling) and replaces only the MapReduce processing part.

---

### Big Data Analytics Life Cycle

**In one line:** The analytics life cycle is the sequence of stages from defining a business problem to collecting and cleaning data, analysing it, communicating results and putting the solution into production.

**Explain it like I'm new.** It is a recipe: decide what dish (problem), buy ingredients (acquire data), wash and chop (clean and prepare), cook (model), plate it (visualize), and serve it regularly (deploy). It is iterative: you often loop back, e.g. finding bad data sends you back to acquisition.

**How it works.** Two versions are commonly taught. Note on naming: the 9-stage version is from Thomas Erl's *Big Data Fundamentals*; the EMC/Dell (now Dell Technologies) Data Analytics Lifecycle has 6 phases. Textbooks and slides mix these up, so know both.

**Version A: 9-stage life cycle (Erl)**

| # | Stage | What happens |
|---|---|---|
| 1 | Business case evaluation | Define the problem, goals, budget; is analytics justified? |
| 2 | Data identification | Find the datasets and sources needed (internal and external) |
| 3 | Data acquisition and filtering | Collect data; discard corrupt or irrelevant records |
| 4 | Data extraction | Pull out needed fields from varied formats (e.g. parse JSON/logs) |
| 5 | Data validation and cleansing | Fix errors, remove duplicates, handle missing values |
| 6 | Data aggregation and representation | Combine datasets into a common, analysable form |
| 7 | Data analysis | Apply statistics/ML to find patterns (descriptive to predictive) |
| 8 | Data visualization | Present results in charts and dashboards |
| 9 | Utilization of analysis results | Use the findings in decisions, products and processes |

**Version B: 6-phase EMC/Dell Data Analytics Lifecycle**

| # | Phase | What happens |
|---|---|---|
| 1 | Discovery | Understand domain, frame business problem, form hypotheses, assess resources |
| 2 | Data preparation | Build a sandbox, extract/load/transform (ETLT), clean and condition data |
| 3 | Model planning | Explore data, select variables and candidate methods |
| 4 | Model building | Train, test and tune models on datasets |
| 5 | Communicate results | Compare outcomes with success criteria; report findings and business value |
| 6 | Operationalize | Deploy to production, pilot, monitor, maintain |

**Simple flow to remember (7 steps used in many classes):** Business problem -> data identification and acquisition -> data preparation and cleaning -> exploration and model planning -> model building and analysis -> communicate results and visualize -> operationalize (deploy and monitor).

**Mapping to eBay's pipeline (Collect -> Store -> Process -> Serve -> Act):**

| eBay pipeline stage | Life cycle stages covered (Version A / B) | eBay example |
|---|---|---|
| Business problem | A1 / B1 Discovery | Improve search relevance; reduce fraud |
| **Collect** | A2-A3 / B2 start | Logs, clicks, orders, images from the site |
| **Store** | A3-A4 / B2 | HDFS holds raw data; HBase/Cassandra/MongoDB hold serving data |
| **Process** | A4-A7 / B2-B4 | MapReduce, Hive, Spark clean data and build search indexes (deck link 5) |
| **Serve** | A8 / B5 | NoSQL stores and dashboards expose results to users and apps |
| **Act** | A9 / B6 | Search, recommendations, fraud checks acting on live data |

```text
Collect -> Store -> Process -> Serve -> Act
  |          |         |         |        |
 acquire   HDFS    clean+model  HBase/   recommendations,
 & filter           MapReduce   Cassandra fraud flags,
                    Hive/Spark  MongoDB  search results
```

Deck link: the mind map connects **Analytics Life Cycle -> MapReduce** (link 5): "Process stage builds search indexes."

**Remember**
- 9 stages (Erl) vs 6 phases (EMC/Dell): different books, same idea.
- Data preparation usually takes the largest share of effort in real projects.
- Cycle is iterative, not strictly linear.
- eBay mapping: Collect, Store, Process, Serve, Act.
- Ends with operationalize/utilize, not with a model sitting on a laptop.

**Common confusion.** "The life cycle ends when the model is built." Correction: results must be communicated and then deployed and monitored (operationalised); otherwise no value is created.

---

### Big Data Applications

**In one line:** Big Data is applied wherever organisations generate large, varied data and benefit from faster or better decisions: e-commerce, healthcare, banking, telecom, social media, smart cities, government and agriculture.

**Explain it like I'm new.** The pattern repeats: collect lots of data about people or things, find patterns, act earlier or more precisely. The table gives a standard example, the main data type involved and the benefit.

| Domain | Example use | Main data type | Benefit |
|---|---|---|---|
| **E-commerce (eBay)** | Search ranking, recommendations, fraud detection, pricing | Clickstream/logs (semi-structured), orders (structured), images (unstructured) | More sales, better matches, fewer fraudulent orders |
| **Healthcare** | Patient records analysis, disease prediction, medical imaging, wearables | Structured records, images (X-ray/MRI), sensor streams | Earlier diagnosis, personalised treatment, lower cost |
| **Banking and finance** | Fraud detection, credit scoring, risk and algorithmic monitoring | Structured transactions, streaming events | Lower fraud losses, faster loan decisions |
| **Telecom** | Churn prediction, network optimisation, call-detail record analysis | Call/usage records, logs, location data | Retain customers, fewer outages, better planning |
| **Social media** | Feeds, ad targeting, sentiment analysis, trend detection | Text, images, video, graphs of connections | Engagement, relevant ads, early trend detection |
| **Smart cities / transport** | Traffic prediction, route optimisation, public transport planning | Sensor/IoT streams, GPS, camera video | Less congestion, efficient services |
| **Government** | Aadhaar-scale identity, tax analysis, welfare targeting, census | Structured demographic and biometric records at national scale | Identity verification, reduced leakage and duplicates |
| **Agriculture** | Crop yield prediction, precision irrigation, weather and soil analysis | Satellite images, sensor and weather data | Higher yield, saved water and inputs |

Note on Aadhaar: India's UIDAI has reported enrolling well over a billion residents (figure varies by year), making it one of the largest biometric identity databases in the world; exact current numbers depend on the UIDAI report you cite.

**Example (eBay) - deck links 6 and 7.** E-commerce is the industry application in the deck: search, recommendations, fraud checks (link 7), with NoSQL stores serving these live features (link 6). Data flow per feature: clicks and logs (Velocity, Variety) -> Hadoop processing -> HBase/Cassandra/MongoDB -> results on the website.

**Remember**
- Know one example per domain and name the data type.
- E-commerce uses all three data types (structured orders, semi-structured logs, unstructured images).
- Fraud detection is the classic velocity-heavy use case (banking and e-commerce).
- Real-time uses need streaming tools; reporting uses batch tools.

---

## Quick revision

- **Big Data**: data too large, fast or varied for traditional single-machine tools; handled by distributed clusters.
- **Evolution**: files -> RDBMS/SQL (1970s-80s) -> data warehouses -> GFS (2003) + MapReduce (2004) -> Hadoop (2006) -> NoSQL -> Spark (2009 Berkeley; Apache 2013-14) -> cloud, streaming, AI.
- **5 Vs**: Volume (size), Velocity (speed), Variety (formats), Veracity (trust), Value (benefit).
- **Data types**: structured (tables), semi-structured (JSON, XML, logs), unstructured (images, video, text).
- **Units**: KB -> MB -> GB -> TB -> PB -> EB -> ZB.
- **Analytics types**: descriptive (what), diagnostic (why), predictive (what next), prescriptive (what to do).
- **Extra characteristics**: variability, visualization, validity, volatility; books list 3, 5, 7 or 10 Vs.
- **RDBMS vs distributed**: scale-up vs scale-out; schema-on-write vs schema-on-read; ACID vs BASE; high vs low cost; backups vs built-in replication.
- **CAP**: during a partition choose consistency or availability.
- **Batch vs stream**: stored/slow/throughput vs continuous/fast/low latency.
- **Layers**: ingestion (Kafka, Flume, Sqoop), storage (HDFS, HBase, Cassandra), processing (MapReduce, Spark, Flink, Storm), query (Hive, Pig), coordination (YARN, ZooKeeper).
- **Life cycle**: 9 stages (Erl) or 6 phases (EMC/Dell); simple flow problem -> data -> prepare -> model -> communicate -> deploy.
- **eBay mapping**: Collect -> Store -> Process -> Serve -> Act.
- **eBay facts (reported)**: 120M+ users, 300M+ searches/day (c. 2013); 8-10 TB/day into first Hadoop cluster; 150 billion log records/day (2009); Greenplum 6.5 PB; Teradata 10+ PB beside Hadoop (2010).
- **Applications**: e-commerce, healthcare, banking/fraud, telecom, social media, smart cities, government, agriculture.

---

## Viva questions

**1. What is Big Data?**
Data whose volume, speed or variety is too great for traditional single-machine databases to store and process economically, so it needs distributed tools. It is defined by the limits of the tools, not by a fixed size.

**2. Name and explain the 5 Vs.**
Volume is how much data; Velocity is how fast it arrives and must be processed; Variety is the many formats; Veracity is how trustworthy or clean it is; Value is the business benefit extracted. eBay example: 8-10 TB a day, 150 billion log records a day, logs and images, fraud time series, and search/recommendation sales.

**3. What are structured, semi-structured and unstructured data? Give examples.**
Structured data has a fixed schema in rows and columns (orders table). Semi-structured has tags or keys but no rigid table (JSON, XML, logs). Unstructured has no predefined model (images, video, free text).

**4. List the data units from KB upward.**
KB, MB, GB, TB, PB, EB, ZB; each step is about 1,000 (or 1,024) times the previous one.

**5. Briefly describe how Big Data evolved.**
Files, then RDBMS and SQL in the 1970s-80s, then data warehouses, then web-scale problems led Google to publish GFS (2003) and MapReduce (2004), Hadoop followed in 2006, then NoSQL, Spark (2009 Berkeley, Apache 2013-14), and now cloud, streaming and AI.

**6. What are the four types of analytics?**
Descriptive (what happened), diagnostic (why), predictive (what will happen) and prescriptive (what should we do). Value and difficulty increase in that order.

**7. Why is Big Data analytics important?**
It gives evidence-based decisions, personalisation, cost savings, fraud and risk detection, new products, and better customer experience. It converts stored data into Value, the fifth V.

**8. Are the characteristics of Big Data the same as the Vs?**
They overlap and many books use the terms interchangeably. Lists vary: 3 Vs (volume, velocity, variety), 5 Vs (add veracity, value), 7 Vs (add variability, visualization), and 10 Vs in some sources (adding validity, volatility, vulnerability). The 5 Vs are the safe core for this syllabus.

**9. Difference between Variety and Variability?**
Variety is the range of formats at a given time (structured, semi-, unstructured). Variability is the change in meaning or flow of data over time, such as seasonal traffic spikes or words changing meaning.

**10. Why can't an RDBMS handle Big Data efficiently? (deck slide 4)**
Three reasons: it scales up on bigger, costlier machines and hits a ceiling; it needs a fixed schema so logs and images fit badly (eBay packed name=value text into one SQL column); and heavy joins over huge tables are slow. Distributed systems scale out on commodity nodes and tolerate failure.

**11. Explain scale-up vs scale-out.**
Scale-up adds CPU, RAM or disk to one machine: simple but expensive with a hard limit. Scale-out adds more ordinary machines: cheaper, nearly unlimited, and fault tolerant but needs distributed software.

**12. What is schema-on-write vs schema-on-read?**
Schema-on-write defines the structure before loading and validates data as it is written (RDBMS). Schema-on-read stores raw data and applies structure at query time (Hadoop, data lakes), which suits varied data.

**13. Compare ACID and BASE.**
ACID (Atomicity, Consistency, Isolation, Durability) gives strict transactional guarantees, typical of RDBMS. BASE (Basically Available, Soft state, Eventual consistency) relaxes consistency to stay available and scalable, typical of many NoSQL systems.

**14. State the CAP theorem.**
In a distributed system with replicated data, when a network partition occurs you can have either consistency or availability, not both. Since partitions are unavoidable, systems lean one way: HBase towards consistency, Cassandra towards availability (tunable).

**15. Did eBay replace SQL with Hadoop?**
No. In 2010 eBay ran Teradata (10+ PB) beside Hadoop, per the deck's sources. Hadoop added cheap scale-out storage and processing; SQL warehouses remained useful for structured reporting and joins. It was reported that Greenplum was replaced by Teradata over reliability, not by Hadoop.

**16. Difference between batch and stream processing? Give tools.**
Batch processes large stored data with higher latency (MapReduce, Spark batch, Hive); stream processes data continuously as it arrives with low latency (Kafka as transport, Storm, Flink, Spark Streaming). Fraud alerts need streaming; building search indexes is batch.

**17. Name the Big Data ecosystem layers with a tool for each.**
Ingestion (Kafka, Flume, Sqoop), storage (HDFS, HBase, Cassandra), processing (MapReduce, Spark, Flink, Storm), query (Hive, Pig), coordination (YARN, ZooKeeper).

**18. Explain the Big Data analytics life cycle.**
It moves from business problem and data identification, to acquisition, cleaning and preparation, exploration and model planning, model building and analysis, communicating results with visualization, and finally operationalising and monitoring. The Erl version has 9 stages and the EMC/Dell version has 6 phases (discovery, data preparation, model planning, model building, communicate results, operationalize).

**19. Map the life cycle to eBay's pipeline.**
Collect is acquisition of logs, clicks and orders; Store is HDFS and NoSQL; Process is cleaning and analysis in MapReduce, Hive or Spark (for example building search indexes); Serve is delivering results through HBase, Cassandra, MongoDB or dashboards; Act is search, recommendations and fraud checks.

**20. Give applications of Big Data with data types.**
E-commerce (logs, orders, images: recommendations and fraud), healthcare (records and imaging: early diagnosis), banking (transactions: fraud detection), telecom (call records: churn prediction), social media (text and media: sentiment and ads), smart cities (sensor streams: traffic), government (Aadhaar-scale identity records), agriculture (satellite and sensor data: yield prediction).
