# eBay and Big Data: From SQL Warehouses to Hadoop and NoSQL — FA1 Case Study Report

Oct 15, 2026 · @Mayank

## Abstract

eBay outgrew its SQL-only data warehouses and moved to a mix of Hadoop and NoSQL systems. This report follows that move and maps each topic of Units 1 and 2 of Big Data Analytics onto it. Using eBay's own engineering talks and blog posts (2009 to 2014), it shows that eBay's data exhibits all five Vs; that HDFS, YARN and MapReduce gave it scale-out storage and processing; and that HBase, Cassandra and MongoDB each serve a different live feature. We recommend HBase for Hadoop-linked workloads and MongoDB for the product catalogue. Most figures are historical (2009 to 2014), and one MongoDB claim still needs checking against its source.

## 1. Introduction and objectives

This report supports Formative Assessment I (FA1), in which each group builds a mind map of Big Data and Hadoop concepts, connects them to a real-world application, and presents it in two minutes. We chose eBay because its engineering teams have published detailed accounts of both the relational systems it started with and the Hadoop and NoSQL systems it adopted.

The objectives are to:

- explain the Big Data concepts of Unit 1 (definition, importance, the 5 Vs, RDBMS versus distributed systems, frameworks, analytics life cycle, applications) using eBay as the example;
- describe the Hadoop architecture of Unit 2 (need for Hadoop, HDFS, YARN, MapReduce) and the NoSQL databases HBase, Cassandra and MongoDB as eBay uses them;
- show how these parts connect in one end-to-end system, as drawn in the mind map;
- answer the questions the presentation must cover: why Big Data matters, why Hadoop is needed, which component stores data, which NoSQL database we recommend and why, and one industry application.

The companion presentation, *ebay-bigdata-fa1.pptx*, has 10 slides. Section 6 of this report points to the slide that matches each part.

## 2. Background: eBay and its data

eBay is an online marketplace. Every search, click, bid, order and listing image produces data. In eBay's 2013 conference talk on Hadoop and HBase, the platform served 120M+ active users, 300M+ search queries a day and 350M+ items.

| Measure | Figure | Year | Source |
| --- | --- | --- | --- |
| Active users | 120M+ | c. 2013 | Hadoop and HBase @ eBay |
| Search queries a day | 300M+ | c. 2013 | Hadoop and HBase @ eBay |
| Items available | 350M+ | c. 2013 | Hadoop and HBase @ eBay |
| New log records a day (Greenplum warehouse) | 150 billion, over 50 TB ingested | 2009 | [Monash (2009)](http://www.dbms2.com/2009/04/30/ebays-two-enormous-data-warehouses/) |
| New data a day into the first large Hadoop cluster | 8 to 10 TB | 2010 | [eBay Tech Blog](https://innovation.ebayinc.com/stories/hadoop-the-power-of-the-elephant/) |

Up to about 2010 eBay's analytics ran on large relational warehouses, Teradata and Greenplum. Hadoop arrived in 2007 as a small test and grew into the main platform for data processing. NoSQL stores followed for the live features of the site.

## 3. Big Data concepts at eBay (Unit 1)

**Definition and importance.** Big Data is data too large, too fast or too varied for a single conventional database to store and process efficiently. It matters because the value is in the analysis: at eBay, search ranking, recommendations and fraud detection all depend on it.

### 3.1 The 5 Vs

| V | Meaning | eBay evidence |
| --- | --- | --- |
| Volume | How much data | 8 to 10 TB of new data a day into eBay's first large Hadoop cluster; Hadoop clusters now hold hundreds of PB |
| Velocity | How fast data arrives | 150 billion new log records a day (2009); 400M+ Cassandra writes a day |
| Variety | Many formats | Logs, clickstream, orders and images; a SQL column packed with name=value text to hold semi-structured logs |
| Veracity | How trustworthy the data is | Fraud-detection time series in Cassandra help flag fake activity |
| Value | The benefit extracted | Search (Cassini), recommendations and dashboards |

Volume and Variety strain a fixed-schema SQL warehouse the most.

### 3.2 RDBMS versus distributed systems

| Aspect | SQL warehouses (Teradata, Greenplum) | Hadoop (distributed) |
| --- | --- | --- |
| Scaling | Scale up on specialised systems | Scale out by adding cheap nodes |
| Schema | Fixed; semi-structured data fits badly | Files stored as they are; structure applied when read |
| Size at eBay | Greenplum: 6.5 PB and 17 trillion records (2009); Teradata: more than 10 PB (2010) | Clusters of thousands of nodes and hundreds of PB |
| Failure | Greenplum was replaced by Teradata mainly over reliability concerns | Every block is kept as 3 copies on different machines |
| Cost of cold data | No comparable tier described in the sources | ARCHIVE tier costs 4 times less per GB |

The two coexisted: in 2010 eBay ran Teradata beside Hadoop, so the change was not either/or. One analyst's 2010 report said eBay then saw Hadoop as limited for joins; its use grew a great deal afterwards.

### 3.3 Analytics life cycle and frameworks

The life cycle at eBay has five stages. **Collect**: searches, clicks, orders and images. **Store**: HDFS. **Process**: MapReduce, Spark and Hive jobs scheduled by YARN. **Serve**: HBase, Cassandra and MongoDB. **Act**: search, recommendations, fraud checks and dashboards.

The frameworks involved are Hadoop (HDFS, YARN, MapReduce), Hive for SQL on Hadoop, and Spark, Storm and Tez, which run alongside MapReduce on Hadoop 2. Kylin, an OLAP engine eBay built and open-sourced in 2014, builds cubes from Hive and stores them in HBase.

## 4. Hadoop architecture at eBay (Unit 2)

**Need for Hadoop.** A distributed system spreads data and work over many ordinary machines and tolerates the failure of some. eBay needed this to process petabytes: its first large cluster, Athena, went live in 2010, and its clusters now have thousands of nodes shared by thousands of users. The sections below follow the three core components.

### 4.1 HDFS: storage

HDFS splits each file into large blocks of 128 to 256 MB. The **NameNode** keeps the map of which blocks belong to which file; the **DataNodes** hold the blocks. Each block is replicated as **3 copies** on different machines, so a failed node loses no data and reads can use another copy.

eBay also added an ARCHIVE tier of storage-only machines (220 TB each) beside the compute machines. Its price per GB is four times lower, on the principle that storage without computing is cheaper than storage with computing. One cluster began with 40 PB and gained 10 PB of archive storage.

### 4.2 YARN: resource management

Hadoop 1 had limits: a maximum of about 4 to 5 thousand nodes, about 40 thousand concurrent tasks, failures that killed all jobs, and fixed slots for map and reduce tasks that left capacity idle. YARN (Hadoop 2) fixes this. The **Resource Manager** assigns cluster resources and **Node Managers** run the tasks on each machine. eBay reported about 2 times the job throughput after moving to Hadoop 2, and YARN lets Spark, Storm and Tez share the cluster with MapReduce.

### 4.3 MapReduce: processing

MapReduce processes data in two steps. **Map** splits the work and emits key-value pairs; **Reduce** combines them. For example, counting searches per query: Map emits (query, 1) for each search and Reduce adds the 1s for each query. eBay uses MapReduce to build its Cassini search indexes (full and near-real-time) and for recommendation jobs that join clickstream, transaction and search data on shared clusters.

| Component | Role | Parts | eBay evidence |
| --- | --- | --- | --- |
| HDFS | Stores data | NameNode (file map), DataNodes (blocks), 3 replicas | Hundreds of PB; ARCHIVE tier 4 times cheaper per GB |
| YARN | Schedules resources | Resource Manager, Node Managers | About 2 times job throughput on Hadoop 2 |
| MapReduce | Processes data | Map, then Reduce | Cassini index builds; recommendation jobs |

## 5. NoSQL at eBay

NoSQL databases drop the fixed relational schema and scale across many nodes. eBay uses all three databases named in the syllabus, each for a different job.

| Database | Type | eBay use | Why it fits |
| --- | --- | --- | --- |
| HBase | Wide-column store on HDFS | Cassini search indexes (built with MapReduce, near real time); dashboard summaries; storage for Kylin OLAP cubes | Fast random reads and writes, billions of updates a day, easy bulk load, direct fit with Hadoop |
| Cassandra | Wide-column, write-heavy | Social signals (like, own, want), fraud-detection time series, order and shipment tracking, mobile notification logs, server metrics; 200 TB+ and 400M+ writes and 100M+ reads a day (2012 to 2013) | Scalable counters and strong write performance; read latency was not a concern |
| MongoDB | Document store | Product metadata with varied, changing fields, bulk-loaded once a day from Hadoop | Flexible schema |

**HBase versus MongoDB.** HBase stores huge, sparse tables on HDFS and supports row-key lookups, range scans and bulk loads from MapReduce. MongoDB stores JSON-like documents with a flexible schema and rich queries, and has its own replication and sharding. HBase suits Hadoop-linked analytics; MongoDB suits catalogue-style data whose fields vary.

**Our recommendation.** For eBay's Hadoop-linked workloads, HBase, because it runs on HDFS and MapReduce can feed it directly. For the product catalogue, MongoDB. The course brief pairs e-commerce with MongoDB, HBase with healthcare and Cassandra with social media; eBay's own choices show that one company can use all three.

The MongoDB row rests on summaries of an eBay talk that we have not read in full (see section 9).

## 6. The system end to end, and the mind map

### 6.1 One pipeline for every click

1. **Collect:** searches, clicks, orders and images are logged.
2. **Store:** the data lands in HDFS, three copies per block.
3. **Process:** YARN schedules MapReduce, Hive and Spark jobs that build indexes and recommendations.
4. **Serve:** HBase, Cassandra and MongoDB feed the live site.
5. **Act:** users see search results, recommendations and fraud checks; analysts see dashboards.

### 6.2 How Hadoop grew at eBay

| When | Milestone |
| --- | --- |
| 2007 | First 4-node Hadoop cluster |
| 2009 to 2010 | Teradata and Greenplum hold the large warehouses; Athena, the first large Hadoop cluster, goes live (2010) |
| 2012 to 2013 | Thousands of nodes; tens of petabytes |
| Hadoop 2 era | YARN adopted; Spark, Storm and Tez run beside MapReduce; hundreds of PB |
| 2014 | Kylin, eBay's OLAP engine on Hadoop and HBase, open-sourced |

eBay's own slides give different node counts for the same years (for example 4,000+ nodes against 10,000 nodes in 2013 to 2014), so we show ranges instead of single numbers.

### 6.3 The mind map

The mind map puts *Big Data Analytics* at the centre with two branches. The left branch covers Unit 1: definition, evolution, the 5 Vs, characteristics, importance, RDBMS versus distributed systems, frameworks, analytics life cycle and applications. The right branch covers Unit 2: need for Hadoop, distributed systems, HDFS (NameNode, DataNode, replication), YARN (Resource Manager, Node Manager), MapReduce (Map, Reduce), NoSQL (HBase, Cassandra, MongoDB) and the eBay case study. Seven numbered cross-links tie the units together:

| # | Unit 1 concept to Unit 2 component | eBay evidence |
| --- | --- | --- |
| 1 | 5 Vs to Need for Hadoop | 8 to 10 TB a day outgrew SQL warehouses |
| 2 | Characteristics to Distributed Systems | Scale-out nodes survive growth and failures |
| 3 | RDBMS vs distributed to HDFS | Hadoop stored data beside Teradata |
| 4 | Frameworks to YARN | Hadoop 2 runs Spark, Storm and Tez |
| 5 | Analytics life cycle to MapReduce | The process stage builds search indexes |
| 6 | Applications to NoSQL | NoSQL stores serve live features |
| 7 | Applications to eBay case study | Search, recommendations, fraud checks |

### 6.4 Where each part appears in the presentation

| Slide | Content | Report section |
| --- | --- | --- |
| 1 | Title | n/a |
| 2 | The research question and eBay's scale | 1, 2 |
| 3 | The 5 Vs | 3.1 |
| 4 | SQL warehouses versus Hadoop | 3.2 |
| 5 | HDFS, YARN, MapReduce | 4 |
| 6 | HBase, Cassandra, MongoDB and our pick | 5 |
| 7 | The mind map | 6.3 |
| 8 | The seven cross-links | 6.3 |
| 9 | Conclusions | 8 |
| 10 | References | References |

## 7. Viva preparation

The FA1 brief lists eight sample questions. Short answers, using eBay, follow.

| Question | Answer |
| --- | --- |
| Explain the 5 Vs of Big Data. | Volume (how much), Velocity (how fast), Variety (formats), Veracity (trustworthiness), Value (benefit). eBay: 8 to 10 TB a day; 150 billion log records a day; logs, clicks, orders and images; fraud signals; search and recommendations. |
| Why can't an RDBMS handle Big Data efficiently? | It scales up rather than out, needs a fixed schema, and struggles with heavy joins over huge tables. eBay packed name=value text into one SQL column to hold semi-structured logs. |
| What is the role of HDFS? | Distributed storage: files are split into 128 to 256 MB blocks held on DataNodes; the NameNode keeps the map of blocks. |
| Why is replication needed? | Each block has 3 copies on different machines, so a failed node loses no data and reads can use another copy. |
| What is YARN? | The cluster's resource manager: the Resource Manager assigns resources and Node Managers run tasks. It fixed Hadoop 1 limits and lets Spark, Storm and Tez share the cluster. |
| Explain MapReduce with an example. | Counting searches per query: Map emits (query, 1) for each search; Reduce adds the 1s per query. eBay uses it to build search indexes. |
| Difference between HBase and MongoDB? | HBase is a wide-column store on HDFS with row-key lookups and bulk loads from MapReduce; MongoDB is a document store with a flexible schema and rich queries. |
| How does eBay use Hadoop? (the brief asks this about Facebook) | HDFS stores the data (hundreds of PB), YARN schedules jobs, MapReduce, Hive and Spark process it, and HBase, Cassandra and MongoDB serve the results to the site. |

The talk itself must also say which NoSQL database we recommend and why: HBase for Hadoop-linked work because it runs on HDFS, MongoDB for the catalogue because of its flexible schema.

## 8. Conclusions

1. **Big Data outgrew SQL-only warehouses.** Volume, Velocity and Variety all appear in eBay's data, and Volume and Variety strain a fixed-schema warehouse most.
2. **Hadoop added scale-out storage and processing.** HDFS stores, YARN schedules and MapReduce processes; eBay ran it beside Teradata rather than replacing it.
3. **NoSQL serves the live features.** HBase, Cassandra and MongoDB each fit a different job. We recommend HBase for Hadoop-linked work and MongoDB for the product catalogue.

## 9. Limitations and items to verify

- **Age of the data.** The sources date from 2009 to 2014. We did not check what eBay runs today, so statements are historical.
- **Inconsistent figures.** eBay's own slides give different node counts for the same years, so the report uses ranges and phrases such as "hundreds of PB".
- **MongoDB.** The MongoDB row comes from search summaries of an eBay talk (*MongoDB at eBay*). Open that source and confirm it before presenting.
- **Undated pages.** Several eBay blog pages show no date and are cited as n.d.
- **Analyst view.** The 2010 statement that eBay saw Hadoop as limited for joins is one analyst's report of eBay's views, not eBay's own wording.
- **Group details.** Group member names and the report author line still need to be added.

## References

1. Monash, C. (2009). [eBay's two enormous data warehouses](http://www.dbms2.com/2009/04/30/ebays-two-enormous-data-warehouses/). DBMS2.
2. Monash, C. (2010). [eBay followup: Greenplum out, Teradata > 10 petabytes, Hadoop has some value, and more](http://www.dbms2.com/2010/10/06/ebay-followup-greenplum-out-teradata-10-petabytes-hadoop-has-some-value-and-more/). DBMS2.
3. eBay Inc. (n.d.). [Hadoop: The power of the elephant](https://innovation.ebayinc.com/stories/hadoop-the-power-of-the-elephant/). eBay Tech Blog.
4. eBay Inc. (n.d.). [HDFS storage efficiency using tiered storage](https://innovation.ebayinc.com/stories/hdfs-storage-efficiency-using-tiered-storage/). eBay Tech Blog.
5. eBay Inc. (n.d.). [Hadoop-2 @ eBay](https://www.slideshare.net/Hadoop_Summit/hadoop2-ebay) \[Slides\]. Hadoop Summit.
6. eBay Inc. (c. 2013). [Hadoop and HBase @ eBay](https://www.slideshare.net/slideshow/ma-june27-140pmroom212v2/24108965) \[Slides\].
7. Patel, J. (2012). [Cassandra at eBay](https://www.slideshare.net/jaykumarpatel/cassandra-at-ebay-13920376) \[Slides\]. Cassandra Summit 2012.
8. eBay Inc. (2014). [Announcing Kylin: Extreme OLAP engine for big data](https://innovation.ebayinc.com/stories/announcing-kylin-extreme-olap-engine-for-big-data/). eBay Tech Blog.
9. MongoDB Inc. (n.d.). [MongoDB at eBay](https://www.slideshare.net/mongodb/mongodb-at-ebay) \[Slides\]. (Found through search summaries; not read in full.)

The course material (*FA 1: AI-Based Mind Map on Big Data and Hadoop Ecosystem*) supplied the syllabus topics and the brief's examples.
