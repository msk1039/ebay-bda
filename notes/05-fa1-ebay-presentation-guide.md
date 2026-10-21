# FA1 eBay presentation guide

Read this with the deck (`../ebay-bigdata-fa1.pptx`, 15 slides) open beside it. Every slide's speaker notes start with **SAY:**, which is a script you can read aloud.

---

## 1. The story in 60 seconds

1. eBay collects huge amounts of data: every search, click, bid and photo.
2. By 2009 its SQL warehouses were struggling. They held too many copies of the data, their designs were rigid, logs didn't fit into tables, and they were expensive to grow.
3. eBay **did not throw SQL away.** It kept Teradata for clean, structured data that thousands of analysts query, and added **Hadoop** for everything else (raw logs, images, text).
4. Inside Hadoop:
   - **HDFS** stores the data.
   - **YARN** shares the machines between jobs.
   - **MapReduce** processes the data, for example building the index behind eBay's search.
5. **NoSQL** databases (HBase, Cassandra, MongoDB) serve the live website quickly.
6. Lesson: use each system for the job it does best.

---

## 2. Slide by slide

| # | Slide | What to say (short) |
|---|---|---|
| 1 | Title | "Our case study is eBay: how its data outgrew its databases, and what it built instead." |
| 2 | Every search, click and bid leaves data behind | Each action becomes data. eBay has 120M+ users, 300M+ searches a day and 350M+ items, and in 2009 one warehouse took in 150 billion log records a day. |
| 3 | The 5 Vs | Big Data means data too large, fast or varied for one database. Give one eBay example per V. Volume and Variety hurt SQL the most. |
| 4 | Four problems with SQL warehouses | (1) Every team kept its own copy, wasting 10–20x the storage. (2) The design is rigid, but 85%+ of the questions were new. (3) Logs (name=value pairs) don't fit fixed columns. (4) Growing means buying a bigger, costlier system, and Greenplum was dropped for reliability. |
| 5 | Why eBay kept both | Each of the three systems is best at a different job (see section 4). In 2010 Hadoop was weak at joins, and analysts and their tools already used SQL. |
| 6 | Platform architecture | Sources feed an integration layer, which loads three systems. Teradata feeds analysts; Hadoop feeds the website (search). |
| 7 | Timeline | 2007 a few nodes; 2010 Athena, 532 nodes; 2013 4,000+ nodes; 2014 moved to Hadoop 2 (YARN); 2015 10,000+ nodes. |
| 8 | HDFS architecture | See section 3.1. |
| 9 | YARN architecture | See section 3.2. |
| 10 | MapReduce architecture (Cassini) | See section 3.3. |
| 11 | NoSQL | HBase holds the search inventory, Cassandra handles very fast writes, MongoDB holds product data with varied fields. **Our pick:** HBase for Hadoop work, MongoDB for the catalogue. |
| 12 | Mind map | The whole system on one page. Read it as a story: the data outgrew SQL, so eBay scaled out to HDFS; YARN schedules the work, MapReduce processes it, and the results feed NoSQL. |
| 13 | Problem → answer table | Each problem is solved by one part of the system, with an eBay fact as proof. |
| 14 | Conclusions | Four takeaways, then "Any questions?" Stay on this slide during the viva. |
| 15 | References | Only if asked. |

**If you only have 2 minutes:** use slides 2, 4, 5, 6, 12 and 14, one or two sentences each.

---

## 3. The three Hadoop architectures, explained simply

### 3.1 HDFS: the storage

**In one line:** HDFS (Hadoop Distributed File System) stores huge files by cutting them into blocks and spreading copies of the blocks across many cheap machines.

**Think of it like a library with one catalogue.** The books are torn into chapters, and each chapter is kept on three different shelves. One librarian holds only the catalogue, which says where every chapter is. The librarian never carries books; readers go straight to the shelves.

```text
   Job / client
      |  1. "where is file X?"        (asks the NameNode)
      v
 +-------------+   standby copy   +-----------------+
 |  NameNode   |<---------------->| Standby NameNode|
 |  (the map)  |                  |  (backup)       |
 +-------------+                  +-----------------+
      ^   3. heartbeats + block lists every few seconds
      |
 +----------+  +----------+  +----------+  |  +----------+
 | DataNode |  | DataNode |  | DataNode |  |  | DataNode |
 |  B1 B2   |  |  B1 B3   |  |  B2 B3   |  |  | (archive)|
 +----------+  +----------+  +----------+  |  +----------+
      ^  2. client reads/writes blocks      |   old data, cheap
         DIRECTLY with the DataNodes        |   storage-only nodes
```

**The parts:**
- **NameNode:** the master. It stores only *metadata*, meaning which blocks make up a file and which machines hold them. It holds no actual data.
- **Standby NameNode:** a backup that takes over if the main NameNode fails (this is called high availability).
- **DataNodes:** the workers that store the blocks on their disks.
- **Block:** a piece of a file, 128 MB by default (eBay used 128–256 MB).
- **Replication:** every block is stored **3 times** on different machines.

**How a read or write works:**
1. The job asks the NameNode where the blocks are, or where to put them.
2. The job reads or writes the blocks **directly** with the DataNodes. When writing, the first DataNode passes the block on to the second, and the second to the third, which makes the 3 copies.
3. Every DataNode sends a **heartbeat** ("I'm alive") and a list of its blocks to the NameNode. If a node goes silent, the NameNode has its blocks copied again from the surviving copies.

**At eBay:**
- Athena (2010) had 532 DataNodes of 10 TB each (5.3 PB) and took in 8–10 TB of new data a day.
- eBay added an **ARCHIVE tier**: storage-only machines of 220 TB each that cost about a quarter as much per GB, used for old data.
- Rule of thumb: about 1 GB of NameNode memory per PB stored.
- After passing 1 billion files, eBay split the NameNode's work in two (called *federation*).

**Remember:**
- The NameNode holds the map; DataNodes hold the data.
- 3 copies mean a failed machine loses nothing.
- Data never flows through the NameNode.

### 3.2 YARN: sharing the machines

**In one line:** YARN (Yet Another Resource Negotiator) decides which job gets how much memory and CPU on which machine, so many jobs and tools can share one cluster.

**Think of it like a restaurant.** The **manager** (ResourceManager) decides which tables each group gets. Each group has a **host** (ApplicationMaster) who asks the manager for tables and organises their own group. Each **waiter** (NodeManager) looks after the tables in one section and reports to the manager.

```text
  Client (submits a MapReduce / Hive / Spark job)
      |  1. submit
      v
 +---------------------------------------------+
 |              ResourceManager                |
 |  Scheduler (queues)  |  Applications Manager|
 +---------------------------------------------+
      |  2. start the job's AppMaster      ^  3. AppMaster asks
      v                                    |     for containers
 +----------------+  +----------------+  +----------------+
 | NodeManager 1  |  | NodeManager 2  |  | NodeManager 3  |
 | [AppMaster]    |  | [Map task]     |  | [Spark AM]     |
 | [Map task]     |  | [Reduce task]  |  | [Executor]     |
 +----------------+  +----------------+  +----------------+
   each machine is also an HDFS DataNode; NodeManagers send heartbeats
```

**The parts:**
- **ResourceManager:** the master for the whole cluster. It has two parts:
  - the **Scheduler**, which divides the cluster into queues (for example one per team) and hands out resources;
  - the **Applications Manager**, which starts the master process for each job.
- **NodeManager:** runs on every machine, starts the work there, and reports health and usage.
- **ApplicationMaster:** one per job. It plans that job and asks for resources.
- **Container:** a slice of one machine (some memory and CPU) in which one task runs.

**How a job runs:**
1. The client submits a job to the ResourceManager.
2. The ResourceManager starts an ApplicationMaster for the job, inside a container on some machine.
3. The ApplicationMaster asks the Scheduler for more containers, then has the NodeManagers run the map, reduce or Spark tasks in them.
4. When the job finishes, its containers are freed for other jobs.

**Why YARN was needed:**
- In Hadoop 1, a single **JobTracker** both managed the cluster and tracked every job. That became a bottleneck, and the cluster could only run MapReduce.
- YARN (Hadoop 2) splits that work: one ResourceManager manages the cluster, and one ApplicationMaster manages each job. As a result, other engines such as Hive, Pig and Spark can share the same machines.

**At eBay:**
- eBay moved its production clusters to Hadoop 2 (YARN) around 2014.
- Its shared clusters run MapReduce, Hive, Pig, Spark and HBase together.
- By 2015 it had 10+ clusters, 10,000+ nodes and 50,000+ jobs a day.
- eBay never published its exact queue setup, so the slide shows the standard design.

**Remember:**
- ResourceManager is one per cluster; NodeManager is one per machine; ApplicationMaster is one per job.
- A container is a slice of memory and CPU.
- YARN separates "manage the cluster" from "manage a job".

### 3.3 MapReduce: the processing, using eBay's search index

**In one line:** MapReduce processes huge data in parallel. **Map** works on each piece separately, the **shuffle** groups the results by key, and **Reduce** combines each group.

**Think of it like counting votes.** Every polling booth counts its own ballots (Map). The counts are sent to one counting centre per candidate (Shuffle). Each centre adds up its candidate's numbers (Reduce).

**eBay's real example: building the Cassini search index.** Cassini is eBay's search engine. Every listing must be turned into an index so searches are fast.

```text
 Listing database (Oracle)
    |-- bulk load (every few hours) --+
    |-- small event updates (live) ---+--> HBase: item + seller tables
                                          (3,600 regions, ~15 TB)
                                                 |  scan
   MapReduce job on a dedicated cluster          v
 +---------------------------------------------------------+
 |  Map (region 1)  Map (region 2) ... Map (region n)      |
 |     each turns its items into index entries             |
 |                      |                                  |
 |            Shuffle + sort: group entries by index shard |
 |                      |                                  |
 |  Reduce -> index shard 1       Reduce -> index shard 2  |
 +---------------------------------------------------------+
                        |
                        v
      HDFS (index files) --> Cassini query nodes --> shoppers' searches
```

**Step by step:**
1. **Input:** all listings are copied from the site's Oracle database into **HBase**. There is a big bulk load every few hours, plus small updates as listings change.
2. **Map:** many map tasks run at once. Each reads one piece (region) of the HBase table and turns each item into index entries.
3. **Shuffle and sort:** Hadoop moves all entries for the same index shard (part of the index) to the same place.
4. **Reduce:** each reduce task writes one shard of the index.
5. **Output:** the index files are saved in HDFS. Cassini's query servers load them and answer shoppers' searches.

**At eBay:**
- The full index is rebuilt every **6 hours**, and small "mini" indexes are built every few minutes for new listings.
- HBase holds about 15 TB, and about 1.2 TB is imported each day.
- The target is to finish within 10 minutes, on clusters used only for index building.

**The classic textbook example, if asked:** counting searches per query.
- Map turns each search into (query, 1), for example (iphone, 1).
- The shuffle groups them: iphone → 1, 1, 1.
- Reduce adds them up: iphone → 3.

**Remember:**
- Map runs in parallel on the pieces; the shuffle groups by key; Reduce combines.
- Tasks run on the machines that already hold the data, so little data moves over the network.
- MapReduce is batch processing (minutes to hours), not instant.

### How the three fit together

```text
 MapReduce / Hive / Spark jobs   <- the work      (processing)
            YARN                 <- who runs where (resources)
            HDFS                 <- the data       (storage)
```

Every worker machine runs both a DataNode (HDFS) and a NodeManager (YARN), so YARN can run each task on the machine that already holds its data.

---

## 4. Why eBay kept SQL and Hadoop side by side (slide 5)

| System (2013) | Size | Holds | Best at |
|---|---|---|---|
| Teradata EDW | 7.5 PB | Structured transactions | Thousands of analysts querying at once, fast SQL joins, BI tools |
| Singularity (Teradata-based) | 40 PB | Semi-structured behaviour logs | SQL on log data |
| Hadoop | 40 PB | Unstructured data (images, text, raw logs) | Cheap storage, batch jobs, machine learning |

**Why not move everything to Hadoop?**
- In 2010 Hadoop was weak at joins.
- 5,000+ analysts and their tools used SQL.
- Teradata handles many users at once.

eBay copies datasets between the two when needed.

---

## 5. Numbers to memorise

| Number | What it is |
|---|---|
| 120M+ / 300M+ / 350M+ | Active users, searches a day, items for sale (c. 2013) |
| 150 billion | New log records a day into Greenplum (2009) |
| 6.5 PB, 17 trillion | Size of the Greenplum warehouse (2009) |
| 10–20x | Storage wasted by data-mart copies |
| 85%+ | Share of analysis that was new questions |
| 7.5 / 40 / 40 PB | Teradata EDW / Singularity / Hadoop (2013), about 90 PB in total |
| 532 nodes, 5.3 PB | Athena, eBay's first big Hadoop cluster (2010) |
| 8–10 TB a day | New data into Athena |
| 3 copies; 128–256 MB | HDFS replication; block size |
| 220 TB, ~1/4 cost | ARCHIVE storage node; cost per GB |
| 10,000+ nodes, 50,000+ jobs a day | eBay Hadoop in 2015 |
| 6 hours | Full Cassini index rebuild |
| 400M+ writes a day | Cassandra at eBay (2012) |

---

## 6. Viva questions (short answers)

1. **What is Big Data?** Data too large, too fast or too varied for one ordinary database to store and process.
2. **Explain the 5 Vs.** Volume (how much), Velocity (how fast), Variety (formats), Veracity (trustworthiness), Value (business use). eBay: 8–10 TB a day; 150 bn records a day; logs, clicks and images; fraud signals; search and recommendations.
3. **Why can't an RDBMS handle Big Data?** It grows by buying bigger machines, needs a fixed schema, and joins over huge tables are slow.
4. **What problems did eBay have with SQL?** Data-mart copies, a rigid design, logs that didn't fit tables, and cost and reliability as it grew.
5. **Why did eBay keep Teradata?** It is best for structured data queried by thousands of analysts in SQL; Hadoop was weak at joins in 2010.
6. **What is the role of HDFS?** Distributed storage: files are split into blocks that are copied 3 times across DataNodes, and the NameNode keeps the map.
7. **NameNode vs DataNode?** The NameNode holds metadata (where blocks are); DataNodes hold the actual blocks.
8. **Why replication?** So a failed machine loses no data, and reads can use any copy.
9. **What if the NameNode fails?** The standby NameNode takes over (high availability).
10. **What is YARN?** Hadoop's resource manager. The ResourceManager hands out resources, NodeManagers run containers, and one ApplicationMaster manages each job.
11. **Why was YARN introduced?** Hadoop 1's single JobTracker was a bottleneck and ran only MapReduce. YARN splits that work and lets Spark, Hive and others share the cluster.
12. **What is a container?** A slice of one machine's memory and CPU where one task runs.
13. **Explain MapReduce with an example.** Map emits (query, 1) for each search; the shuffle groups by query; Reduce adds them up. At eBay, MapReduce builds the Cassini search index from HBase every 6 hours.
14. **What is the shuffle?** The step between Map and Reduce that sends all values with the same key to the same reducer.
15. **HBase vs MongoDB?** HBase is a wide-column store on HDFS, good for huge tables and bulk loads from MapReduce. MongoDB is a document store with a flexible schema, good for product data.
16. **Which NoSQL do you recommend, and why?** HBase for Hadoop-linked work, because it runs on HDFS; MongoDB for the product catalogue.
17. **How does eBay use Hadoop?** HDFS stores it, YARN schedules it, MapReduce, Hive and Spark process it, and the results feed search, recommendations and NoSQL stores.

---

## 7. Last-night checklist

- [ ] Fill in "[Group members]" on slides 1 and 14.
- [ ] Practise sections 3.1–3.3 aloud. Draw each diagram once on paper from memory.
- [ ] Learn the numbers table.
- [ ] Do the 2-minute version (slides 2, 4, 5, 6, 12, 14) twice with a timer.
- [ ] Open the MongoDB at eBay source (see references) and confirm slide 11's MongoDB row, or be ready to say it is "reported".
- [ ] Never make up a number. If unsure, explain the idea and say "around" or "reported".
