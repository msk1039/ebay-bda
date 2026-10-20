# Unit 2: Hadoop Architecture

Study notes for Big Data Analytics (B.Tech, PCCoE). This unit is the core of the FA1 eBay presentation: deck slides 4-8 (SQL vs Hadoop, HDFS/YARN/MapReduce, NoSQL, mind map, cross-links). Each section below ends with an **eBay link** that tells you which slide to point at.

How to use these notes: read each topic top to bottom once, then use **Quick revision** and the **Viva questions** at the end. Background on the 5 Vs and RDBMS vs distributed systems is in `01-unit1-big-data-basics.md`; the presentation plan is in `05-fa1-ebay-presentation-guide.md`; Spark is in `03-unit3-apache-spark.md`.

Reading tip on eBay numbers: they come from eBay's own talks and blog posts (2009-2014), they are historical, and eBay's slides are not always consistent with each other. Quote them with the year, or say "hundreds of PB", as the deck does.

---

## Role of distributed systems in Big Data

**In one line:** A distributed system is a group of ordinary computers connected by a network that store data and do work together, so the group behaves like one big, failure-tolerant computer.

### Explain it like I'm new

Imagine you must read 1,000 books by tomorrow. One super-fast reader still has a limit. Better: hire 100 ordinary readers, give each 10 books, and merge their summaries. If one reader falls ill, someone else covers those 10 books. That is a distributed system.

Key terms:

- **Node**: one computer (server) in the group.
- **Cluster**: the whole group of nodes working as a team. A Hadoop cluster can have a few nodes or thousands.
- **Commodity hardware**: ordinary, cheap, off-the-shelf servers (not special mainframes). Individually they fail now and then; that is accepted and handled by software.
- **Vertical scaling (scale up)**: make one machine bigger (more CPU, RAM, disk). Costs rise steeply and there is a hard ceiling.
- **Horizontal scaling (scale out)**: add more machines. Cost grows roughly in a straight line and there is almost no ceiling.
- **Parallelism**: split a job into pieces and run the pieces at the same time on different nodes.
- **Data locality ("move compute to data")**: shipping a few kilobytes of program code to the node that already holds the data is far cheaper than pulling terabytes of data across the network to the program.
- **Fault tolerance**: the system keeps working, and loses no data, when some nodes fail. Done by keeping copies (replication) and re-running failed tasks.

### Why one big machine fails

1. **Ceiling**: there is a biggest server you can buy; data keeps growing beyond it.
2. **Cost**: a machine 10x bigger costs far more than 10x the price.
3. **Single point of failure**: if it dies, everything stops.
4. **Disk speed (I/O) bottleneck**: reading 1 TB from one disk at roughly 100-200 MB/s takes hours; 1,000 disks reading in parallel take seconds to minutes.
5. **Network bottleneck**: moving data to the computer is slower than moving the computer's code to the data.

### How it works

```text
          Scale UP (vertical)                 Scale OUT (horizontal)

        +-----------------------+         +------+ +------+ +------+ +------+
        |  one huge machine     |         | node | | node | | node | | node | ...
        |  big CPU/RAM/disk     |         | cheap| | cheap| | cheap| | cheap|
        |  expensive, 1 failure |         +------+ +------+ +------+ +------+
        |  point, hard ceiling  |            \        |        |       /
        +-----------------------+             +------ network -------+
                                              add nodes -> more storage + more CPU
```

Core design ideas used by Hadoop:

1. **Split** big files into blocks and **spread** them across nodes.
2. **Replicate** each block (3 copies) on different nodes/racks.
3. **Send the computation to the data** (locality).
4. **Detect failures** (heartbeats) and **recover automatically** (re-replicate, re-run tasks).

### Example (eBay)

eBay's first large Hadoop cluster (Athena, live 2010) took in about 8-10 TB of new data a day; eBay's Greenplum warehouse had taken in 150 billion new log records a day by 2009. One database server could not store and process that, so eBay spread work over many cheap nodes. This is **cross-link 2** on deck slide 8: *Characteristics -> Distributed Systems: scale-out nodes survive growth and failures.* It is also the "Hadoop" column of slide 4: add cheap nodes, 3 copies of every block.

### Remember

- Scale **out** (many cheap nodes) beats scale **up** (one big machine) for Big Data.
- Failures are normal at scale; software (replication + retry) hides them.
- **Move compute to data**, not data to compute.
- Parallel reads over many disks are what make terabytes fast.

**Common confusion:** "Commodity hardware means low quality." Not exactly: it means standard, inexpensive, widely available servers. They do fail more often than premium kit, which is why Hadoop replicates data.

---

## Need for Hadoop / Spark

**In one line:** Hadoop was built because relational databases and single machines could not store or process web-scale, mostly unstructured data cheaply; Spark came later to make processing much faster by keeping data in memory.

### Explain it like I'm new

A traditional **RDBMS** (relational database management system, e.g. Oracle, MySQL, Teradata) stores neat tables with a fixed schema and is brilliant at transactions and SQL. But when data grows to hundreds of terabytes and arrives as logs, clicks and images:

- It **scales up**, not out (expensive, ceiling).
- It needs a **fixed schema** up front; semi-structured data fits badly. (eBay packed `name=value` text into a single SQL column to hold logs.)
- Heavy **joins over huge tables** get slow.
- Specialised warehouse hardware and licences are **costly per terabyte**.

**What Hadoop solved** (Google published the GFS and MapReduce papers in 2003-2004; Hadoop is the open-source implementation, started by Doug Cutting and Mike Cafarella around 2005-06):

- Cheap storage: **HDFS** spreads files over commodity nodes with replication.
- Scalable processing: **MapReduce** runs code in parallel where the data lives.
- **Schema on read**: store files as they are, apply structure only when you read them.
- Open source and scalable to thousands of nodes.

**Why Spark came later.** MapReduce writes intermediate results to disk after every stage. That is safe but slow, and terrible for **iterative** work (machine learning repeats a step many times) and **interactive** work (an analyst wants answers in seconds). Spark (started at UC Berkeley around 2009, Apache project from 2013-14) keeps working data **in memory** across steps, and offers one engine for SQL, streaming, ML and graphs. Spark does not replace HDFS or YARN; it commonly runs on them. Details in `03-unit3-apache-spark.md`.

### Compare: Hadoop MapReduce vs Spark

| Aspect | Hadoop MapReduce | Apache Spark |
| --- | --- | --- |
| Where intermediate data goes | Written to disk (HDFS) after each stage | Kept in memory where possible |
| Speed | Slower (disk I/O every stage) | Often much faster, especially for iterative jobs (vendor claims up to ~100x in memory; real gains vary) |
| Best for | Large one-pass batch jobs on huge data | Iterative ML, interactive queries, streaming, batch |
| Programming | Verbose Java (Map and Reduce classes) | Concise APIs in Python, Scala, Java, R, SQL |
| Storage | Has its own: HDFS | None of its own; reads HDFS, S3, etc. |
| Resource manager | YARN | YARN, Kubernetes, Mesos or standalone |
| Memory need | Low (disk-based) | Higher (RAM-hungry) |
| Fault tolerance | Re-run failed task; data replicated in HDFS | Rebuild lost partitions from lineage (the recorded steps) |

### Example (eBay)

Deck slide 4: eBay ran Greenplum (6.5 PB, 17 trillion records) and later Teradata (10+ PB in 2010), but needed cheaper, flexible, scale-out storage and processing, so Hadoop grew beside them: "Not either/or." Hadoop 2 at eBay also runs Spark, Storm and Tez beside MapReduce (slide 5 and cross-link 4). This is **cross-link 1** on slide 8: *5 Vs -> Need for Hadoop: 8-10 TB a day outgrew SQL warehouses.* Note the honest caveat in the speaker notes: a 2010 analyst report said eBay then saw Hadoop as limited for joins.

### Remember

- RDBMS limits: scale up, fixed schema, expensive, slow on huge joins.
- Hadoop = HDFS (storage) + YARN (resources) + MapReduce (processing) on commodity nodes.
- MapReduce pays a **disk I/O** cost between stages; Spark uses **memory**.
- Spark complements Hadoop; it is not a replacement for HDFS.

**Common confusion:** "Hadoop replaced SQL warehouses at eBay." No: in 2010 eBay ran Teradata beside Hadoop.

---

## Hadoop ecosystem overview

**In one line:** The Hadoop ecosystem is the core trio (HDFS, YARN, MapReduce) plus many tools layered on top for SQL, scripting, NoSQL, data import, coordination and scheduling.

### Explain it like I'm new

Think of Hadoop as a city. HDFS is the warehouse district (storage), YARN is the city council allocating land and power (resources), MapReduce is a factory type (processing). Around them sit specialist services: a translator that lets SQL speakers use the factory (Hive), a delivery truck bringing in goods from other databases (Sqoop), and so on.

### How it works (layers)

```text
+---------------------------------------------------------------------------+
|  ACCESS / ANALYTICS    Hive (SQL)   Pig (scripts)   Spark (fast engine)   |
|                        Mahout / Spark MLlib (machine learning)            |
+---------------------------------------------------------------------------+
|  NoSQL STORE           HBase (wide-column on HDFS)                        |
+---------------------------------------------------------------------------+
|  PROCESSING            MapReduce   |  Spark / Tez / Storm (run on YARN)   |
+---------------------------------------------------------------------------+
|  RESOURCE MGMT         YARN (ResourceManager + NodeManagers)              |
+---------------------------------------------------------------------------+
|  STORAGE               HDFS (NameNode + DataNodes)                        |
+---------------------------------------------------------------------------+
   Side services:  Sqoop (DB <-> HDFS)   Flume (logs -> HDFS)
                   Oozie (workflow scheduling)   ZooKeeper (coordination)
```

One line each:

- **HDFS**: distributed file system; stores big files as replicated blocks.
- **YARN**: cluster resource manager; schedules containers for any application.
- **MapReduce**: the original batch processing model (map, shuffle, reduce).
- **Hive**: SQL-like language (HiveQL) that compiles queries into MapReduce/Tez/Spark jobs; a data warehouse on Hadoop.
- **Pig**: a scripting language (Pig Latin) for data pipelines, also compiled to jobs.
- **HBase**: NoSQL wide-column database on HDFS for random real-time reads/writes.
- **Sqoop**: bulk transfer between relational databases and HDFS (SQL to Hadoop, hence the name).
- **Flume**: collects and moves streaming log data into HDFS.
- **Oozie**: workflow scheduler that chains Hadoop jobs (run A, then B, daily at 2 am).
- **ZooKeeper**: coordination service (leader election, configuration, locks); used by HBase and HDFS high availability.
- **Spark**: fast in-memory engine, runs on YARN, reads HDFS.

### Hadoop 1 vs Hadoop 2 vs Hadoop 3

| Version | Key points |
| --- | --- |
| **Hadoop 1** (2011-12) | HDFS + MapReduce only. A single **JobTracker** did both resource management and job scheduling; **TaskTrackers** ran tasks in fixed map/reduce **slots**. Single NameNode (a single point of failure). Block size 64 MB. Limits of about 4-5 thousand nodes and ~40 thousand concurrent tasks (eBay's Hadoop-2 talk). Only MapReduce could run. |
| **Hadoop 2** (2013) | **YARN** splits the JobTracker's work into ResourceManager + per-application ApplicationMaster; NodeManagers replace TaskTrackers; generic **containers** replace slots. Many engines (Spark, Tez, Storm) share the cluster. **NameNode high availability** (active + standby). Block size 128 MB. |
| **Hadoop 3** (2017) | **Erasure coding** in HDFS (parity instead of full copies: about 1.5x storage overhead rather than 3x for a common setup). **Multiple standby NameNodes** (Hadoop 2 allowed one). Needs Java 8+. Better YARN features (e.g., resource types such as GPUs). |

### Example (eBay)

eBay's Hadoop-2 talk listed the Hadoop 1 limits above and reported about **2x job throughput** after moving to YARN. That is deck slide 5's YARN column.

### Remember

- Core trio: HDFS, YARN, MapReduce. Everything else sits on or beside them.
- Sqoop = relational DB import/export; Flume = log ingest; Oozie = scheduling; ZooKeeper = coordination.
- Hadoop 1 -> 2: JobTracker/TaskTracker replaced by YARN. Hadoop 3: erasure coding + multiple standby NameNodes.

---

## HDFS (Storage)

**In one line:** HDFS (Hadoop Distributed File System) splits each file into large blocks, stores 3 copies of each block on different nodes, and keeps a map of where everything is on a master node called the NameNode.

### Explain it like I'm new

Think of a library with a huge catalogue. The **catalogue** (NameNode) knows which shelf (DataNode) holds each page-bundle (block) of every book (file), but it does not hold the books. The **shelves** (DataNodes) hold the actual bundles, and each bundle is photocopied onto 3 different shelves in different rooms so a flood in one room loses nothing.

**Blocks.** Files are cut into fixed-size blocks: **128 MB by default in Hadoop 2+** (64 MB in Hadoop 1); eBay's deck mentions 128-256 MB. A 1 GB file becomes 8 blocks of 128 MB. Blocks are large so that the seek time is tiny compared to transfer time, and so that the NameNode has fewer blocks to track. A final block smaller than 128 MB only uses its real size on disk (a 1 MB tail does not waste 127 MB).

### How it works

**Components**

- **NameNode (master)**: holds the **metadata**: the directory tree (namespace), file-to-block list, and block-to-DataNode map (block locations, rebuilt from block reports and kept in RAM). It persists the namespace in two files:
  - **FsImage**: a snapshot of the namespace at some point.
  - **EditLog**: a running log of every change since the snapshot.
  On start-up it loads the FsImage, replays the EditLog, then waits for block reports.
- **DataNodes (workers)**: store the actual block files on local disk, serve reads/writes, send a **heartbeat roughly every 3 seconds** ("I'm alive") and a **block report** (list of blocks they hold; periodically, default around every 6 hours, plus at start-up). If the NameNode misses heartbeats for roughly 10 minutes it marks the DataNode dead and re-replicates its blocks.
- **Secondary NameNode**: periodically **checkpoints**: it fetches the FsImage and EditLog, merges them into a new FsImage, and returns it, so the EditLog does not grow forever. **It is NOT a hot backup.** It cannot take over if the NameNode dies.
- **Standby NameNode (High Availability, Hadoop 2+)**: a real hot spare. Active and standby share the EditLog via a Quorum Journal Manager (JournalNodes); DataNodes report to both; ZooKeeper-based failover controllers switch roles automatically. It also does the checkpointing, so with HA you do not need a Secondary NameNode. Hadoop 3 allows more than one standby.

```text
                         +-------------------------+
   Client  <--metadata-->|        NameNode         |  FsImage + EditLog on disk
   (app)                 |  namespace + block map  |  block map in RAM
                         +-----------+-------------+
                                     | heartbeats (~3 s) + block reports
              +----------------------+----------------------+
              |                      |                      |
        +-----v-----+          +-----v-----+          +-----v-----+
        | DataNode1 |          | DataNode2 |          | DataNode3 | ...
        | B1  B3    |          | B1  B2    |          | B2  B3    |
        +-----------+          +-----------+          +-----------+
   Client <======== reads/writes the block DATA directly with DataNodes =======>
```

**Replication factor 3 and rack awareness.** A **rack** is a group of nodes sharing a network switch. Losing a whole rack (switch or power failure) is a real risk, so default placement is:

1. **Replica 1** on the writer's node (or a random node if the client is outside the cluster).
2. **Replica 2** on a node in a **different rack**.
3. **Replica 3** on a different node in the **same rack as replica 2**.

So there are copies in 2 racks: survives a node or a whole-rack failure, while cross-rack write traffic is limited to one hop. Reads pick the nearest copy.

```text
        Rack A                          Rack B
   +----------------+            +----------------+
   | node1: copy 1  |            | node4: copy 2  |
   | node2:         |            | node5: copy 3  |
   | node3:         |            | node6:         |
   +----------------+            +----------------+
```

**Write path (step by step)**

```text
 Client           NameNode            DataNode1     DataNode2     DataNode3
   | 1. create(file) ->|                   |             |             |
   |<- 2. OK + list of 3 DataNodes for block 1 (checks permissions, no duplicate)
   | 3. stream packets ---------------->   |             |             |
   |                       DN1 4. forwards ---------->    |             |
   |                                       |   DN2 5. forwards ------->  |
   |<-------------- 6. acks flow back DN3 -> DN2 -> DN1 -> client ------|
   | 7. repeat 2-6 for each next block                                   |
   | 8. close(file) ->|  NameNode commits the file (EditLog entry)
```

In words: the client asks the NameNode to create the file; the NameNode picks 3 DataNodes; the client sends the block to DataNode 1 as small packets, which pipeline to DataNode 2 and then 3; acknowledgements travel back; the client repeats per block and finally closes the file. The data never passes through the NameNode.

**Read path (step by step)**

```text
 Client                NameNode                 DataNodes
   | 1. open(file) ------->|
   |<- 2. block list + DataNode addresses, sorted nearest-first
   | 3. read block 1 directly from nearest DataNode  ------------>
   | 4. read block 2 ... (if a DataNode fails or checksum bad -> try next replica)
   | 5. close
```

**Failure handling.** If a DataNode stops sending heartbeats, the NameNode marks it dead, finds blocks that now have fewer than 3 copies, and tells other DataNodes to copy them until each block is back to 3. Checksums detect corrupt blocks, which are replaced from a good replica.

**Write-once-read-many (WORM).** Files are written once; you can append, but not edit in the middle. This simplifies consistency and suits batch analytics (logs, history).

**What HDFS is bad at**

- **Many small files**: each file, directory and block costs roughly 150 bytes of NameNode RAM. Ten million tiny files strain NameNode memory far more than the same data in a few large files, and each small file also spawns a separate map task. Fixes: combine files (SequenceFile, Parquet, HAR archives).
- **Low-latency access**: HDFS is tuned for high throughput of big sequential reads, not millisecond lookups; use HBase or another NoSQL store for that.
- **Random in-place updates**: not supported (WORM).

### Example (eBay)

Deck slide 5: HDFS stores the data; NameNode keeps the file map, DataNodes hold 128-256 MB blocks, 3 copies, "hundreds of PB". **eBay's ARCHIVE tier** (slide 4 mentions "old data on 4x cheaper nodes"): eBay added storage-only machines, 220 TB each, beside compute machines, so cold data sits on cheaper nodes with a price per GB four times lower ("storage without computing is cheaper than storage with computing"). One cluster began at 40 PB and gained 10 PB of archive storage (eBay tiered storage post). This uses HDFS storage policies that tag data as HOT/WARM/COLD/ARCHIVE. Cross-link 3 on slide 8: *RDBMS vs Distributed -> HDFS: Hadoop stored data beside Teradata.*

### Remember

- Block: 128 MB (Hadoop 2+), 64 MB (Hadoop 1). Replication factor 3.
- NameNode = metadata (FsImage + EditLog, block map); DataNode = blocks + ~3 s heartbeats + block reports.
- Secondary NameNode = **checkpointing helper, not a failover**. Standby NameNode (HA) = failover.
- Rack-aware: 1 local, 2nd and 3rd on another rack.
- WORM; bad for small files and low latency.

**Common confusion:** "The Secondary NameNode is the backup NameNode." No. It only merges FsImage and EditLog; if the NameNode fails, you need HA (a Standby NameNode) for automatic recovery.

**Common confusion 2:** "Data flows through the NameNode." No. The NameNode only provides locations; clients move data directly to/from DataNodes.

---

## YARN (Resource Management)

**In one line:** YARN (Yet Another Resource Negotiator) is Hadoop's cluster operating system: it decides which application gets how much CPU and memory on which node, so many different engines can share one cluster.

### Explain it like I'm new

A restaurant kitchen has limited stoves and cooks. The **ResourceManager** is the head chef who decides which order gets which stove. Each station has a **NodeManager** supervising its stoves. Each order (job) brings its own **ApplicationMaster**, a coordinator who asks the head chef for stoves and then supervises its own dishes. A **container** is one reserved bundle of resources (e.g., 2 CPU cores + 4 GB RAM) on one node.

In Hadoop 1 the JobTracker did all this and only knew MapReduce slots. YARN separated "manage the cluster" from "run one job", which is why Spark, Tez and Storm can all run beside MapReduce.

### How it works

Components:

- **ResourceManager (RM)**, one per cluster (can have an HA standby), made of:
  - **Scheduler**: allocates containers according to policy (capacity, fairness); does not monitor or restart tasks.
  - **ApplicationsManager**: accepts job submissions, launches the first container for each application's ApplicationMaster, and restarts it on failure.
- **NodeManager (NM)**, one per worker node: launches and monitors containers, reports resource usage and health to the RM through heartbeats.
- **ApplicationMaster (AM)**, one per application: negotiates containers from the RM, tells NMs to run tasks, tracks progress, handles task failures.
- **Container**: an allocation of memory + CPU on a node in which a task runs.

**Job submission flow**

```text
 Client --1. submit app--> ResourceManager (ApplicationsManager)
                              | 2. allocate container for the AppMaster
                              v
                          NodeManager A: starts ApplicationMaster
                              | 3. AM registers, requests containers (with data-locality hints)
                              v
                          ResourceManager (Scheduler) -- 4. grants containers -->
                              |
                              v
        NodeManager B: container (map task)      NodeManager C: container (map task)
        NodeManager D: container (reduce task)
                              | 5. tasks run, report progress to AM
                              v
              6. AM finishes, unregisters; containers released; client gets result
```

Step by step: (1) client submits the application and its JAR to the RM; (2) the RM finds a node and starts the ApplicationMaster in the first container; (3) the AM asks the RM for more containers, preferably on nodes holding the input blocks (locality); (4) the Scheduler grants them; (5) the AM contacts those NodeManagers, which launch the tasks; (6) on completion the AM unregisters and resources are freed.

**Schedulers**

| Scheduler | Idea | Good for |
| --- | --- | --- |
| **FIFO** | First come, first served; one queue | Tiny/test clusters; a long job blocks everyone |
| **Capacity** | Cluster divided into queues, each with a guaranteed share (e.g., 60% for team A); idle capacity can be borrowed | Multi-tenant organisations needing guarantees (default in Apache Hadoop) |
| **Fair** | Running jobs get an equal share of resources over time; small jobs finish quickly | Mixed small and large jobs |

### Example (eBay)

Deck slide 5: YARN schedules resources; Resource Manager assigns cluster resources, Node Manager runs tasks; eBay reported about **2x job throughput** on Hadoop 2. The reason (from eBay's Hadoop-2 talk): Hadoop 1's fixed map/reduce slots left capacity idle and one JobTracker was a bottleneck and a single point of failure; YARN's flexible containers use the whole cluster and let Spark, Storm and Tez share it. This is **cross-link 4**: *Big Data Frameworks -> YARN: Hadoop 2 runs Spark, Storm and Tez.*

### Remember

- RM (Scheduler + ApplicationsManager), NM per node, AM per application, Container = CPU + RAM.
- YARN separates resource management from processing, which is why many engines can share the cluster.
- Schedulers: FIFO, Capacity, Fair.
- eBay: about 2x throughput after Hadoop 2 (eBay-reported).

**Common confusion:** "The ApplicationMaster is part of the ResourceManager." No. The AM is per application and runs in a container on a worker node; the RM is a single cluster-level service.

---

## MapReduce (Processing)

**In one line:** MapReduce is a programming model that processes huge data in parallel in two user-written steps: Map (turn each record into key-value pairs) and Reduce (combine all values for each key).

### Explain it like I'm new

Counting votes in a big election: each polling booth counts locally and writes "party X: 120" (Map). All the counts for party X are then sent to one officer who adds them (Reduce). The "sending all counts for the same party to the same officer" step is the **shuffle**, handled by the framework. You write only Map and Reduce; Hadoop does splitting, scheduling, shuffling, sorting and failure recovery.

### How it works

Stages: **Input splits -> Map -> Combiner (optional) -> Partition -> Shuffle & Sort -> Reduce -> Output**.

1. **Input splits**: input files are divided into logical splits (usually one per HDFS block, e.g., 128 MB). One map task per split, ideally scheduled on a node holding that block (data locality).
2. **Map**: reads one record at a time and emits **key-value pairs**.
3. **Combiner (optional)**: a "mini-reducer" run on each mapper's output locally to pre-aggregate and shrink network traffic. Only valid when the operation is associative and commutative (sum, max, count) and the reducer's input/output types match; do not use it for an average computed naively.
4. **Partition**: decides which reducer gets each key, by default `hash(key) mod number_of_reducers`, so the same key always goes to the same reducer.
5. **Shuffle & Sort**: map outputs are copied over the network to the reducers (shuffle) and sorted/grouped by key (sort), giving `key -> list of values`.
6. **Reduce**: called once per key with the list of values; emits the final pair(s).
7. **Output**: reducers write results to HDFS (one file per reducer, `part-r-00000`, ...).

```text
 Input        Split     Map            Combine      Partition +      Reduce      Output
 (HDFS)                                (optional)   Shuffle & Sort               (HDFS)

 block1 -> split1 -> map1 -> combiner1 --\                     
 block2 -> split2 -> map2 -> combiner2 ---+--> hash(key) -> reducer1 -> part-00000
 block3 -> split3 -> map3 -> combiner3 --/                  -> reducer2 -> part-00001
          (map output written to local disk)  (copied over network, merged by key)
```

### Word-count walkthrough

Input file with three lines (assume each line is its own split, to keep it small):

```text
Split 1: "deer bear river"
Split 2: "car car river"
Split 3: "deer car bear"
```

**Map** (one task per split; emits (word, 1) for each word):

```text
Map 1: (deer,1) (bear,1) (river,1)
Map 2: (car,1)  (car,1)  (river,1)
Map 3: (deer,1) (car,1)  (bear,1)
```

**Combiner** (local sum per mapper; only Map 2 changes):

```text
Comb 1: (deer,1) (bear,1) (river,1)
Comb 2: (car,2)  (river,1)            <- two (car,1) merged into (car,2)
Comb 3: (deer,1) (car,1)  (bear,1)
```

**Partition** (2 reducers; illustrative assignment, the real hash differs):

```text
Reducer 1 gets: bear, car       Reducer 2 gets: deer, river
```

**Shuffle & Sort** (group by key, sorted):

```text
Reducer 1:  bear -> [1, 1]      car -> [2, 1]
Reducer 2:  deer -> [1, 1]      river -> [1, 1]
```

**Reduce** (sum each list):

```text
Reducer 1:  (bear, 2)  (car, 3)
Reducer 2:  (deer, 2)  (river, 2)
```

**Output** written to HDFS: `bear 2, car 3, deer 2, river 2`. Without the combiner, the network would carry 9 pairs instead of 8 in this tiny case; on real data (a word like "the" appearing millions of times per mapper) the saving is huge.

Tiny pseudo-code (the part you write):

```python
def map(line):                 # key = byte offset (ignored), value = one line of text
    for word in line.split():
        emit(word, 1)

def reduce(word, counts):      # counts = all the 1s (or partial sums) for this word
    emit(word, sum(counts))
```

### Example (eBay): count searches per query

eBay gets 300M+ searches a day (c. 2013). The search log has one line per search. To find the most popular queries:

```text
Log lines (query, user, time):      Map output (query, 1):
 "iphone 12",  u1, 10:01            (iphone 12, 1)
 "nike shoes", u2, 10:01            (nike shoes, 1)
 "iphone 12",  u3, 10:02            (iphone 12, 1)

Shuffle & Sort groups by query:     iphone 12  -> [1, 1]
                                    nike shoes -> [1]
Reduce sums:                        (iphone 12, 2)  (nike shoes, 1)
```

A combiner sums locally on each mapper first, so each mapper sends one record per distinct query instead of one per search. This is exactly the example in the deck's slide 5 speaker notes. eBay also uses MapReduce to build its **Cassini** search indexes and for recommendation jobs. **Cross-link 5**: *Analytics life cycle -> MapReduce: the Process stage builds search indexes.*

### Strengths and limits

| Strengths | Limits |
| --- | --- |
| Scales to petabytes on commodity nodes | **Disk I/O between stages**: map output to local disk, reduce output to HDFS |
| Automatic parallelism, locality, fault tolerance (failed tasks re-run) | **Not suited to iterative algorithms** (each iteration is a new job reading from disk) |
| Simple model; mature | High start-up latency; not for interactive queries or real-time |
| | Verbose code; multi-step pipelines are awkward (hence Hive, Pig, Spark) |

### Remember

- Flow: split -> map -> (combine) -> partition -> shuffle & sort -> reduce -> output.
- Map emits key-value pairs; the framework groups by key; Reduce aggregates each group.
- Combiner = local mini-reduce to cut network traffic; optional.
- Partitioner: `hash(key) mod R` sends each key to one reducer.
- Weakness: disk I/O between stages; poor for iterative and interactive work (Spark fixes this).

**Common confusion:** "The combiner is the same as the reducer." It often uses the same code for sums, but it runs per mapper (a local optimisation, may run zero or more times), whereas the reducer runs once per key over all data. The result must be the same with or without the combiner.

---

## NoSQL Databases

**In one line:** NoSQL ("not only SQL") databases store data in non-relational models (key-value, document, wide-column, graph) so they can scale out across many nodes and handle flexible, huge, fast-changing data.

### Explain it like I'm new

A relational database is like a strict spreadsheet: every row must fit the same columns, and related data is split across tables joined at query time. That is perfect for bank ledgers but awkward when each product has different fields (a shirt has size and colour, a laptop has RAM and CPU), when data comes at millions of writes a second, or when you want to spread it over 100 cheap servers.

**Why NoSQL:**

- **Scale out** easily (automatic sharding/partitioning across nodes).
- **Flexible schema** (add fields without altering a table).
- **High throughput** for reads/writes.
- **High availability** through replication.
- Data model matches the application (documents, graphs).
- Trade-off: usually weaker joins and weaker or tunable consistency; SQL skills and ACID guarantees do not carry over fully.

### CAP theorem, ACID and BASE

**CAP theorem** (Eric Brewer): a distributed data store can guarantee only two of these three at once when a network failure happens:

- **C - Consistency**: every read sees the latest write (all nodes agree).
- **A - Availability**: every request gets a (non-error) response.
- **P - Partition tolerance**: the system keeps working when the network splits nodes into groups that cannot talk.

Network partitions cannot be ruled out in a real cluster, so P is a must; the real choice is **CP** (stay consistent, refuse some requests during a partition) or **AP** (stay available, may return stale data and reconcile later). Rough placement: **HBase = CP**, **MongoDB = CP by default** (a replica set elects a primary; writes go to the primary), **Cassandra = AP by default but tunable**. A single-node RDBMS gets C and A because it has no partition to worry about.

```text
              Consistency
                 /\
            CP  /  \  CA (single-node RDBMS)
        HBase  /    \
     MongoDB  /______\
   Partition ---- AP ---- Availability
   tolerance     Cassandra
```

**ACID vs BASE**

| | ACID (traditional RDBMS) | BASE (many NoSQL stores) |
| --- | --- | --- |
| Stands for | Atomicity, Consistency, Isolation, Durability | Basically Available, Soft state, Eventual consistency |
| Meaning | A transaction is all-or-nothing, leaves data valid, is isolated from others, and survives crashes | System always responds; state may change over time without new input; replicas converge **eventually** |
| Priority | Correctness first | Availability and scale first |
| Typical use | Banking, orders, inventory counts | Feeds, catalogues, logs, counters |

Many modern NoSQL stores (MongoDB multi-document transactions, Cassandra lightweight transactions) offer some ACID features; the table shows the typical design stance.

### Types of NoSQL

| Type | Data model | Examples | Typical uses |
| --- | --- | --- | --- |
| **Key-value** | Key -> opaque value (like a giant hash map) | Redis, Amazon DynamoDB | Caching, sessions, shopping carts, leaderboards |
| **Document** | Key -> JSON-like document with nested fields | MongoDB, CouchDB | Product catalogues, content management, user profiles |
| **Column-family / wide-column** | Row key -> column families -> columns (sparse, sorted) | HBase, Cassandra | Time series, logs, huge sparse tables, write-heavy workloads |
| **Graph** | Nodes + edges (relationships) with properties | Neo4j | Social networks, fraud rings, recommendations by relationship |

**Common confusion:** "NoSQL means no SQL at all / no schema at all." It means "not only SQL"; Cassandra even has an SQL-like language (CQL), and applications still have an implicit schema.

**eBay link:** Deck slide 6 shows eBay uses three NoSQL databases for three jobs. **Cross-link 6**: *Applications -> NoSQL: NoSQL stores serve live features.* Details follow.

### HBase

**In one line:** HBase is an open-source, wide-column NoSQL database modelled on Google's Bigtable, running on top of HDFS, giving fast random real-time reads and writes on huge tables with strong consistency.

**Explain it like I'm new.** HDFS is great for reading big files start to end but cannot update one record or fetch one row quickly. HBase adds that on top of HDFS. Think of a giant sorted address book where you can jump to any name instantly, add an entry, and the book itself is stored safely across many shelves (HDFS).

**How it works**

- **Table** -> rows identified by a **row key**; rows are stored **sorted by row key** (lexicographic), so range scans are fast. Row-key design is the most important design decision.
- **Column families**: groups of columns declared up front (e.g., `profile`, `orders`); columns inside a family can be added freely per row (sparse: empty cells cost nothing). Each cell has a **timestamp/version**.
- **Regions**: a table is split by row-key range into regions; each region is served by one **RegionServer** (a worker process, usually on a DataNode machine).
- **HMaster**: assigns regions to RegionServers, handles schema changes and load balancing; it is not in the read/write path.
- **ZooKeeper**: tracks which servers are alive, elects the active HMaster, and tells clients where the `hbase:meta` table is.
- **Write path**: write to the **WAL** (write-ahead log on HDFS) and the in-memory **MemStore**; when full, MemStore is flushed to an immutable **HFile** on HDFS; background **compaction** merges HFiles.
- **Consistency**: **strong consistency per row** (a row lives in exactly one region/RegionServer at a time). Data durability comes from HDFS replication.

```text
 Client --(ZooKeeper: where is meta?)--> ZooKeeper quorum
   |                                         ^  (HMaster election, server liveness)
   |  read/write row                         |
   v                                     +---+-----+
 RegionServer 1 [regions A-F]           | HMaster |  assigns regions, balances
 RegionServer 2 [regions G-M]           +---------+
 RegionServer 3 [regions N-Z]
     |  WAL + MemStore -> HFiles
     v
 HDFS (DataNodes: 3 copies of every HFile)
```

**Data model example** (table `users`, row key = `user123`):

| Row key | `profile:name` | `profile:city` | `orders:o1001` | `orders:o1002` |
| --- | --- | --- | --- | --- |
| user123 | Asha | Pune | `{item: shoes, 2500}` | `{item: phone, 18000}` |
| user124 | Ravi | (empty) | `{item: bag, 900}` | (empty) |

```text
hbase> create 'users', 'profile', 'orders'          # table + 2 column families
hbase> put 'users', 'user123', 'profile:name', 'Asha'
hbase> get 'users', 'user123'                       # random read by row key
hbase> scan 'users', {STARTROW => 'user100', STOPROW => 'user200'}   # range scan
```

**Example (eBay):** deck slide 6: HBase backs the **Cassini search indexes** (built by MapReduce, near-real-time updates) and **Kylin OLAP cubes** (Kylin, open-sourced by eBay in 2014, builds cubes from Hive and stores them in HBase). Why it fits: fast random reads/writes on HDFS, and MapReduce can bulk-load it; eBay talks reported billions of updates a day.

**Remember**

- Wide-column, on HDFS; row key (sorted) + column families + timestamped cells.
- HMaster + RegionServers + ZooKeeper; WAL -> MemStore -> HFile.
- Strong consistency (CP); random real-time read/write; MapReduce bulk-load friendly.

### Cassandra

**In one line:** Cassandra is a masterless, peer-to-peer, wide-column NoSQL database built for very high write throughput and always-on availability across many nodes and data centres.

**Explain it like I'm new.** Instead of one boss, every node is equal, like a round table where any member can take your order. Data is spread around a ring, each piece kept on several members, so no member is special and none is a single point of failure.

**How it works**

- **Ring, peer-to-peer, masterless**: all nodes have the same role; any node can accept a request (it acts as **coordinator** for that request). Nodes learn about each other by **gossip**.
- **Consistent hashing / partitioner**: the partition key is hashed to a token on a ring; each node owns a token range, so adding or removing nodes moves only a small slice of data. **Replication factor N** copies go to the next nodes on the ring (and across racks/data centres).
- **Tunable consistency** per query: levels such as `ONE`, `QUORUM`, `ALL`. With replication factor N, write level W and read level R, if **R + W > N** reads see the latest write (e.g., N = 3, QUORUM = 2 each: 2 + 2 > 3). `ONE` is faster but may return stale data: the AP end of CAP. This is why Cassandra is called "eventually consistent by default, tunable towards strong".
- **Write-optimised path**: write is appended to the **commit log** (sequential, on disk, for durability) and put into the in-memory **memtable**; when the memtable is full it is flushed to an immutable **SSTable** on disk; background **compaction** merges SSTables. Writes never need a read or an in-place update, so they are very fast. Reads may check several SSTables (Bloom filters help skip irrelevant ones), so reads are comparatively more expensive.
- **CQL (Cassandra Query Language)**: SQL-like syntax, but no joins; you design tables around the queries you will run.

```text
   Client --> any node (coordinator)
                  |  token = hash(partition key)
                  v
            +-----------+
         N1 |    ring   | N2        write goes to the 3 replica nodes that own the token
            | N6     N3 |
         N5 |    N4     |           per node:  commit log + memtable  -->  SSTables on disk
            +-----------+
```

```text
-- CQL example: order tracking
CREATE TABLE order_events (
    order_id  text,
    event_ts  timestamp,
    status    text,
    PRIMARY KEY (order_id, event_ts)      -- partition key = order_id, sorted by event_ts
);
INSERT INTO order_events (order_id, event_ts, status)
       VALUES ('A100', toTimestamp(now()), 'SHIPPED');
SELECT * FROM order_events WHERE order_id = 'A100';
```

**Example (eBay):** deck slides 3 and 6: Cassandra at eBay (Patel, 2012) handles **social signals** (like, own, want), **fraud-detection time series**, **order and shipment tracking**, mobile notification logs and server metrics; reported at 200 TB+ with **400M+ writes and 100M+ reads a day** (2012-2013). Why it fits: very fast writes and scalable counters; read latency was not a concern (eBay-reported). The 400M+ writes a day appears on slide 3 as an example of Velocity.

**Remember**

- Masterless ring, gossip, consistent hashing, replication across nodes/data centres.
- Tunable consistency (ONE/QUORUM/ALL; R + W > N).
- Write path: commit log -> memtable -> SSTable (+ compaction). CQL = SQL-like.

### MongoDB

**In one line:** MongoDB is a document database that stores flexible, JSON-like records (BSON) in collections, with rich queries, built-in replication and sharding.

**Explain it like I'm new.** Instead of splitting a product across five tables, you store the whole product as one self-contained document, like a filled-in form that can have extra boxes for products that need them.

**How it works**

- **Document**: a JSON-like record stored as **BSON** (binary JSON, adds types such as dates and binary). Documents can nest arrays and sub-documents.
- **Collection**: a group of documents (like a table, but without a fixed schema). **Flexible schema**: documents in one collection can have different fields.
- **Replica set**: one **primary** (takes writes) plus **secondaries** (copies); if the primary fails, the secondaries elect a new one automatically. This gives high availability.
- **Sharding**: a collection is split by a **shard key** across several shards (each shard is itself a replica set); `mongos` routers direct queries; config servers hold the metadata. This gives horizontal scale.
- **Consistency**: by default reads/writes go to the primary (strongly consistent); reading from secondaries can be stale (CP-leaning).
- **Rich queries and indexes**: filters on any field, secondary indexes, aggregation pipeline.

```text
   App --> mongos router --> Shard 1 (replica set: primary + 2 secondaries)
                         --> Shard 2 (replica set: primary + 2 secondaries)
          config servers (which key range lives on which shard)
```

```text
// Product document in collection "products" (mongo shell / JavaScript)
db.products.insertOne({
  sku: "LAP-001", title: "Laptop 14in", price: 55000,
  specs: { ram_gb: 16, cpu: "i5" },          // nested sub-document
  tags: ["electronics", "laptop"]            // array
});
// Query: laptops under 60000 with 16 GB RAM, cheapest first
db.products.find({ tags: "laptop", price: { $lt: 60000 }, "specs.ram_gb": 16 })
           .sort({ price: 1 });
```

A shirt document in the same collection can carry `size` and `colour` instead of `specs`; no table change is needed.

**Example (eBay):** deck slide 6: MongoDB holds **product metadata** with varied, changing fields, **loaded daily from Hadoop**; it fits because of the **flexible schema**. **Caution (the deck flags this):** the MongoDB row comes from search summaries of the "MongoDB at eBay" talk, which the group has not read in full. In the presentation, say "reported by eBay's MongoDB talk" and do not state numbers about it. The course brief pairs e-commerce with MongoDB, which is why the group recommends it for the catalogue.

**Remember**

- Documents (BSON) in collections; flexible schema; replica sets + sharding; rich queries.
- Best for catalogue-style data with varied fields.

### Compare: HBase vs Cassandra vs MongoDB

| Aspect | HBase | Cassandra | MongoDB |
| --- | --- | --- | --- |
| **Type / data model** | Wide-column; row key, column families, versioned cells | Wide-column; partition key + clustering columns | Document (BSON), collections |
| **Architecture** | Master-based: HMaster + RegionServers + ZooKeeper, on HDFS | Masterless peer-to-peer ring, gossip, own storage | Replica set (primary + secondaries) + sharding with mongos routers |
| **Storage layer** | HDFS (HFiles) | Local disks (SSTables) | Local disks (WiredTiger engine) |
| **Consistency (CAP)** | Strong per row (CP) | Tunable; AP by default | Strong on primary by default (CP-leaning); can read secondaries |
| **Writes / reads** | Fast random read/write; bulk load from MapReduce | Extremely fast writes; reads costlier | Good general read/write; rich ad-hoc queries |
| **Query language** | Shell/API (get, put, scan); SQL via Phoenix | CQL (SQL-like, no joins) | JSON-style query API, aggregation pipeline |
| **Best for** | Huge sparse tables in a Hadoop environment; search indexes; analytics serving | Write-heavy, always-on, multi-data-centre workloads; time series, counters | Varied, evolving, catalogue-style data; fast development |
| **eBay use (deck slide 6)** | Cassini search indexes; Kylin OLAP cubes | Social signals, fraud time series, order tracking | Product metadata, loaded daily from Hadoop (not fully verified) |

**Which NoSQL for e-commerce, and why?** There is no single answer, which is itself a good viva point: **MongoDB for the product catalogue** (varied product fields, flexible schema, rich search/filter queries), **Cassandra for high-volume writes** (events, order tracking, counters, fraud time series), **HBase for Hadoop-linked serving** (search indexes, OLAP cubes, bulk-loaded from MapReduce). The deck's pick: HBase for Hadoop-linked work because it runs on HDFS, MongoDB for the product catalogue; eBay itself uses all three.

---

## Case study: How Facebook Leverages Hadoop and HDFS for Big Data Storage and Analytics

This is the syllabus case study. The group presents eBay instead (deck slide 7 notes), but the examiner may still ask. Only well-documented points are given; figures are company-reported and dated, so hedge them ("reported", with the year).

**In one line:** Facebook adopted Hadoop around 2007-08 to store and analyse the huge logs and activity data of its social network, built Hive on top so analysts could use SQL instead of Java, and became known for running some of the largest HDFS clusters in the world.

### Problem

Facebook's user base and activity data grew rapidly in the late 2000s. Daily logs of page views, clicks, ad impressions and user actions piled up faster than a traditional data warehouse could handle economically. Engineers and analysts needed to run reports and experiments over all of it. Writing raw Java MapReduce for every question was too slow for hundreds of analysts who knew SQL.

### Architecture (simplified)

```text
 Web servers / app tier (user actions, page views, ad impressions)
        |  log events
        v
   Scribe (log collection servers; open-sourced by Facebook in 2008)
        |
        v
   HDFS  (large clusters; 3-way replication; warehouse of raw + processed data)
        |
        +--> Hive (HiveQL -> MapReduce jobs)  --> reports, dashboards, data warehouse tables
        +--> Hadoop MapReduce jobs            --> ads, ranking and news-feed analytics
        +--> (later) Presto                    --> interactive SQL on the same data

   Separate branch:  Facebook Messages  --> HBase (on HDFS)   [announced 2010]
```

### Key components and milestones

| Item | What it was | Notes |
| --- | --- | --- |
| **Hadoop adoption** | Started around 2007-08 for log analysis and warehousing | Early use was small, then became a core platform |
| **Hive** | SQL-like **HiveQL** compiled into MapReduce jobs; tables over files in HDFS | Created at Facebook, open-sourced in 2008 (later an Apache project); lets analysts query without writing Java |
| **Scribe** | Distributed log collection into HDFS | Facebook open-sourced it (2008); the log-ingest role is similar to Flume |
| **Cluster size** | Among the largest HDFS clusters | Reported about **21 PB** in 2010; reported **100+ PB** around 2012 (company-reported; treat as approximate) |
| **HBase for Messages** | Facebook Messages ran on HBase | Announced 2010; shows HBase used for low-latency, high-write user-facing data, not just batch |
| **NameNode HA** | "AvatarNode" high-availability work | Facebook worked on removing the NameNode single point of failure before HDFS's own HA arrived |
| **Corona** | Facebook's improved MapReduce job scheduler | Addressed JobTracker scalability limits (about 2012); comparable to the problem YARN later solved |
| **Presto** | Distributed SQL engine for interactive queries over Hadoop data | Developed around 2012-13, open-sourced 2013; fills the gap Hive-on-MapReduce left (slow, batch) |

### Uses

- **Analytics and reporting**: daily/weekly business and product metrics; the data warehouse for analysts.
- **Advertising**: ad performance, targeting analysis, billing reports.
- **News feed and product insights**: measuring how features and ranking changes affect engagement (experiments/A-B tests).
- **Operational analysis and abuse detection**: spotting spam and abnormal patterns in logs.
- **Messaging storage** on HBase.

### Lessons

1. **Scale-out on commodity hardware** was the only affordable way to store logs of a social network.
2. **SQL on Hadoop (Hive) is what opens Big Data to analysts**: the tool mattered as much as the storage.
3. **Hadoop 1's weak spots** (single NameNode, single JobTracker) hurt at scale; Facebook built AvatarNode and Corona, and the community later built HA and YARN.
4. **Batch MapReduce is too slow for interactive questions**, leading to Presto (and elsewhere to Spark).
5. **HDFS alone is not enough for real-time serving**: HBase handled the low-latency Messages workload.

### Facebook vs eBay (use this to answer: "how is your case like the syllabus case?")

| Aspect | Facebook | eBay (our case) |
| --- | --- | --- |
| Domain | Social network: posts, messages, ads | E-commerce marketplace: search, listings, orders |
| Hadoop start | About 2007-08 | 2007 (4-node cluster); first large cluster, Athena, 2010 |
| What drove Hadoop | Log and activity data growing faster than warehouses could handle | 8-10 TB a day of new data; semi-structured logs strained SQL warehouses |
| Relational systems | Used MySQL for the live site; Hadoop/Hive as the warehouse | Teradata and Greenplum warehouses kept running beside Hadoop |
| SQL layer | Hive (created at Facebook); later Presto | Hive for SQL; Spark, Storm and Tez on YARN |
| Scale (company-reported) | ~21 PB (2010); 100+ PB (c. 2012) | "Hundreds of PB" in the Hadoop-2 era; ARCHIVE tier |
| NoSQL | HBase for Messages | HBase (Cassini, Kylin), Cassandra, MongoDB |
| Scheduler | Corona (custom); YARN later in the wider community | YARN (about 2x throughput on Hadoop 2) |
| Common thread | Moved from one big database to scale-out Hadoop | Kept SQL tools beside it and added NoSQL for live features |

Short answer: both firms hit the same wall (data volume and variety outgrew a fixed-schema, scale-up database), both chose HDFS + MapReduce on commodity nodes, both added a SQL layer (Hive) and HBase for low-latency serving. Difference: Facebook's data is social activity and ads; eBay's is search, listings and transactions, and eBay mixes three NoSQL stores.

**Remember (case study):** Hadoop c. 2007-08; Hive (HiveQL, open-sourced 2008); Scribe for logs; one of the largest HDFS clusters (21 PB in 2010, 100+ PB c. 2012, reported); HBase for Messages (2010); AvatarNode (HA); Corona (scheduler); Presto (2012-13, interactive).

---

## Quick revision

- **Distributed system**: many cheap nodes in a cluster; scale **out**; parallelism, data locality (move code to data), fault tolerance via replication.
- **Why Hadoop**: RDBMS scales up, needs fixed schema, costly, slow on huge joins. Hadoop = HDFS + YARN + MapReduce on commodity nodes; **Spark** adds in-memory speed for iterative/interactive work.
- **Ecosystem**: Hive (SQL), Pig (scripts), HBase (NoSQL), Sqoop (RDBMS import), Flume (logs), Oozie (workflows), ZooKeeper (coordination), Spark.
- **Hadoop 1**: JobTracker/TaskTracker, slots, single NameNode, 64 MB blocks. **Hadoop 2**: YARN, HA NameNode, 128 MB. **Hadoop 3**: erasure coding, multiple standby NameNodes.
- **HDFS**: blocks 128 MB, replication 3; NameNode = metadata (FsImage + EditLog); DataNodes = blocks, heartbeat ~3 s, block reports; Secondary NameNode = checkpoint only (not a backup); Standby NameNode = HA.
- **Rack awareness**: copy 1 local, copies 2 and 3 on another rack. **WORM**; bad for small files and low latency. eBay ARCHIVE tier: 4x cheaper per GB.
- **YARN**: ResourceManager (Scheduler + ApplicationsManager), NodeManager, ApplicationMaster, Container. Schedulers: FIFO, Capacity, Fair. eBay: ~2x throughput.
- **MapReduce**: split -> map -> combiner (optional) -> partition -> shuffle & sort -> reduce -> output. Weakness: disk I/O, not iterative. eBay: count searches per query; builds Cassini indexes.
- **NoSQL**: key-value (Redis, DynamoDB), document (MongoDB, CouchDB), wide-column (HBase, Cassandra), graph (Neo4j). **CAP**: pick CP or AP under partitions. **BASE** vs **ACID**.
- **HBase**: on HDFS, row key, column families, regions, HMaster, ZooKeeper, strongly consistent. **Cassandra**: masterless ring, tunable consistency, commit log -> memtable -> SSTable. **MongoDB**: BSON documents, flexible schema, replica sets, sharding.
- **eBay**: HBase = Cassini + Kylin; Cassandra = social signals, fraud, order tracking; MongoDB = product metadata (not fully verified).
- **Facebook case**: Hadoop c. 2007-08, Hive, Scribe, huge HDFS (21 PB 2010; 100+ PB c. 2012 reported), HBase Messages, AvatarNode, Corona, Presto.

---

## Viva questions

**1. What is a distributed system and why does Big Data need one?**
A cluster of commodity nodes that store data and run work in parallel as one system. One machine hits ceilings in storage, disk speed and cost, and is a single point of failure; adding nodes (scale out) grows capacity and speed and tolerates failures. eBay's 8-10 TB a day outgrew single warehouses (deck slide 4, cross-link 2).

**2. What is data locality?**
Running the computation on the node that already stores the data block ("move compute to data"), because moving a small program is far cheaper than moving terabytes across the network. MapReduce and YARN schedule map tasks on nodes holding the input blocks.

**3. Why can't an RDBMS handle Big Data efficiently, and what did Hadoop change?**
An RDBMS scales up (expensive, ceiling), needs a fixed schema (poor for logs, images), and is slow for heavy joins over huge tables. Hadoop scales out on cheap nodes, stores files as they are (schema on read), and replicates data. At eBay, though, Hadoop ran beside Teradata rather than replacing it.

**4. Hadoop vs Spark: what is the main difference?**
MapReduce writes intermediate data to disk between stages; Spark keeps data in memory, so it is much faster for iterative (machine learning) and interactive work. Spark has no storage of its own and commonly runs on HDFS and YARN.

**5. What is the role of the NameNode versus a DataNode?**
The NameNode is the master: it holds metadata (namespace, file-to-block map, block locations) with FsImage + EditLog, and decides replica placement. DataNodes are workers that store the actual blocks, serve reads/writes, and send heartbeats (about every 3 seconds) and block reports. Data flows directly between clients and DataNodes, not through the NameNode.

**6. What happens when a DataNode fails?**
Its heartbeats stop; after a timeout (about 10 minutes by default) the NameNode marks it dead, finds blocks with fewer than 3 replicas, and instructs other DataNodes to copy them until replication is restored. Clients read from the surviving replicas meanwhile, so no data is lost.

**7. Why is replication needed, and what is the default factor?**
For fault tolerance and read throughput: with 3 copies, spread across at least two racks (1 local, 2 on another rack), a node or rack failure loses no data and reads can use the nearest copy. Cost: 3x storage (Hadoop 3 erasure coding reduces this to about 1.5x for cold data). eBay keeps 3 copies of every block.

**8. What is the Secondary NameNode? Is it a backup?**
No. It is a checkpointing helper that periodically merges the EditLog into the FsImage to keep the EditLog small and speed up NameNode restarts. It cannot take over. Real failover needs a Standby NameNode in an HA setup (Hadoop 2+; multiple standbys in Hadoop 3).

**9. Describe the HDFS write path.**
The client asks the NameNode to create the file; the NameNode returns 3 DataNodes for each block; the client streams the block to DataNode 1, which pipelines it to DataNode 2 and then 3; acknowledgements flow back; the client repeats for each block and closes the file, and the NameNode commits the metadata.

**10. What is the small-files problem in HDFS?**
Every file, directory and block is an object in NameNode RAM (roughly 150 bytes each), so millions of tiny files exhaust memory and also create one map task per file, which is inefficient. Solution: merge small files into larger ones (SequenceFiles, Parquet, HAR). HDFS is also poor at low-latency access and in-place updates because it is write-once-read-many.

**11. What are the components of YARN and what does each do?**
ResourceManager (cluster-wide; Scheduler allocates containers by policy, ApplicationsManager accepts jobs and starts the ApplicationMaster); NodeManager (per node; launches and monitors containers); ApplicationMaster (per job; negotiates containers and tracks tasks); Container (bundle of CPU and RAM where a task runs).

**12. Why did YARN matter, and what did eBay gain?**
It separated resource management from processing, removing the JobTracker bottleneck and fixed map/reduce slots, so Spark, Tez and Storm can share the cluster with MapReduce. eBay reported about 2x job throughput on Hadoop 2 (deck slide 5, cross-link 4).

**13. Compare the FIFO, Capacity and Fair schedulers.**
FIFO runs jobs in arrival order (a big job blocks others). Capacity splits the cluster into queues with guaranteed shares per team. Fair gives running jobs an equal share over time so short jobs finish quickly.

**14. Explain MapReduce with the word-count example.**
Map reads each line and emits (word, 1). The framework partitions by hash of the key, shuffles and sorts so all values for a word arrive together, e.g., `car -> [1,1,1]`. Reduce sums them to `(car, 3)`. eBay's equivalent: Map emits (query, 1) for each search; Reduce adds them to count searches per query.

**15. What is a combiner and when can you use it?**
A local, optional mini-reducer applied to each mapper's output before the shuffle, reducing network traffic (e.g., (car,1),(car,1) becomes (car,2)). It must be associative and commutative (sum, count, max), and the job must give the same result with or without it. It is not safe for a naive average.

**16. What are the limits of MapReduce?**
Intermediate results go to disk between stages (slow), start-up latency is high, it is poor for iterative algorithms and interactive queries, and the Java code is verbose. This led to Hive/Pig on top and to Spark.

**17. State the CAP theorem. Where do HBase, Cassandra and MongoDB sit?**
During a network partition a distributed store can keep either consistency or availability, not both (partition tolerance is a must). HBase is CP (strong per-row consistency); MongoDB is CP-leaning by default (writes to the primary); Cassandra is AP by default with tunable consistency (R + W > N for strong reads).

**18. What is the difference between ACID and BASE?**
ACID (Atomicity, Consistency, Isolation, Durability) gives strict transactions, typical of RDBMS. BASE (Basically Available, Soft state, Eventual consistency) favours availability and scale, with replicas converging over time, typical of many NoSQL stores.

**19. HBase vs MongoDB: how do they differ?**
HBase is a wide-column store on HDFS with a master-based architecture (HMaster, RegionServers, ZooKeeper), row-key lookups and range scans, strong per-row consistency and MapReduce bulk loading; best for huge sparse Hadoop-linked tables. MongoDB is a document store (BSON) with a flexible schema, rich queries, replica sets and sharding of its own; best for catalogue-style data with varied fields.

**20. Which NoSQL database would you choose for e-commerce, and why? How does Cassandra differ?**
For the product catalogue, MongoDB: products have different attributes and need flexible schema and rich filtering (the deck's pick). For Hadoop-linked work such as search indexes and OLAP cubes, HBase, since it runs on HDFS and MapReduce can load it. For very heavy writes (events, order tracking, fraud time series), Cassandra: masterless ring, tunable consistency, commit log -> memtable -> SSTable. eBay uses all three. Caveat: the MongoDB row is not fully verified in the group's sources.
