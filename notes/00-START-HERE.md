# Big Data Analytics: Study Notes (start here)

These notes teach the whole syllabus from scratch (Units 1 to 4) and prepare you to present the FA1 eBay deck (`../ebay-bigdata-fa1.pptx`) and handle the viva.

## Files, in reading order

| # | File | What it covers | Needed for tomorrow? |
|---|---|---|---|
| 1 | [05-fa1-ebay-presentation-guide.md](05-fa1-ebay-presentation-guide.md) | The eBay story, what to say on each slide, HDFS / YARN / MapReduce architecture explained simply, numbers to memorise, viva questions | **Yes. Read this first.** |
| 2 | [01-unit1-big-data-basics.md](01-unit1-big-data-basics.md) | What Big Data is, the 5 Vs, RDBMS vs distributed systems, frameworks, the analytics life cycle, applications | **Yes** (slides 2–5) |
| 3 | [02-unit2-hadoop-and-nosql.md](02-unit2-hadoop-and-nosql.md) | Distributed systems, HDFS, YARN, MapReduce, NoSQL (HBase, Cassandra, MongoDB), the Facebook case study | **Yes** (slides 6–13) |
| 4 | [03-unit3-apache-spark.md](03-unit3-apache-spark.md) | Spark core, the spark shell, writing apps, RDDs and optimization, Spark algorithms, Twitter sentiment analysis | Later (FA2 / exam) |
| 5 | [04-unit4-big-data-visualization.md](04-unit4-big-data-visualization.md) | Chart types, challenges, geospatial / network / time-series / heatmap, streaming and high-dimensional data, tools, Uber pricing case | Later (FA2 / exam) |
| 6 | [06-fa2-spark-presentation-guide.md](06-fa2-spark-presentation-guide.md) | **FA2:** what the Olist dataset has, a cell-by-cell walkthrough of the notebook, the 5-minute script, rubric checklist, numbers to memorise, 30 viva questions | **For FA2: read this first, then 03 and 04** |

The FA1 deck covers **Units 1 and 2 only**. Units 3 and 4 are what your FA2 Spark notebook (`../fa2/`) was graded on, and they're in the exam syllabus.

## How each unit file is laid out
Each topic has five parts:
- **In one line:** the sentence to say in a viva.
- **Explain it like I'm new:** the idea from scratch.
- **How it works:** the mechanism, with diagrams.
- **Example:** usually from eBay or our own project.
- **Remember:** the facts to memorise.

Each file ends with **Quick revision** (a one-page cheat sheet) and **Viva questions** with model answers.

## One-day plan before the presentation

| Time | Do this |
|---|---|
| 1 hour | Read `05-fa1-ebay-presentation-guide.md` with the deck open beside it; draw the three architecture diagrams from memory. |
| 1.5 hours | Read `01-unit1-big-data-basics.md`. Skip the deep parts; focus on the 5 Vs and RDBMS vs distributed. |
| 2 hours | Read `02-unit2-hadoop-and-nosql.md`. Focus on HDFS, YARN, MapReduce, the three NoSQL databases and the Facebook case. |
| 30 min | Learn the "Numbers to memorise" table and practise the slides aloud (2-minute route: slides 2, 4, 5, 6, 12, 14), with a timer. |
| 45 min | Have a friend quiz you with the viva questions from the guide and both unit files. |
| Night | Read only the two "Quick revision" sections for Units 1 and 2. |

## If you're asked something you don't know
Say what you do know about the nearest concept, then link it back to eBay. For example: *"I'm not sure of the exact figure, but the principle is that HDFS keeps three copies, so a failed node loses no data. eBay relied on that across thousands of nodes."* Never make up a number.
