# Style guide for the study notes (all writers follow this)

Reader: a B.Tech student who knows basic programming and SQL but has **no Big Data background**, presenting the FA1 eBay deck tomorrow and facing a viva. They must be able to learn each topic from zero, then answer exam/viva questions in their own words.

## Every topic in the syllabus gets this shape
1. **In one line** — a plain-English definition (the version to say out loud in a viva).
2. **Explain it like I'm new** — 1-3 short paragraphs from first principles. Define every term the first time it appears. Use one everyday analogy where it genuinely helps (library, post office, restaurant kitchen...), then connect back to the real thing.
3. **How it works** — the mechanism, step by step. Use an ASCII diagram in a ```text block when there is a flow or architecture (markdown must render in any viewer, so no Mermaid).
4. **Example** — a concrete one; prefer the **eBay** case (FA1) or the **Olist/Spark** project (FA2) when it fits, otherwise a simple generic one.
5. **Remember** — 3-5 bullet key points / numbers worth memorising.
6. Where relevant: **Compare** tables (X vs Y), and **Common confusion** (a misconception and the correction).

## End of each file
- **Quick revision** — a 10-15 line cheat sheet of the whole unit.
- **Viva questions** — 12-20 likely questions with 2-4 sentence model answers, from easy to hard.

## Rules
- Accuracy over flourish. Use standard textbook facts (e.g., HDFS default block 128 MB in Hadoop 2+/3, replication factor 3). If a figure is company-reported or varies by source/year, say so ("reported", "around", with year). Never invent statistics, quotes, or dates. If unsure, leave the number out.
- Short paragraphs, headings per syllabus topic (use the syllabus wording as headings so the reader can tick them off), bold key terms, tables for comparisons.
- Plain Markdown only (headings, lists, tables, code blocks). No HTML, no emoji.
- Code: small, commented snippets only where they teach (PySpark for Unit 3, Python plotting for Unit 4).
- Cross-reference sibling files by name where useful: `01-unit1-big-data-basics.md`, `02-unit2-hadoop-and-nosql.md`, `03-unit3-apache-spark.md`, `04-unit4-big-data-visualization.md`, `05-fa1-ebay-presentation-guide.md`.
- Length: thorough but readable — roughly 3,000-5,000 words per unit file.

## Project context you can draw on
- FA1 deck (eBay): /Users/meow/pccoe/btech/BDA/fa1/notes/source/fa1_deck_extract.txt (slide text + speaker notes). Report: /Users/meow/pccoe/btech/BDA/fa1/reports/source/fa1_report.md.
- FA2 project (Spark on the Olist marketplace data as a stand-in for eBay): /Users/meow/pccoe/btech/BDA/fa1/fa2/PLAN.md, fa2/outputs/insights.md, fa2/outputs/metrics.json, fa2/outputs/viva_qa.md, code in fa2/analysis/*.py.
