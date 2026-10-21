// Build the eBay Big Data FA1 deck (academic-pptx rules: action titles, white content slides,
// navy/blue/orange only, one font, citations on slides, references + conclusions at the end).
const fs = require("fs");
const pptxgen = require("pptxgenjs");
const SKILL = "/Users/meow/Library/Application Support/Claude/local-agent-mode-sessions/skills-plugin/8ef16924-38d0-468f-9113-4c9497267210/d42e73aa-f121-4707-bdc7-41d43966dcc0/skills/pptx";
const { applyTheme } = require(SKILL + "/scripts/apply_theme.js");

// Usage: node build_fa1_deck.js <out.pptx> [layout.json]   (needs pptxgenjs + jszip; SKILL path = pptx skill dir)
const OUT = process.argv[2] || "ebay-bigdata-fa1.pptx";
const LOGFILE = process.argv[3] || "layout.json";

const THEME = {
  name: "Academic Navy",
  headFontFace: "Arial",
  bodyFontFace: "Arial",
  colors: {
    dk1: "2D2D2D", lt1: "FFFFFF", dk2: "1F4E79", lt2: "EBF3FA",
    accent1: "2E75B6", accent2: "C55A11", accent3: "777777",
    accent4: "FFF2CC", accent5: "1A3A5C", accent6: "CCCCCC",
    hlink: "2E75B6", folHlink: "777777",
  },
};

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 x 5.625 in
pres.title = "eBay: from SQL warehouses to Hadoop and NoSQL";
pres.subject = "Big Data Analytics FA1 - eBay case study mapped to Unit 1 and Unit 2";
pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
const C = pres.SchemeColor;

// ---------- logging (drives the preview/fit checker) ----------
const LOG = [];
let cur = 0;
let slide = null;
let curMaster = null;
const clone = (x) => JSON.parse(JSON.stringify(x));

function newSlide(master, section, notes) {
  cur += 1;
  curMaster = master;
  slide = pres.addSlide({ masterName: master, sectionTitle: section });
  LOG.push({ s: cur, k: "slide", master });
  if (notes) slide.addNotes(notes);
  return slide;
}
function title(text) {
  LOG.push({ s: cur, k: "title", text, master: curMaster });
  slide.addText(text, { placeholder: "title" });
}
// text box or text-in-shape; margin in points
function T(content, o) {
  const opts = Object.assign({ margin: 0, valign: "top", isTextBox: true }, o);
  LOG.push({ s: cur, k: "text", content: clone(content), o: clone(opts) });
  slide.addText(content, clone(opts));
}
function shape(type, o) {
  LOG.push({ s: cur, k: "shape", type, o: clone(o) });
  slide.addShape(pres.shapes[type], clone(o));
}
function line(x1, y1, x2, y2, o) {
  const x = Math.min(x1, x2), y = Math.min(y1, y2);
  const w = Math.abs(x2 - x1), h = Math.abs(y2 - y1);
  // flipV when the line runs from bottom-left to top-right
  const flipV = (x2 - x1) * (y2 - y1) < 0;
  const opts = Object.assign({ x, y, w, h, line: { color: C.accent3, width: 1 } }, o);
  if (flipV) opts.flipV = true;
  LOG.push({ s: cur, k: "line", x1, y1, x2, y2, o: clone(opts) });
  slide.addShape(pres.shapes.LINE, clone(opts));
}
function table(rows, o) {
  LOG.push({ s: cur, k: "table", rows: clone(rows), o: clone(o) });
  slide.addTable(clone(rows), clone(o));
}

const FS = { title: 24, body: 20, label: 16, cite: 12 };
const M = 0.5;

function cite(text) {
  T(text, { x: M, y: 5.12, w: 8.4, h: 0.3, fontSize: FS.cite, color: C.accent3, align: "left", valign: "top", objectName: "Source citation" });
}

// ---------- layouts ----------
pres.defineSlideMaster({
  title: "TITLE",
  objects: [
    { rect: { x: 0, y: 0, w: 10, h: 5.625, fill: { color: C.text2 } } },
    { placeholder: { options: { name: "title", type: "title", x: 0.7, y: 1.2, w: 8.6, h: 1.7, fontSize: 32, bold: true, color: C.background1, margin: 0, valign: "top", align: "left" }, text: "" } },
  ],
});
pres.defineSlideMaster({
  title: "CONTENT",
  objects: [
    { placeholder: { options: { name: "title", type: "title", x: 0.5, y: 0.2, w: 9.0, h: 0.85, fontSize: FS.title, bold: true, color: C.text2, margin: 0, valign: "top", align: "left" }, text: "" } },
  ],
  slideNumber: { x: 9.1, y: 5.3, w: 0.5, h: 0.25, color: C.accent3, fontSize: 10 },
});
pres.defineSlideMaster({
  title: "CLOSE",
  objects: [
    { rect: { x: 0, y: 0, w: 10, h: 5.625, fill: { color: C.text2 } } },
    { placeholder: { options: { name: "title", type: "title", x: 0.5, y: 0.25, w: 9.0, h: 0.85, fontSize: FS.title, bold: true, color: C.background1, margin: 0, valign: "top", align: "left" }, text: "" } },
  ],
});

// ---------- shared pieces ----------
function callout(text, y, h, name) {
  T(text, {
    shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06,
    x: M, y: y || 4.5, w: 9.0, h: h || 0.5, margin: 6,
    fill: { color: C.accent4 }, line: { color: C.accent2, width: 1 },
    fontSize: FS.label, bold: true, color: C.text1, align: "left", valign: "middle", objectName: name || "So-what callout",
  });
}
function arrowR(x1, x2, y, n) {
  line(x1, y, x2, y, { line: { color: C.accent1, width: 2, endArrowType: "triangle" }, objectName: "Arrow " + n });
}
function arrowD(x, y1, y2, n) {
  line(x, y1, x, y2, { line: { color: C.accent1, width: 2, endArrowType: "triangle" }, objectName: "Arrow " + n });
}
function badge(t, x, y, d, name) {
  T(t, { shape: pres.shapes.OVAL, x, y, w: d, h: d, margin: 0, fill: { color: C.accent2 }, line: { color: C.accent2, width: 0.5 }, fontSize: 13, bold: true, color: C.background1, align: "center", valign: "middle", objectName: name });
}

// ---------- architecture-diagram vocabulary ----------
// kind: "core" (navy, white text) | "comp" (blue outline on tint) | "ext" (grey outline, white) | "hot" (yellow + orange)
const BOX = {
  core: { fill: C.text2, line: C.text2, head: C.background1, sub: C.background2 },
  comp: { fill: C.background2, line: C.accent1, head: C.text2, sub: C.text1 },
  ext: { fill: C.background1, line: C.accent3, head: C.text2, sub: C.accent3 },
  hot: { fill: C.accent4, line: C.accent2, head: C.text1, sub: C.text1 },
};
function box(x, y, w, h, head, sub, kind, name, fs) {
  const k = BOX[kind || "comp"], f = fs || 12;
  const runs = [{ text: head, options: { bold: true, color: k.head, fontSize: f, breakLine: !!sub } }];
  if (sub) runs.push({ text: sub, options: { color: k.sub, fontSize: f - 1.5 } });
  T(runs, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.05, x, y, w, h, margin: [5, 5, 2, 2], fill: { color: k.fill }, line: { color: k.line, width: 1 }, align: "center", valign: "middle", objectName: name || head });
}
// dashed container with a label in its top-left corner
function zone(x, y, w, h, label, name, align) {
  shape("RECTANGLE", { x, y, w, h, fill: { color: C.background1 }, line: { color: C.accent6, width: 1, dashType: "dash" }, objectName: (name || label) + " zone" });
  T(label, { x: x + 0.08, y: y + 0.04, w: w - 0.16, h: 0.2, fontSize: 10, bold: true, color: C.accent3, charSpacing: 1, align: align || "left", valign: "top", objectName: (name || label) + " zone label" });
}
// connector: solid blue = data, dashed orange = control; optional small label
function link(x1, y1, x2, y2, o) {
  o = o || {};
  const ctl = o.control;
  // the drawn line runs left-to-right; put the head on whichever end is (x2, y2)
  const flipV = (x2 - x1) * (y2 - y1) < 0;
  const endX = Math.max(x1, x2), endY = flipV ? Math.min(y1, y2) : Math.max(y1, y2);
  const headAtEnd = (x1 === x2 && y1 === y2) || (Math.abs(x2 - endX) < 1e-9 && Math.abs(y2 - endY) < 1e-9);
  const ln = { color: ctl ? C.accent2 : C.accent1, width: o.width || 1.5 };
  if (!o.noArrow) ln[headAtEnd ? "endArrowType" : "beginArrowType"] = "triangle";
  if (o.both) { ln.endArrowType = "triangle"; ln.beginArrowType = "triangle"; }
  if (ctl) ln.dashType = "dash";
  line(x1, y1, x2, y2, { line: ln, objectName: o.name || "Link" });
  if (o.label) T(o.label, { x: o.lx, y: o.ly, w: o.lw || 1.2, h: 0.2, fontSize: 10, italic: true, color: ctl ? C.accent2 : C.accent1, align: o.la || "left", valign: "middle", objectName: "Link label: " + o.label });
}
function legend(y) {
  line(M, y + 0.1, M + 0.35, y + 0.1, { line: { color: C.accent1, width: 1.5, endArrowType: "triangle" }, objectName: "Legend data" });
  T("data", { x: M + 0.42, y, w: 0.5, h: 0.2, fontSize: 10, color: C.accent3, valign: "middle", objectName: "Legend data label" });
  line(M + 0.95, y + 0.1, M + 1.3, y + 0.1, { line: { color: C.accent2, width: 1.5, dashType: "dash", endArrowType: "triangle" }, objectName: "Legend control" });
  T("control / metadata", { x: M + 1.37, y, w: 1.5, h: 0.2, fontSize: 10, color: C.accent3, valign: "middle", objectName: "Legend control label" });
}
// =====================================================================
// Architecture slides (native PowerPoint shapes so they stay editable)
// =====================================================================
function factsPanel(x, y, w, h, head, items, name) {
  shape("RECTANGLE", { x, y, w, h, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 }, objectName: name + " card" });
  T(head, { x: x + 0.15, y: y + 0.1, w: w - 0.3, h: 0.32, fontSize: 16, bold: true, color: C.accent1, align: "left", valign: "top", objectName: name + " heading" });
  T(items.flatMap(([a, b], i) => [
    { text: a + " ", options: { bold: true, color: C.text2, bullet: { indent: 12 } } },
    { text: b, options: { breakLine: i < items.length - 1 } },
  ]), { x: x + 0.15, y: y + 0.5, w: w - 0.3, h: h - 0.6, fontSize: 12, color: C.text1, align: "left", valign: "top", paraSpaceAfter: 6, objectName: name + " facts" });
}

// ---------- HDFS architecture ----------
function archHDFS(notes, facts, cites) {
  newSlide("CONTENT", "Architecture", notes);
  title("HDFS at eBay: one NameNode keeps the map, thousands of DataNodes keep the data");
  // clients
  box(0.5, 1.2, 1.5, 0.62, "Ingestion jobs", "logs, clicks, listings", "ext", "HDFS client: ingestion", 11.5);
  box(0.5, 1.95, 1.5, 0.62, "Analytics jobs", "MapReduce, Hive, Pig", "ext", "HDFS client: jobs", 11.5);
  // name nodes
  box(2.6, 1.2, 1.85, 0.62, "Active NameNode", "file → blocks → nodes", "core", "Active NameNode", 11.5);
  box(4.65, 1.2, 1.6, 0.62, "Standby NameNode", "high availability", "comp", "Standby NameNode", 11.5);
  link(4.45, 1.51, 4.65, 1.51, { control: true, name: "NameNode sync", noArrow: true, both: true });
  // clients ask the NameNode
  link(2.0, 1.4, 2.6, 1.4, { control: true, name: "Client to NameNode", label: "1. where?", lx: 1.98, ly: 1.06, lw: 0.75 });
  // DataNode zones
  zone(0.5, 2.85, 3.35, 1.55, "DISK TIER · COMPUTE NODES (HOT)", "Disk tier");
  zone(4.0, 2.85, 2.25, 1.55, "ARCHIVE TIER (COLD)", "Archive tier");
  [0, 1, 2].forEach((i) => box(0.6 + i * 1.07, 3.18, 0.98, 0.62, "DataNode", "+ compute", "comp", "Hot DataNode " + (i + 1), 11));
  [0, 1].forEach((i) => box(4.1 + i * 1.07, 3.18, 0.98, 0.62, "DataNode", "220 TB", "comp", "Archive DataNode " + (i + 1), 11));
  T("Blocks of 128–256 MB, each copied to 3 DataNodes", { x: 0.6, y: 3.88, w: 3.15, h: 0.45, fontSize: 11, italic: true, color: C.accent3, align: "left", valign: "top", objectName: "Disk tier note" });
  T("Old data; storage-only nodes at ~¼ the cost per GB", { x: 4.1, y: 3.88, w: 2.05, h: 0.5, fontSize: 11, italic: true, color: C.accent3, align: "left", valign: "top", objectName: "Archive tier note" });
  // client writes blocks to DataNodes, replicas pipeline
  link(1.25, 2.57, 1.25, 3.18, { name: "Client to DataNode", label: "2. read / write blocks", lx: 1.32, ly: 2.62, lw: 1.6 });
  link(1.58, 3.49, 1.67, 3.49, { name: "Replica pipeline 1" });
  link(2.65, 3.49, 2.74, 3.49, { name: "Replica pipeline 2" });
  // heartbeats
  link(3.4, 3.18, 3.4, 1.82, { control: true, name: "Heartbeats", label: "3. heartbeat + block report", lx: 3.47, ly: 2.36, lw: 1.9 });
  // tier mover
  link(3.85, 3.49, 4.0, 3.49, { name: "Mover to archive" });
  factsPanel(6.6, 1.2, 2.9, 3.5, "At eBay", facts, "HDFS facts");
  legend(4.82);
  cite(cites);
}

// ---------- YARN architecture ----------
function archYARN(notes, facts, cites) {
  newSlide("CONTENT", "Architecture", notes);
  title("YARN at eBay: one ResourceManager shares each cluster between many jobs");
  box(0.5, 1.2, 1.55, 0.7, "Clients", "MapReduce, Hive, Pig, Spark jobs", "ext", "YARN clients", 11.5);
  // resource manager with two parts
  shape("ROUNDED_RECTANGLE", { x: 2.45, y: 1.12, w: 3.8, h: 0.95, rectRadius: 0.05, fill: { color: C.text2 }, line: { color: C.text2, width: 1 }, objectName: "ResourceManager" });
  T("ResourceManager", { x: 2.55, y: 1.15, w: 3.6, h: 0.25, fontSize: 12, bold: true, color: C.background1, align: "left", valign: "middle", objectName: "ResourceManager label" });
  box(2.55, 1.45, 1.75, 0.52, "Scheduler", "queues per team", "comp", "RM Scheduler", 11);
  box(4.4, 1.45, 1.75, 0.52, "Applications Mgr", "starts each job's AM", "comp", "RM ApplicationsManager", 11);
  link(2.05, 1.55, 2.45, 1.55, { name: "Submit job", label: "1. submit", lx: 1.85, ly: 1.27, lw: 0.6, la: "center" });
  // worker nodes
  zone(0.5, 2.6, 5.75, 2.05, "WORKERS · ALSO HDFS DATANODES", "Worker nodes", "right");
  const nodes = [
    ["NodeManager 1", [["MR AppMaster", "hot"], ["Map task", "comp"]]],
    ["NodeManager 2", [["Map task", "comp"], ["Reduce task", "comp"]]],
    ["NodeManager 3", [["Spark AM", "hot"], ["Spark executor", "comp"]]],
  ];
  nodes.forEach(([n, cs], i) => {
    const x = 0.62 + i * 1.88;
    shape("RECTANGLE", { x, y: 2.9, w: 1.75, h: 1.62, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 }, objectName: n });
    T(n, { x: x + 0.08, y: 2.94, w: 1.6, h: 0.25, fontSize: 11, bold: true, color: C.text2, align: "left", valign: "middle", objectName: n + " label" });
    cs.forEach(([c, k], j) => box(x + 0.1, 3.24 + j * 0.62, 1.55, 0.52, c, "container", k, n + " " + c, 10.5));
  });
  // control arrows
  link(2.6, 2.07, 1.0, 3.24, { control: true, name: "RM launches AM", label: "2. launch AppMaster", lx: 0.5, ly: 2.2, lw: 1.45 });
  link(6.0, 2.9, 6.0, 2.07, { control: true, name: "Heartbeats", label: "heartbeats", lx: 4.95, ly: 2.22, lw: 0.98, la: "right" });
  link(2.37, 3.5, 2.5, 3.5, { control: true, name: "AM to tasks" });
  link(1.9, 3.24, 3.2, 2.07, { control: true, name: "AM asks for containers", label: "3. ask for containers", lx: 3.2, ly: 2.22, lw: 1.6 });
  factsPanel(6.6, 1.2, 2.9, 3.5, "At eBay", facts, "YARN facts");
  legend(4.82);
  cite(cites);
}

// ---------- MapReduce: the Cassini search-index build ----------
function archMR(notes, facts, cites) {
  newSlide("CONTENT", "Architecture", notes);
  title("MapReduce at eBay: building the Cassini search index from HBase");
  // row A: listings into HBase (left to right)
  box(0.5, 1.15, 1.3, 0.75, "Listing database", "Oracle", "ext", "Listing DB", 11.5);
  box(2.15, 1.1, 1.55, 0.38, "Bulk load", null, "comp", "Bulk loader", 11);
  box(2.15, 1.55, 1.55, 0.38, "Event updates", null, "comp", "Event agents", 11);
  link(1.8, 1.4, 2.15, 1.29, { name: "DB to bulk" });
  link(1.8, 1.65, 2.15, 1.74, { name: "DB to events" });
  box(4.05, 1.1, 2.2, 0.83, "HBase", "item + seller tables, 3,600 regions", "core", "HBase tables", 12);
  link(3.7, 1.29, 4.05, 1.29, { name: "Bulk to HBase" });
  link(3.7, 1.74, 4.05, 1.74, { name: "Events to HBase" });
  // row B: the MapReduce job (right to left)
  zone(0.5, 2.18, 5.75, 1.62, "MAPREDUCE JOB · DEDICATED INDEX CLUSTER", "MR job");
  link(5.55, 1.93, 5.55, 2.48, { name: "HBase to maps", label: "scan", lx: 5.62, ly: 2.0, lw: 0.5 });
  ["region 1", "region 2", "region n"].forEach((rg, i) => {
    box(4.85, 2.48 + i * 0.43, 1.3, 0.36, "Map · " + rg, null, "comp", "Map task " + (i + 1), 10);
    link(4.85, 2.66 + i * 0.43, 4.5, 2.66 + i * 0.43, { name: "Map to shuffle " + (i + 1) });
  });
  box(2.95, 2.48, 1.55, 1.22, "Shuffle + sort", "group item records by index shard", "hot", "Shuffle", 11.5);
  ["shard 1", "shard 2"].forEach((sh, i) => {
    box(0.62, 2.55 + i * 0.6, 1.95, 0.48, "Reduce", "write index " + sh, "comp", "Reduce " + (i + 1), 11);
    link(2.95, 2.79 + i * 0.6, 2.57, 2.79 + i * 0.6, { name: "Shuffle to reduce " + (i + 1) });
  });
  // row C: index out to search (left to right)
  link(1.6, 3.63, 1.6, 4.05, { name: "Reduce to HDFS" });
  box(0.5, 4.05, 2.0, 0.6, "HDFS", "index files", "core", "HDFS index files", 12);
  box(2.85, 4.05, 1.9, 0.6, "Cassini query nodes", "serve the index", "comp", "Query nodes", 11.5);
  box(5.1, 4.05, 1.15, 0.6, "Shoppers", "search results", "ext", "Shoppers", 11.5);
  link(2.5, 4.35, 2.85, 4.35, { name: "HDFS to query" });
  link(4.75, 4.35, 5.1, 4.35, { name: "Query to shoppers" });
  factsPanel(6.6, 1.15, 2.9, 3.5, "At eBay", facts, "MapReduce facts");
  cite(cites);
}

// ---------- documented analytics platform ----------
function archPlatform(notes, cites) {
  newSlide("CONTENT", "Architecture", notes);
  title("eBay's analytics platform: one integration layer feeds three data systems");
  const TOP = 1.2;
  // column headers
  [["SOURCES", 0.5, 1.45], ["INTEGRATION", 2.25, 1.25], ["DATA SYSTEMS", 3.85, 2.35], ["ACCESS", 6.5, 1.45], ["USERS", 8.25, 1.25]].forEach(([t, x, w]) =>
    T(t, { x, y: TOP - 0.05, w, h: 0.2, fontSize: 10, bold: true, color: C.accent3, charSpacing: 1, align: "left", valign: "top", objectName: "Column " + t }));
  const y0 = TOP + 0.25;
  box(0.5, y0, 1.45, 0.8, "Site databases", "listings, orders, bids", "ext", "Source: site DBs", 11);
  box(0.5, y0 + 0.95, 1.45, 0.8, "Clickstream", "web logs, sessions", "ext", "Source: clickstream", 11);
  box(0.5, y0 + 1.9, 1.45, 0.8, "Images + text", "unstructured", "ext", "Source: unstructured", 11);
  box(2.25, y0, 1.25, 2.7, "Data integration", "Ab Initio, Informatica, GoldenGate; UC4 scheduler", "core", "Integration layer", 11.5);
  [0, 0.95, 1.9].forEach((d, i) => link(1.95, y0 + d + 0.4, 2.25, y0 + d + 0.4, { name: "Source to integration " + (i + 1) }));
  box(3.85, y0, 2.35, 0.8, "Teradata EDW · 7.5 PB", "structured, very high concurrency", "hot", "Teradata EDW", 11);
  box(3.85, y0 + 0.95, 2.35, 0.8, "Singularity · 40 PB", "Teradata-based, semi-structured", "hot", "Singularity", 11);
  // Hadoop with its parts
  shape("ROUNDED_RECTANGLE", { x: 3.85, y: y0 + 1.9, w: 2.35, h: 1.25, rectRadius: 0.05, fill: { color: C.text2 }, line: { color: C.text2, width: 1 }, objectName: "Hadoop system" });
  T("Hadoop · 40 PB", { x: 3.95, y: y0 + 1.94, w: 2.15, h: 0.25, fontSize: 11, bold: true, color: C.background1, align: "center", valign: "middle", objectName: "Hadoop label" });
  [[["HDFS", 0.6], ["YARN", 0.6], ["Spark", 0.89]], [["MapReduce", 0.78], ["Hive · Pig", 0.78], ["HBase", 0.53]]].forEach((row, r) => {
    let cx = 3.95;
    row.forEach(([t, w]) => {
      T(t, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.05, x: cx, y: y0 + 2.24 + r * 0.42, w, h: 0.34, margin: 1, fill: { color: C.background2 }, line: { color: C.background2, width: 0.5 }, fontSize: 9, bold: true, color: C.text2, align: "center", valign: "middle", objectName: "Hadoop part " + t });
      cx += w + 0.03;
    });
  });
  [0.4, 1.35, 2.5].forEach((d, i) => link(3.5, y0 + d, 3.85, y0 + d, { name: "Integration to system " + (i + 1) }));
  // transfers between Teradata side and Hadoop
  link(5.85, y0 + 1.9, 5.85, y0 + 1.75, { both: true, name: "Singularity Hadoop transfer" });
  T("dataset transfers", { x: 6.22, y: y0 + 1.72, w: 1.2, h: 0.2, fontSize: 9.5, italic: true, color: C.accent1, align: "left", valign: "middle", objectName: "Transfer label" });
  // access + users
  box(6.5, y0, 1.45, 1.75, "DataHub + BI tools", "MicroStrategy, Tableau, SAS, Excel, SQL", "comp", "Access tools", 11);
  link(6.2, y0 + 0.4, 6.5, y0 + 0.4, { name: "EDW to access" });
  link(6.2, y0 + 1.35, 6.5, y0 + 1.35, { name: "Singularity to access" });
  box(8.25, y0, 1.25, 1.75, "Analysts", "5,000+ users (2010)", "ext", "Users: analysts", 11.5);
  link(7.95, y0 + 0.87, 8.25, y0 + 0.87, { name: "Access to analysts" });
  box(6.5, y0 + 2.25, 3.0, 0.9, "eBay website", "Cassini search index, recommendations", "comp", "Users: website", 11.5);
  link(6.2, y0 + 2.7, 6.5, y0 + 2.7, { name: "Hadoop to site" });
  T("Unstructured data lives only in Hadoop; structured, high-concurrency work stays on Teradata.", { x: M, y: 4.62, w: 9.0, h: 0.3, fontSize: 13, italic: true, color: C.text2, align: "left", valign: "top", objectName: "Platform caption" });
  cite(cites);
}
// =====================================================================
// Mind map: system view (structure of the approved map, recoloured to the deck theme)
// =====================================================================
function mindMap(notes) {
  newSlide("CONTENT", "Big picture", notes);
  title("eBay's Big Data system at a glance");
  const GY = [1.45, 3.0, 4.55], SP = 0.28, LHh = 0.24;
  const leftBr = [
    { name: "THE DATA", leaves: ["Volume · 8–10 TB a day", "Velocity · 150 bn logs a day", "Variety · logs, images", "Veracity · fraud signals", "Value · search to sales"] },
    { name: "SQL LIMITS", leaves: ["Teradata · 10+ PB", "Greenplum · 6.5 PB", "Fixed-schema workaround", "Fix · scale out, not up", "Growth · 4 to 1000s nodes"] },
    { name: "HDFS", leaves: ["NameNode · file map", "DataNodes · 128–256 MB", "Replication · 3 copies", "ARCHIVE tier · 4× cheaper", "Scale · hundreds of PB"] },
  ];
  const rightBr = [
    { name: "NOSQL", leaves: ["HBase · Cassini, Kylin", "Cassandra · fraud, orders", "Cassandra · 400M+ writes", "MongoDB · product data", "Pick · HBase on HDFS"] },
    { name: "MAPREDUCE", leaves: ["Map · split the work", "Reduce · combine results", "Cassini index builds", "Recommendation jobs", "Clicks + orders joins"] },
    { name: "YARN", leaves: ["Resource Manager", "Node Managers", "Hadoop 2 · since 2014", "Runs Hive, Pig, Spark", "Hive · SQL on Hadoop"] },
  ];
  const TR = { color: C.text2, width: 1.25 };
  line(3.95, GY[0], 3.95, GY[2], { line: TR, objectName: "Left trunk" });
  line(6.05, GY[0], 6.05, GY[2], { line: TR, objectName: "Right trunk" });
  line(3.95, GY[1], 4.2, GY[1], { line: TR, objectName: "Left root link" });
  line(5.8, GY[1], 6.05, GY[1], { line: TR, objectName: "Right root link" });
  function branch(b, gy, side) {
    const left = side === "L";
    const pillX = left ? 2.5 : 6.2, barX = left ? 2.38 : 7.62, leafX = left ? 0.25 : 7.7, trunkX = left ? 3.95 : 6.05;
    const top = gy - (4 * SP + LHh) / 2;
    const centers = b.leaves.map((_, i) => top + SP * i + LHh / 2);
    const BL = { color: C.accent1, width: 1.25 };
    line(barX, centers[0], barX, centers[4], { line: BL, objectName: b.name + " bar" });
    line(barX, gy, left ? pillX : pillX + 1.3, gy, { line: BL, objectName: b.name + " bar link" });
    line(left ? pillX + 1.3 : trunkX, gy, left ? trunkX : pillX, gy, { line: BL, objectName: b.name + " trunk link" });
    b.leaves.forEach((t, i) => {
      line(left ? leafX + 2.05 : barX, centers[i], left ? barX : leafX, centers[i], { line: { color: C.accent1, width: 1 }, objectName: "Leaf stub" });
      T(t, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06, x: leafX, y: top + SP * i, w: 2.05, h: LHh, margin: [6, 3, 1, 1], fill: { color: C.background2 }, line: { color: C.accent1, width: 0.75 }, fontSize: 10.5, color: C.text1, align: "left", valign: "middle", objectName: "Leaf: " + t });
    });
    T(b.name, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.17, x: pillX, y: gy - 0.17, w: 1.3, h: 0.34, margin: [2, 2, 1, 1], fill: { color: C.accent1 }, line: { color: C.accent1, width: 1 }, fontSize: 10.5, bold: true, color: C.background1, charSpacing: 0.5, align: "center", valign: "middle", objectName: "Branch: " + b.name });
  }
  leftBr.forEach((b, i) => branch(b, GY[i], "L"));
  rightBr.forEach((b, i) => branch(b, GY[i], "R"));
  const seq = (x, ya, yb, label, lx) => {
    line(x, ya, x, yb, { line: { color: C.accent2, width: 1.5, dashType: "dash", endArrowType: "triangle" }, objectName: "Flow " + label });
    T(label, { x: lx, y: (ya + yb) / 2 - 0.1, w: 0.75, h: 0.2, fontSize: 10, italic: true, bold: true, color: C.accent2, objectName: "Flow label " + label });
  };
  seq(3.15, GY[0] + 0.19, GY[1] - 0.19, "outgrew", 3.22);
  seq(3.15, GY[1] + 0.19, GY[2] - 0.19, "scale out", 3.22);
  seq(6.85, GY[2] - 0.19, GY[1] + 0.19, "schedules", 6.92);
  seq(6.85, GY[1] - 0.19, GY[0] + 0.19, "feeds", 6.92);
  T("ANALYTICS LIFE CYCLE", { x: 3.95, y: 0.7, w: 2.1, h: 0.18, fontSize: 9, bold: true, color: C.accent3, charSpacing: 1.5, align: "center", objectName: "Life cycle label" });
  ["Collect", "Store", "Process", "Serve", "Act"].forEach((t, i) => {
    T(t, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06, x: 4.3, y: 0.92 + 0.28 * i, w: 1.4, h: 0.24, margin: [2, 2, 1, 1], fill: { color: C.background1 }, line: { color: C.accent6, width: 0.75 }, fontSize: 10.5, color: C.text1, align: "center", valign: "middle", objectName: "Life cycle chip: " + t });
  });
  line(5.0, 2.32, 5.0, 2.48, { line: { color: C.accent3, width: 1.25, endArrowType: "triangle" }, objectName: "Into system" });
  T([{ text: "eBay", options: { fontSize: 24, bold: true, color: C.background1, breakLine: true } }, { text: "Big Data system", options: { fontSize: 10.5, color: C.background2 } }], {
    shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.15, x: 4.2, y: 2.5, w: 1.6, h: 1.0, margin: 4,
    fill: { color: C.text2 }, line: { color: C.text2, width: 1.5 }, align: "center", valign: "middle", objectName: "Centre node",
  });
  line(5.0, 3.5, 5.0, 3.66, { line: { color: C.accent3, width: 1.25, endArrowType: "triangle" }, objectName: "Out of system" });
  T("OUTCOMES", { x: 3.95, y: 3.7, w: 2.1, h: 0.18, fontSize: 9, bold: true, color: C.accent3, charSpacing: 1.5, align: "center", objectName: "Outcomes label" });
  ["Search", "Recommendations", "Fraud checks", "Dashboards"].forEach((t, i) => {
    T(t, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06, x: 4.3, y: 3.92 + 0.28 * i, w: 1.4, h: 0.24, margin: [2, 2, 1, 1], fill: { color: C.accent4 }, line: { color: C.accent2, width: 0.75 }, fontSize: 10.5, color: C.text1, align: "center", valign: "middle", objectName: "Outcome chip: " + t });
  });
}
// =====================================================================
// 1. Title
// =====================================================================
pres.addSection({ title: "Opening" });
newSlide("TITLE", "Opening",
  "SAY: Good morning. Our case study is eBay, one of the biggest online marketplaces. We will show what happened when its data grew too big for traditional SQL warehouses, why it did not simply throw them away, and how its Hadoop and NoSQL systems are built.");
title("How eBay's data outgrew its databases, and what it built instead");
T("Big Data Analytics  ·  FA1 mind-map case study", {
  x: 0.7, y: 3.2, w: 8.6, h: 0.4, fontSize: 16, color: C.background2, align: "left", valign: "top", objectName: "Subtitle",
});
T("[Group members]  ·  PCCoE, B.Tech  ·  October 2026", {
  x: 0.7, y: 3.75, w: 8.6, h: 0.4, fontSize: 15, color: C.background2, align: "left", valign: "top", objectName: "Authors",
});

// =====================================================================
// 2. A day on eBay
// =====================================================================
newSlide("CONTENT", "Opening",
  "SAY: Start with one shopper. Every time someone searches, clicks a listing, bids or buys, or lists an item with photos, eBay records it. Multiply that by more than 120 million active users: 300 million searches a day and 350 million items for sale. Already in 2009 one of eBay's warehouses was taking in 150 billion new log records every day. By then the hard part was no longer collecting the data, it was storing and analysing it.\n\n" +
  "2-MINUTE ROUTE (if the time limit is strict): slides 2, 4, 5, 6, 12 and 14, one or two sentences each.");
title("Every search, click and bid on eBay leaves data behind");
const acts = [["Search", "Query logs"], ["Click", "Clickstream events"], ["Bid or buy", "Order records"], ["List an item", "Photos and item text"]];
acts.forEach(([a, d], i) => {
  const x = 0.5 + i * 2.3;
  T(a, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06, x, y: 1.3, w: 2.1, h: 0.45, margin: 4, fill: { color: C.accent1 }, line: { color: C.accent1, width: 1 }, fontSize: FS.label, bold: true, color: C.background1, align: "center", valign: "middle", objectName: "Action " + (i + 1) });
  arrowD(x + 1.05, 1.78, 2.02, "action " + (i + 1));
  T(d, { shape: pres.shapes.RECTANGLE, x, y: 2.05, w: 2.1, h: 0.5, margin: 4, fill: { color: C.background2 }, line: { color: C.accent1, width: 0.75 }, fontSize: 15, color: C.text1, align: "center", valign: "middle", objectName: "Data " + (i + 1) });
});
[["120M+", "active users"], ["300M+", "searches a day"], ["350M+", "items for sale"], ["150 bn", "new log records a day (2009)"]].forEach(([n, l], i) => {
  T([
    { text: n, options: { fontSize: 30, bold: true, color: C.text2, breakLine: true } },
    { text: l, options: { fontSize: 15, color: C.text1 } },
  ], {
    shape: pres.shapes.RECTANGLE, x: 0.5 + i * 2.3, y: 2.85, w: 2.1, h: 1.4, margin: 8,
    fill: { color: C.background1 }, line: { color: C.accent6, width: 1 },
    align: "left", valign: "middle", objectName: "Stat card " + (i + 1),
  });
});
callout("By 2009 the hard part was no longer collecting data, but storing and analysing it.", 4.45, 0.5);
cite("Sources: Hadoop and HBase @ eBay (c. 2013); Monash (2009).");

// =====================================================================
// 3. 5 Vs
// =====================================================================
pres.addSection({ title: "The data" });
newSlide("CONTENT", "The data",
  "SAY: Big Data is data that is too large, too fast or too varied for one ordinary database. eBay shows all five Vs. Volume: 8 to 10 terabytes of new data a day into its first big Hadoop cluster. Velocity: 150 billion log records a day in 2009. Variety: logs, clicks, orders and images. Veracity: fake activity has to be caught. Value: search ranking and recommendations are where the money is. The right-hand column is the key point: each V is a different reason one database cannot cope.\n\n" +
  "VIVA - Explain the 5 Vs: Volume = how much data; Velocity = how fast it arrives and must be processed; Variety = many formats (structured, semi-structured, unstructured); Veracity = how trustworthy or clean it is; Value = the business benefit extracted from it.");
title("eBay's data shows all five Vs, and each one pushes past a single database");
T("Big Data: data too large, too fast or too varied for one ordinary database to store and process.", {
  x: M, y: 1.12, w: 9.0, h: 0.32, fontSize: 15, italic: true, color: C.text2, align: "left", valign: "top", objectName: "Definition",
});
const v14 = (t) => ({ text: t, options: { color: C.text1, fontSize: 14, valign: "middle" } });
const k14 = (t, hi) => ({ text: t, options: { bold: true, color: C.text2, fontSize: 15, valign: "middle", fill: { color: hi ? C.accent4 : C.background1 } } });
const h14 = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.text2 }, fontSize: 15, valign: "middle" } });
table([
  [h14("V"), h14("eBay evidence"), h14("Why one database struggles")],
  [k14("Volume", true), v14("8–10 TB of new data a day into the first big Hadoop cluster"), v14("One machine's disks and CPUs run out")],
  [k14("Velocity"), v14("150 billion new log records a day (2009); 400M+ Cassandra writes a day"), v14("Data arrives faster than it can be loaded")],
  [k14("Variety", true), v14("Logs, clicks, orders, images; logs packed as name=value text in one column"), v14("A fixed schema cannot hold every format")],
  [k14("Veracity"), v14("Fraud-detection time series in Cassandra help flag fake activity"), v14("Fake or noisy records must be caught first")],
  [k14("Value"), v14("Search ranking, recommendations and dashboards turn data into sales"), v14("Value comes only if all data is analysed")],
], {
  x: M, y: 1.52, w: 9.0, colW: [1.25, 4.6, 3.15], rowH: [0.36, 0.52, 0.52, 0.52, 0.52, 0.52],
  border: { type: "solid", color: C.accent6, pt: 0.75 }, margin: [0.03, 0.1, 0.03, 0.1], objectName: "5 Vs table",
});
callout("So what: Volume and Variety strain a fixed-schema SQL warehouse most.", 4.58, 0.46);
cite("Sources: Monash (2009); Hadoop: The Power of the Elephant (eBay, n.d.); Cassandra at eBay (Patel, 2012).");

// =====================================================================
// 4. Problems with the SQL warehouses
// =====================================================================
pres.addSection({ title: "SQL limits" });
newSlide("CONTENT", "SQL limits",
  "SAY: So what actually went wrong with SQL? By 2009 eBay ran two huge relational warehouses: Teradata for clean business data and Greenplum, where all the log data landed first. Greenplum alone held 6.5 petabytes and 17 trillion records. Four problems showed up. One: copies. Every team had its own data mart, a separate database with its own copy of the data. eBay's own architect showed this wasted 10 to 20 times the storage, used only 10 to 20 percent of the hardware, and needed more than 40 people to run. Two: rigid design. More than 85 percent of eBay's analysis was new, unknown questions, but a warehouse schema has to be designed in advance. Three: semi-structured logs. Logs are name=value pairs that do not fit fixed columns, so eBay squeezed the rare ones into one big text column. Four: scale and reliability. A warehouse grows by buying a bigger system, and Greenplum was dropped in 2010 mainly over reliability.\n\n" +
  "VIVA - Why can't an RDBMS handle Big Data efficiently? It scales up rather than out, needs a fixed schema, and joins over huge tables are slow. The data-mart figures are from Oliver Ratzesberger's 2010 USENIX FAST keynote slides.");
title("eBay's SQL warehouses ran into four problems as the data grew");
[["6.5 PB", "in Greenplum (2009)"], ["17 trillion", "records in it"], ["150 bn", "new records a day"], ["5,000+", "analysts (2010)"]].forEach(([n, l], i) => {
  T([{ text: n, options: { fontSize: 20, bold: true, color: C.text2, breakLine: true } }, { text: l, options: { fontSize: 13, color: C.text1 } }], {
    x: 0.5 + i * 2.3, y: 1.1, w: 2.2, h: 0.6, align: "left", valign: "top", objectName: "Scale " + (i + 1),
  });
});
const probs = [
  ["1  Copies everywhere", "Each team had its own data mart: 10–20× wasted storage, 10–20% of hardware used, 40+ staff to run them."],
  ["2  Rigid design", "85%+ of analysis was new questions, but a warehouse schema must be designed in advance."],
  ["3  Semi-structured logs", "Logs are name=value pairs; uncommon ones were packed into one big text column."],
  ["4  Scale and reliability", "Growing meant buying a bigger system; Greenplum was dropped in 2010, mainly over reliability."],
];
probs.forEach(([h, d], i) => {
  const x = 0.5 + (i % 2) * 4.6, y = 1.8 + Math.floor(i / 2) * 1.3;
  shape("RECTANGLE", { x, y, w: 4.4, h: 1.2, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 }, objectName: "Problem card " + (i + 1) });
  T(h, { x: x + 0.15, y: y + 0.08, w: 4.1, h: 0.32, fontSize: 16, bold: true, color: C.accent1, align: "left", valign: "top", objectName: "Problem heading " + (i + 1) });
  T(d, { x: x + 0.15, y: y + 0.42, w: 4.1, h: 0.78, fontSize: 14, color: C.text1, align: "left", valign: "top", objectName: "Problem detail " + (i + 1) });
});
callout("So eBay needed cheap, schema-free storage that grows by adding machines.", 4.45, 0.5);
cite("Sources: Monash (2009, 2010); Ratzesberger (2010).");

// =====================================================================
// 5. Why eBay kept both
// =====================================================================
newSlide("CONTENT", "SQL limits",
  "SAY: So why didn't eBay just move everything to Hadoop? Because each system was better at a different job, and by 2013 eBay ran three of them, about 90 petabytes in total. The Teradata warehouse, 7.5 PB, holds structured transaction data in a classic relational model and serves thousands of analysts at the same time. Singularity, 40 PB, is eBay's own Teradata-based system for semi-structured user-behaviour data, which is where those name=value logs went. Hadoop, 40 PB, takes the unstructured data, images and text, and the machine learning, cheaply on commodity machines. At the time Hadoop was also weak at joins and at queries from many users at once, and eBay's analysts and BI tools all spoke SQL. So eBay kept SQL where it was strong and moved datasets between the systems when needed.\n\n" +
  "VIVA - In 2010 an analyst (Curt Monash) reported that eBay avoided Hadoop for anything requiring data movement, such as joins, and used it for special jobs such as image analysis and analysing search results.");
title("eBay kept SQL warehouses beside Hadoop because each was best at a different job");
const sys = [
  ["Teradata EDW", "7.5 PB", "Structured transactions in a relational model", "Thousands of analysts at once; fast SQL joins; BI tools", "hot"],
  ["Singularity", "40 PB", "Semi-structured user-behaviour data (name=value logs)", "SQL on semi-structured data; deep-dive analysis", "hot"],
  ["Hadoop", "40 PB", "Unstructured data: images, text, raw logs", "Cheap scale-out storage; batch jobs and machine learning", "comp"],
];
sys.forEach(([n, size, holds, good, kind], i) => {
  const x = 0.5 + i * 3.07;
  shape("RECTANGLE", { x, y: 1.15, w: 2.87, h: 2.35, fill: { color: kind === "hot" ? C.accent4 : C.background2 }, line: { color: kind === "hot" ? C.accent2 : C.accent1, width: 1 }, objectName: "System card " + (i + 1) });
  T([{ text: n + "  ", options: { fontSize: 17, bold: true, color: C.text2 } }, { text: size, options: { fontSize: 17, bold: true, color: C.accent2 } }], { x: x + 0.15, y: 1.25, w: 2.6, h: 0.35, align: "left", valign: "top", objectName: "System name " + (i + 1) });
  T([
    { text: "Holds: ", options: { bold: true, color: C.text2 } }, { text: holds, options: { breakLine: true } },
    { text: "Best at: ", options: { bold: true, color: C.text2 } }, { text: good, options: {} },
  ], { x: x + 0.15, y: 1.68, w: 2.6, h: 1.75, fontSize: 14, color: C.text1, align: "left", valign: "top", paraSpaceAfter: 8, objectName: "System detail " + (i + 1) });
});
shape("RECTANGLE", { x: M, y: 3.65, w: 9.0, h: 1.35, fill: { color: C.background1 }, line: { color: C.accent6, width: 1 }, objectName: "Why not all Hadoop card" });
T("Why not move everything to Hadoop?", { x: 0.65, y: 3.72, w: 8.7, h: 0.3, fontSize: 15, bold: true, color: C.accent1, align: "left", valign: "top", objectName: "Why heading" });
T([
  { text: "Joins: in 2010 eBay avoided Hadoop for joins and other jobs that move lots of data", options: { bullet: { indent: 14 }, breakLine: true } },
  { text: "People: 5,000+ analysts and their BI tools already worked in SQL, with many queries at once", options: { bullet: { indent: 14 }, breakLine: true } },
  { text: "Flow: datasets are copied between Teradata and Hadoop on request, so each side keeps its strength", options: { bullet: { indent: 14 } } },
], { x: 0.65, y: 4.03, w: 8.7, h: 0.95, fontSize: 13, color: C.text1, align: "left", valign: "top", paraSpaceAfter: 2, objectName: "Why bullets" });
cite("Sources: iTnews (2013); ODBMS.org interview (n.d.); Monash (2010); Hennig (2013).");

// =====================================================================
// 6. Platform architecture
// =====================================================================
pres.addSection({ title: "Architecture" });
archPlatform(
  "SAY: Here is how eBay's analytics platform fits together, based on eBay's own architecture slide from 2010 and its 2013 figures. On the left are the sources: the site databases with listings, orders and bids, the clickstream logs, and unstructured data like images. An integration layer, using tools such as Ab Initio, Informatica and GoldenGate, scheduled by UC4, loads them into three systems. The two Teradata systems feed analysts through BI tools like MicroStrategy and Tableau. Hadoop feeds the website itself, for example the Cassini search index. Datasets can be copied between Teradata and Hadoop. The next three slides open up Hadoop: HDFS, YARN and MapReduce.\n\n" +
  "NOTE: Hadoop's internal parts and the Hadoop-to-website link come from eBay's 2013 and 2015 Hadoop talks; the rest of the layering is from the 2010 USENIX FAST keynote slides.",
  "Sources: Ratzesberger (2010); iTnews (2013); ODBMS.org (n.d.); Hennig (2013); Hadoop at eBay (c. 2015).");

// =====================================================================
// 7. Timeline
// =====================================================================
newSlide("CONTENT", "Architecture",
  "SAY: Hadoop did not arrive overnight. It started in 2007 with a handful of nodes. In 2010 eBay built Athena, its first large cluster: 532 nodes, 5.3 petabytes, taking 8 to 10 terabytes of new data a day. By 2013 it had more than 4,000 nodes and over 50 petabytes, and its new Cassini search engine was built on HBase and MapReduce. In 2014 production clusters moved to Hadoop 2 with YARN, and eBay open-sourced Kylin. By 2015 eBay ran more than 10 clusters with over 10,000 nodes and 50,000 jobs a day, all still beside the Teradata systems.\n\n" +
  "NOTE: eBay's slides give different node counts for nearby years, so the slide quotes each figure with its year.");
title("eBay's Hadoop grew from a few test nodes in 2007 to over 10,000 nodes");
line(0.6, 1.75, 9.4, 1.75, { line: { color: C.text2, width: 2 }, objectName: "Timeline axis" });
[
  ["2007", "First Hadoop cluster: a handful of nodes, as an experiment"],
  ["2010", "Athena goes live: 532 nodes, 5.3 PB, 8–10 TB of new data a day"],
  ["2013", "4,000+ nodes, 50+ PB; Cassini search built on HBase + MapReduce"],
  ["2014", "Production clusters move to Hadoop 2 (YARN); Kylin open-sourced"],
  ["2015", "10+ clusters, 10,000+ nodes, 50,000+ jobs a day"],
].forEach(([yr, txt], i) => {
  const x = 0.5 + i * 1.84;
  T(yr, { x, y: 1.15, w: 1.64, h: 0.4, fontSize: 18, bold: true, color: C.accent2, align: "left", valign: "bottom", objectName: "Year " + (i + 1) });
  shape("OVAL", { x: x + 0.02, y: 1.65, w: 0.2, h: 0.2, fill: { color: C.accent2 }, line: { color: C.background1, width: 1.5 }, objectName: "Milestone dot " + (i + 1) });
  T(txt, { shape: pres.shapes.RECTANGLE, x, y: 2.05, w: 1.64, h: 2.0, margin: 8, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 }, fontSize: 15, color: C.text1, align: "left", valign: "top", objectName: "Milestone " + (i + 1) });
});
callout("Hadoop grew beside the SQL warehouses, not instead of them.", 4.45, 0.5);
cite("Sources: eBay Tech Blog; eBay HUG (2010); Hennig (2013); Hortonworks (2014); Hadoop at eBay (2015).");

// =====================================================================
// 8-10. HDFS, YARN, MapReduce architecture
// =====================================================================
archHDFS(
  "SAY: This is HDFS, Hadoop's storage layer, as eBay runs it. A job that wants to read or write a file first asks the NameNode where the blocks are; the NameNode only holds the map of files to blocks to machines. The data itself goes straight between the job and the DataNodes. Files are cut into 128 to 256 MB blocks and each block is copied to three DataNodes, so a failed machine loses nothing. DataNodes send heartbeats and block reports so the NameNode always knows what is alive. eBay added a cheaper ARCHIVE tier: storage-only machines of 220 TB each, about a quarter of the cost per GB, for old data. eBay's first big cluster, Athena, had 532 such nodes in 2010.\n\n" +
  "VIVA - Role of HDFS: distributed storage. NameNode = metadata (the map), DataNodes = the blocks. Replication = 3 copies so a failed node loses no data. A standby NameNode (high availability) was on eBay's 2013 plan; around 2017 eBay split the namespace in two (federation) after passing 1 billion files.",
  [["2010:", "Athena: 532 nodes of 10 TB each (5.3 PB), 8–10 TB new data a day"], ["Masters:", "NameNode, JobTracker, ZooKeeper, HBase Master on larger servers"], ["Memory:", "~1 GB of NameNode memory per PB stored"], ["Tiers:", "40 PB DISK + 10 PB ARCHIVE (220 TB nodes)"], ["Later:", "1 billion files → namespace split in two"]],
  "Sources: eBay HUG (2010); eBay Tech Blog (Hadoop, tiered storage, HDFS federation).");
archYARN(
  "SAY: YARN is the part that shares the cluster. In Hadoop 1 one JobTracker had to manage every job on the cluster, which became a bottleneck. YARN splits that job in two. A ResourceManager, with a scheduler that divides the cluster into queues, decides who gets how much memory and CPU. For each job it starts an ApplicationMaster, which asks for containers and runs the actual map, reduce or Spark tasks in them on the NodeManagers. NodeManagers report back with heartbeats. Every worker is also an HDFS DataNode, so tasks run next to their data. eBay moved its production clusters to Hadoop 2 in 2014, and its shared clusters run MapReduce, Hive, Pig, Spark and HBase side by side.\n\n" +
  "VIVA - What is YARN? Yet Another Resource Negotiator: ResourceManager (global, schedules) + NodeManager (per machine, runs containers) + ApplicationMaster (per job). The queue layout shown is the standard design; eBay's exact scheduler settings are not published.",
  [["Why:", "Hadoop 1's single JobTracker ran every job; YARN splits it into a ResourceManager + one AppMaster per job"], ["2014:", "eBay's production clusters move to Hadoop 2"], ["Shared:", "MapReduce, Hive, Pig, Spark, HBase on one cluster"], ["Scale:", "10+ clusters, 10,000+ nodes, 50,000+ jobs a day"]],
  "Container layout is the standard YARN design. Sources: Hennig (2013); Hortonworks (2014); eBay (2015).");
archMR(
  "SAY: Here is MapReduce doing a real eBay job: building the index for Cassini, eBay's search engine. Every listing lives in the site's Oracle database. It is copied into HBase two ways: a bulk load every few hours, and small event updates in near real time. HBase holds the item and seller tables split into 3,600 regions. A MapReduce job on a dedicated cluster then scans them: each map task reads one region and turns items into index entries, the shuffle groups the entries by index shard, and each reduce task writes one shard of the index. The index files go to HDFS and are loaded by the Cassini query nodes that answer shoppers' searches. A full index is rebuilt every 6 hours, with mini indexes every few minutes.\n\n" +
  "VIVA - Explain MapReduce with an example: Map = process each piece in parallel and emit key-value pairs; Shuffle = group by key; Reduce = combine each group. Here the key is the index shard. The split into map, shuffle and reduce steps follows the standard pattern; the pipeline, schedule and sizes are eBay's.",
  [["Input:", "whole site inventory in HBase: ~15 TB, 1.2 TB imported a day"], ["Full index:", "rebuilt every 6 hours"], ["Mini index:", "every few minutes, for new listings"], ["Target:", "SLA under 10 minutes"], ["Where:", "dedicated index-building clusters"]],
  "Sources: Cassini HBase (eBay, 2012); Hadoop and HBase @ eBay (c. 2013); Hadoop at eBay (c. 2015).");

// =====================================================================
// 11. NoSQL
// =====================================================================
pres.addSection({ title: "Serving live" });
newSlide("CONTENT", "Serving live",
  "SAY: Hadoop is great for big batch jobs, but the live website needs answers in milliseconds. That is the job of NoSQL databases, which drop the fixed relational schema and spread across many machines. HBase sits on HDFS and holds the whole site inventory for Cassini search, plus the Kylin analytics cubes. Cassandra is built for fast writes: social signals like 'want' and 'own', fraud time series and order tracking, at 400 million writes a day. MongoDB holds product details whose fields differ from item to item. Our pick: HBase for Hadoop-linked work, MongoDB for the product catalogue.\n\n" +
  "VIVA - HBase vs MongoDB: HBase is a wide-column store built on HDFS, with row-key lookups and range scans, strong consistency per row, and bulk loading from MapReduce. MongoDB is a document store (JSON-like documents) with a flexible schema and rich queries, with its own replication and sharding. Cassandra is a masterless wide-column store tuned for very fast writes. CAUTION: the MongoDB row comes from summaries of an eBay talk (MongoDB at eBay) that we have not read in full - check it before presenting.");
title("For the live site, eBay matched each feature to the NoSQL store built for it");
table([
  [h14("Database"), h14("Type"), h14("eBay use, and at what scale"), h14("Why it fits")],
  [k14("HBase"), v14("Wide-column store on HDFS"), v14("Whole site inventory for Cassini search (~15 TB); Kylin OLAP cubes"), v14("Fast random reads and writes; MapReduce bulk-loads it")],
  [k14("Cassandra"), v14("Wide-column, write-heavy"), v14("Social signals, fraud time series, order tracking; 200 TB+, 400M+ writes a day"), v14("Very fast writes; scalable counters")],
  [k14("MongoDB"), v14("Document store"), v14("Product metadata with varied fields, loaded daily from Hadoop"), v14("Flexible schema")],
], {
  x: M, y: 1.2, w: 9.0, colW: [1.35, 1.75, 3.5, 2.4], rowH: [0.4, 0.82, 0.82, 0.62],
  border: { type: "solid", color: C.accent6, pt: 0.75 }, margin: [0.04, 0.1, 0.04, 0.1], objectName: "NoSQL table",
});
callout("Our pick: HBase for Hadoop-linked work, because it runs on HDFS; MongoDB for the product catalogue.", 4.1, 0.85, "Recommendation callout");
cite("Sources: Cassini HBase (eBay, 2012); Kylin (eBay, 2014); Patel (2012); MongoDB at eBay (n.d.).");

// =====================================================================
// 12. Mind map
// =====================================================================
pres.addSection({ title: "Big picture" });
mindMap("SAY: This is the whole system on one page. Read it like the story: on the left, the data outgrew the SQL warehouses, so eBay scaled out with HDFS. On the right, YARN schedules the work, MapReduce processes it, and the results feed the NoSQL stores. The centre shows the life of the data, from collection to action, and the outcomes shoppers and analysts see.\n\n" +
  "For the 'use an AI tool' criterion, recreate this map in Napkin AI, XMind, Canva or Miro from the same text and keep this slide as the reference.");

// =====================================================================
// 13. Problem -> answer links
// =====================================================================
newSlide("CONTENT", "Big picture",
  "SAY: The links on the map come down to this table: every problem in eBay's data is answered by one part of the system, and each has an eBay fact behind it. Volume is answered by HDFS spreading data over cheap nodes; failures by three copies; rigid schemas by storing files as they are; many engines by YARN; huge batch jobs by MapReduce; the live site by NoSQL; and structured, many-user analysis by keeping Teradata.");
title("Each problem in eBay's data is answered by one part of the system");
const num = (t) => ({ text: t, options: { bold: true, color: C.accent2, fontSize: 13, valign: "middle", align: "center" } });
const c2 = (t) => ({ text: t, options: { bold: true, color: C.text2, fontSize: 13, valign: "middle" } });
const c3 = (t) => ({ text: t, options: { color: C.text1, fontSize: 13, valign: "middle" } });
const h15 = (t, al) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.text2 }, fontSize: 14, valign: "middle", align: al || "left" } });
table([
  [h15("#", "center"), h15("Problem"), h15("Answered by"), h15("eBay evidence")],
  [num("1"), c3("One machine can't hold it all"), c2("HDFS on cheap nodes"), c3("8–10 TB a day into Athena (2010)")],
  [num("2"), c3("Machines fail"), c2("3 copies per block"), c3("A failed node loses no data")],
  [num("3"), c3("Rigid schema, raw logs"), c2("Store raw files"), c3("Unstructured data lives only in Hadoop")],
  [num("4"), c3("Many jobs, one cluster"), c2("YARN"), c3("MapReduce, Hive, Spark on one cluster")],
  [num("5"), c3("Huge batch jobs"), c2("MapReduce"), c3("Full Cassini index every 6 hours")],
  [num("6"), c3("Live site needs fast reads"), c2("NoSQL stores"), c3("Whole inventory in HBase for search")],
  [num("7"), c3("Many users, structured data"), c2("Keep Teradata"), c3("5,000+ users; 7.5 PB EDW (2013)")],
], {
  x: M, y: 1.15, w: 9.0, colW: [0.45, 2.75, 2.2, 3.6], rowH: [0.38, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45],
  border: { type: "solid", color: C.accent6, pt: 0.75 }, margin: [0.03, 0.1, 0.03, 0.1], objectName: "Problem answer table",
});
cite("Sources: eBay HUG (2010); ODBMS.org (n.d.); Hadoop at eBay (c. 2015); Cassini HBase (2012); iTnews (2013).");

// =====================================================================
// 14. Conclusions
// =====================================================================
pres.addSection({ title: "Close" });
newSlide("CLOSE", "Close",
  "SAY: To close. eBay's SQL warehouses strained under copies, rigid schemas and semi-structured logs. eBay did not throw them away: it kept SQL for structured, many-user analysis and added Hadoop for everything else, about 90 PB across three systems by 2013. Inside Hadoop, HDFS stores, YARN shares the cluster and MapReduce builds the search index. NoSQL serves the live site. The lesson for any e-commerce company: use each system for the job it does best. Thank you; we are happy to take questions.\n\n" +
  "Likely viva: 5 Vs (slide 3), why RDBMS can't cope (slide 4), why keep both (slide 5), role of HDFS and replication (slide 8), YARN (slide 9), MapReduce example (slide 10), HBase vs MongoDB (slide 11).");
title("eBay's lesson: use each system for the job it does best");
T([
  { text: "1. SQL warehouses strained: ", options: { bold: true } },
  { text: "data-mart copies, rigid schemas and semi-structured logs at 6.5 PB and 150 bn records a day.", options: { breakLine: true } },
  { text: "2. eBay kept SQL and added Hadoop: ", options: { bold: true } },
  { text: "Teradata for structured, many-user analysis; Hadoop for the rest; ~90 PB in total (2013).", options: { breakLine: true } },
  { text: "3. Inside Hadoop: ", options: { bold: true } },
  { text: "HDFS stores with 3 copies, YARN shares each cluster, MapReduce builds the search index.", options: { breakLine: true } },
  { text: "4. NoSQL serves the live site: ", options: { bold: true } },
  { text: "HBase for search, Cassandra for fast writes, MongoDB for the catalogue.", options: {} },
], { x: M, y: 1.0, w: 9.0, h: 3.55, fontSize: 18, color: C.background1, align: "left", valign: "top", paraSpaceAfter: 14, objectName: "Takeaways" });
T("Are there questions or feedback?   [Group members]  ·  PCCoE, B.Tech, Big Data Analytics  ·  FA1", { x: M, y: 4.75, w: 9.0, h: 0.4, fontSize: 14, color: C.background2, align: "left", valign: "top", objectName: "Contact" });

// =====================================================================
// 15. References
// =====================================================================
newSlide("CONTENT", "Close",
  "Dates marked n.d. were not shown on the pages we read. The MongoDB at eBay entry was found through search summaries; open it and confirm the claims before presenting. dbms2.com posts were read through their InformationWeek copies. eBay's slides are not always consistent with each other (e.g. node counts), so each figure is quoted with its year.");
T("References", { x: M, y: 0.2, w: 9.0, h: 0.5, fontSize: FS.title, bold: true, color: C.text2, align: "left", valign: "top", objectName: "References heading" });
const refs = [
  "Monash, C. (2009). eBay's two enormous data warehouses. DBMS2.",
  "Monash, C. (2010). eBay followup: Greenplum out, Teradata > 10 petabytes, Hadoop has some value, and more. DBMS2.",
  "Ratzesberger, O. (2010). Keynote slides. USENIX FAST '10. static.usenix.org/events/fast10/tech/slides/ratzesberger.pdf",
  "iTnews (2013). Inside eBay's 90PB data warehouse. itnews.com.au/news/inside-ebay8217s-90pb-data-warehouse-342615",
  "ODBMS.org (n.d.). Interview on eBay's analytics systems. odbms.org/blog/?p=1184",
  "eBay Inc. (2010). Hadoop at eBay [Slides]. Bay Area HUG. slideshare.net/slideshow/2-hadoope-bayhug20100721/4819530",
  "eBay Inc. (n.d.). Hadoop: The power of the elephant. eBay Tech Blog.",
  "Hennig (2013). Hadoop @ eBay: past, present and future [Slides]. slideshare.net/slideshow/hadoop-ebay-past-present-and-future/29371000",
  "eBay Inc. (c. 2013). Hadoop and HBase @ eBay [Slides]. slideshare.net/slideshow/ma-june27-140pmroom212v2/24108965",
  "Hortonworks (2014). Hadoop Summit talk on Hadoop 2 [Slides]. slideshare.net/slideshow/t-145p230avavilapalli-mac/35985135",
  "eBay Inc. (c. 2015). Hadoop at eBay [Slides]. slideshare.net/slideshow/hadoop-at-ebay-53095161/53095161",
  "eBay Inc. (2012). HBase: the use case in eBay Cassini [Slides]. slideshare.net/slideshow/8-h-base-the-use-case-in-e-bay-cassini/13138742",
  "eBay Inc. (n.d.). HDFS storage efficiency using tiered storage. eBay Tech Blog.",
  "eBay Inc. (n.d.). Enabling HDFS federation having 1B file system objects. eBay Tech Blog.",
  "eBay Inc. (2014). Announcing Kylin: Extreme OLAP engine for big data. eBay Tech Blog.",
  "Patel, J. (2012). Cassandra at eBay [Slides]. Cassandra Summit 2012.",
  "MongoDB Inc. (n.d.). MongoDB at eBay [Slides]. slideshare.net/mongodb/mongodb-at-ebay",
];
const half = Math.ceil(refs.length / 2);
[refs.slice(0, half), refs.slice(half)].forEach((col, c) => {
  T(col.map((r, i) => ({ text: r, options: { breakLine: i < col.length - 1 } })), {
    x: M + c * 4.6, y: 0.8, w: 4.4, h: 4.4, fontSize: 10, color: C.text1, align: "left", valign: "top", paraSpaceAfter: 5, objectName: "Reference list " + (c + 1),
  });
});

// ---------- write ----------
(async () => {
  await pres.writeFile({ fileName: OUT });
  await applyTheme(OUT, THEME);
  fs.writeFileSync(LOGFILE, JSON.stringify(LOG));
  console.log("wrote", OUT, "slides:", cur);
})().catch((e) => { console.error("BUILD FAILED:", e); process.exit(1); });
