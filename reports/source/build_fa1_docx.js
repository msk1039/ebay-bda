// Build FA1 report .docx from fa1_report.md. Run: NODE_PATH=/opt/homebrew/lib/node_modules node build_fa1_docx.js
const fs = require('fs');
const path = require('path');
const D = require('docx');
const {
  Document, Packer, Paragraph, TextRun, ExternalHyperlink, Table, TableRow, TableCell, WidthType,
  ShadingType, BorderStyle, AlignmentType, HeadingLevel, LevelFormat, Header, Footer, PageNumber,
  TableOfContents, PageBreak, VerticalAlign,
} = D;

const SRC = path.join(__dirname, 'fa1_report.md');
const OUT = path.join(__dirname, '..', 'FA1_eBay_BigData_Report.docx');
const NAVY = '1F4E79', BLUE = '2E75B6', GREY = '7F7F7F';
const FONT = 'Calibri';
const SHORT_TITLE = 'FA1 — eBay and Big Data';

// ---------- inline parsing ----------
function inline(text, base = {}) {
  const out = [];
  const re = /\\\[|\\\]|\[([^\]]+)\]\(([^)]+)\)|\*\*(.+?)\*\*|\*(.+?)\*/g;
  let last = 0, m;
  const plain = (t, extra = {}) => { if (t) out.push(new TextRun({ text: t, ...base, ...extra })); };
  while ((m = re.exec(text))) {
    plain(text.slice(last, m.index));
    if (m[0] === '\\[') plain('[');
    else if (m[0] === '\\]') plain(']');
    else if (m[2] !== undefined) {
      out.push(new ExternalHyperlink({
        link: m[2],
        children: [new TextRun({ text: m[1], ...base, color: '0563C1', underline: { type: 'single' } })],
      }));
    } else if (m[3] !== undefined) plain(m[3], { bold: true });
    else plain(m[4], { italics: true });
    last = re.lastIndex;
  }
  plain(text.slice(last));
  return out;
}

// ---------- markdown block parser ----------
function parse(md) {
  const lines = md.split('\n');
  const blocks = [];
  // skip H1 title, blank line and the "date · @author" export artifact line (replaced by title page)
  let i = lines.findIndex(l => l.startsWith('## '));
  while (i < lines.length) {
    const ln = lines[i];
    if (!ln.trim()) { i++; continue; }
    let m;
    if ((m = ln.match(/^(#{2,3}) (.*)$/))) { blocks.push({ t: 'h', level: m[1].length - 1, text: m[2] }); i++; continue; }
    if (ln.startsWith('|')) {
      const rows = [];
      while (i < lines.length && lines[i].startsWith('|')) {
        const cells = lines[i].replace(/^\||\|$/g, '').split(' | ').map(s => s.trim());
        rows.push(cells); i++;
      }
      rows.splice(1, 1); // separator row
      blocks.push({ t: 'table', rows }); continue;
    }
    if (/^- /.test(ln)) {
      const items = [];
      while (i < lines.length && /^- /.test(lines[i])) { items.push(lines[i].slice(2)); i++; }
      blocks.push({ t: 'ul', items }); continue;
    }
    if (/^\d+\. /.test(ln)) {
      const items = [];
      while (i < lines.length && /^\d+\. /.test(lines[i])) { items.push(lines[i].replace(/^\d+\. /, '')); i++; }
      blocks.push({ t: 'ol', items }); continue;
    }
    blocks.push({ t: 'p', text: ln }); i++;
  }
  return blocks;
}

// ---------- tables ----------
const TABLE_CAPTIONS = [
  "eBay's scale and data volumes",
  'The 5 Vs at eBay',
  'SQL warehouses versus Hadoop at eBay',
  'HDFS, YARN and MapReduce compared',
  'HBase, Cassandra and MongoDB at eBay',
  'How Hadoop grew at eBay',
  'The seven cross-links between Unit 1 and Unit 2',
  'Where each part appears in the presentation',
  'Viva questions and short answers',
];
const TABLE_WIDTHS = [
  [2900, 1900, 1100, 3126],
  [1100, 2000, 5926],
  [1500, 3000, 4526],
  [1400, 1700, 2900, 3026],
  [1200, 1600, 3400, 2826],
  [2000, 7026],
  [600, 4000, 4426],
  [900, 4900, 3226],
  [3000, 6026],
];
const border = { style: BorderStyle.SINGLE, size: 4, color: 'BFBFBF' };
const borders = { top: border, bottom: border, left: border, right: border };

function buildTable(rows, widths) {
  const trs = rows.map((cells, r) => {
    const header = r === 0;
    const fill = header ? NAVY : (r % 2 === 0 ? 'EAF1F8' : 'FFFFFF');
    return new TableRow({
      tableHeader: header,
      cantSplit: true,
      children: cells.map((c, ci) => new TableCell({
        width: { size: widths[ci], type: WidthType.DXA },
        borders,
        shading: { type: ShadingType.CLEAR, fill, color: 'auto' },
        margins: { top: 80, bottom: 80, left: 120, right: 120 },
        verticalAlign: VerticalAlign.TOP,
        children: [new Paragraph({
          spacing: { before: 0, after: 0, line: 252 },
          children: header
            ? [new TextRun({ text: c, bold: true, color: 'FFFFFF', size: 20 })]
            : inline(c, { size: 20 }),
        })],
      })),
    });
  });
  return new Table({
    width: { size: 9026, type: WidthType.DXA },
    columnWidths: widths,
    rows: trs,
  });
}

const caption = (txt) => new Paragraph({
  keepNext: true,
  spacing: { before: 120, after: 80 },
  children: [new TextRun({ text: txt, italics: true, size: 19, color: '595959' })],
});

// ---------- assemble body ----------
const md = fs.readFileSync(SRC, 'utf8');
const blocks = parse(md);
const body = [];
const numbering = [{
  reference: 'bullets',
  levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT,
    style: { paragraph: { indent: { left: 720, hanging: 360 } } } }],
}];
let olCount = 0, tableNo = 0;

blocks.forEach((b, idx) => {
  if (b.t === 'h') {
    body.push(new Paragraph({
      heading: b.level === 1 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
      children: [new TextRun({ text: b.text })],
    }));
  } else if (b.t === 'p') {
    body.push(new Paragraph({ children: inline(b.text) }));
  } else if (b.t === 'ul') {
    let items = b.items;
    items = items.filter(s => !s.startsWith('**Group details.**')); // deliberate deviation
    items.forEach(s => body.push(new Paragraph({
      numbering: { reference: 'bullets', level: 0 },
      spacing: { after: 80 },
      children: inline(s),
    })));
  } else if (b.t === 'ol') {
    const ref = 'ol' + (++olCount);
    numbering.push({
      reference: ref,
      levels: [{ level: 0, format: LevelFormat.DECIMAL, text: '%1.', alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }],
    });
    b.items.forEach(s => body.push(new Paragraph({
      numbering: { reference: ref, level: 0 },
      spacing: { after: 80 },
      children: inline(s),
    })));
  } else if (b.t === 'table') {
    const n = tableNo++;
    const widths = TABLE_WIDTHS[n];
    if (!widths || widths.length !== b.rows[0].length || widths.reduce((a, c) => a + c, 0) !== 9026)
      throw new Error('bad widths for table ' + (n + 1));
    body.push(caption(`Table ${n + 1}: ${TABLE_CAPTIONS[n]}`));
    body.push(buildTable(b.rows, widths));
    body.push(new Paragraph({ spacing: { before: 0, after: 120 }, children: [] }));
  }
});

// ---------- title page + TOC ----------
const tp = (text, o = {}) => new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { before: o.before || 0, after: o.after || 120 },
  children: [new TextRun({ text, bold: o.bold, size: o.size || 22, color: o.color || '000000' })],
});
const titlePage = [
  tp('Big Data Analytics (B.Tech) — Pimpri Chinchwad College of Engineering (PCCoE)', { size: 24, color: GREY, before: 1800, after: 600 }),
  tp('Formative Assessment 1 (FA1): AI-Based Mind Map on Big Data and Hadoop Ecosystem', { size: 26, bold: true, color: BLUE, after: 600 }),
  tp('eBay and Big Data', { size: 52, bold: true, color: NAVY, after: 200 }),
  tp('From SQL Warehouses to Hadoop and NoSQL — Case Study Report', { size: 28, color: BLUE, after: 900 }),
  tp('Group members: [add names]', { size: 24, after: 120 }),
  tp('Academic year: [add]', { size: 24, after: 600 }),
  tp('Case study: eBay', { size: 24, bold: true, color: NAVY }),
];

const tocSection = [
  new Paragraph({
    spacing: { before: 0, after: 200 },
    children: [new TextRun({ text: 'Table of Contents', bold: true, size: 32, color: NAVY })],
  }),
  new TableOfContents('Table of Contents', { hyperlink: true, headingStyleRange: '1-2' }),
  new Paragraph({
    spacing: { before: 200, after: 0 },
    children: [new TextRun({ text: 'Right-click → Update Field in Word to refresh page numbers.', italics: true, size: 19, color: '595959' })],
  }),
  new Paragraph({ children: [new PageBreak()] }),
];

const pageProps = {
  size: { width: 11906, height: 16838 },
  margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
};

const doc = new Document({
  creator: 'FA1 Group',
  title: 'eBay and Big Data — FA1 Case Study Report',
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: FONT, size: 22 }, paragraph: { spacing: { line: 276, after: 120 } } } },
    paragraphStyles: [
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { font: FONT, size: 32, bold: true, color: NAVY },
        paragraph: { spacing: { before: 360, after: 120 }, keepNext: true, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { font: FONT, size: 26, bold: true, color: BLUE },
        paragraph: { spacing: { before: 240, after: 120 }, keepNext: true, outlineLevel: 1 } },
      { id: 'Heading3', name: 'Heading 3', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { font: FONT, size: 23, bold: true, color: NAVY },
        paragraph: { spacing: { before: 200, after: 100 }, keepNext: true, outlineLevel: 2 } },
    ],
  },
  numbering: { config: numbering },
  sections: [
    { properties: { page: pageProps }, children: titlePage },
    {
      properties: { page: pageProps },
      headers: { default: new Header({ children: [new Paragraph({
        alignment: AlignmentType.RIGHT,
        children: [new TextRun({ text: SHORT_TITLE, size: 18, color: GREY })] })] }) },
      footers: { default: new Footer({ children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [
          new TextRun({ text: 'Page ', size: 18, color: GREY }),
          new TextRun({ children: [PageNumber.CURRENT], size: 18, color: GREY }),
          new TextRun({ text: ' of ', size: 18, color: GREY }),
          new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, color: GREY }),
        ] })] }) },
      children: tocSection.concat(body),
    },
  ],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(OUT, buf);
  console.log('wrote', OUT, buf.length, 'bytes; tables:', tableNo);
});
