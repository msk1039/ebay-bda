# Shared style for the FA1 and FA2 Word reports

Both reports must look like a matched pair. Build with docx-js (`docx` npm package, installed globally at /opt/homebrew/lib/node_modules/docx; use `NODE_PATH=/opt/homebrew/lib/node_modules node build.js`).

- Page: A4 (11906 x 16838 DXA), margins 1 inch (1440) all sides, portrait.
- Font: Calibri throughout. Body 11 pt, line spacing 1.15, 6 pt after paragraphs.
- Headings (built-in HeadingLevel so the TOC works): H1 16 pt bold navy #1F4E79, 18 pt before / 6 pt after; H2 13 pt bold #2E75B6; H3 11.5 pt bold #1F4E79.
- Title page (own page, no header/footer): course line "Big Data Analytics (B.Tech) — Pimpri Chinchwad College of Engineering (PCCoE)", assessment line (e.g., "Formative Assessment 1 (FA1)"), report title (26 pt bold navy), subtitle (14 pt #2E75B6), then "Group members: [add names]" and "Academic year: [add]" placeholders, then case study line "Case study: eBay".
- Second page: Table of contents (TableOfContents, headings 1-2), with a short italic note "Right-click → Update Field in Word to refresh page numbers."
- Header (from page 3 on, or all non-title pages): report short title, 9 pt grey #7F7F7F, right-aligned. Footer: "Page X of Y" centred, 9 pt grey.
- Tables: full content width 9026 DXA; set columnWidths AND each cell width (DXA); header row bold white text on navy #1F4E79 (ShadingType.CLEAR), body rows alternating white / #EAF1F8; cell margins ~80/120 DXA; 10 pt text; borders light grey #BFBFBF single 4.
- Figures: centred image, width ~6.0 in max (keep aspect ratio from the PNG's real pixel size), followed by a caption paragraph "Figure N: <takeaway>" in 9.5 pt italic grey, centred; number figures sequentially. Tables get a caption above: "Table N: ..." 9.5 pt italic.
- Code snippets (FA2 only): Consolas 9 pt in a single-cell table with light grey fill #F2F2F2, no more than ~15 lines each.
- Lists: use numbering config (LevelFormat.BULLET / DECIMAL), never literal bullet characters.
- Links: ExternalHyperlink, blue underlined.
- No emoji, no decorative graphics.
