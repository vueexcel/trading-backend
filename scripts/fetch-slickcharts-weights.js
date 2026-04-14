#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');

const SOURCES = [
  { key: 'dowjones', url: 'https://www.slickcharts.com/dowjones' },
  { key: 'sp500', url: 'https://www.slickcharts.com/sp500' },
  { key: 'nasdaq100', url: 'https://www.slickcharts.com/nasdaq100' }
];
const FALLBACK_PREFIX = 'https://r.jina.ai/http://';

function stripTags(html) {
  return String(html || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function parsePercent(text) {
  const s = String(text || '')
    .replace(/[%\s]/g, '')
    .replace(/,/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseRowsFromTable(tableHtml) {
  const rows = [];
  const trMatches = tableHtml.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  for (const tr of trMatches) {
    const cellMatches = tr.match(/<(td|th)[^>]*>[\s\S]*?<\/\1>/gi) || [];
    const cells = cellMatches.map((c) => stripTags(c));
    if (cells.length < 5) continue;

    const first = String(cells[0] || '').toLowerCase();
    if (first.includes('company') || first.includes('symbol') || first.includes('weight')) continue;

    // Slickcharts columns are typically: #, Company, Symbol, Weight, Price, Chg, % Chg
    const symbol = String(cells[2] || '').toUpperCase().trim();
    const weight = parsePercent(cells[3]);
    if (!symbol || weight == null) continue;

    rows.push({
      symbol,
      company: cells[1] || '',
      weight
    });
  }
  return rows;
}

function parseSlickchartsPage(html) {
  const tableMatch = String(html).match(/<table[\s\S]*?<\/table>/i);
  if (!tableMatch) return [];
  return parseRowsFromTable(tableMatch[0]);
}

function parseRowsFromMarkdown(md) {
  const rows = [];
  const lines = String(md || '').split(/\r?\n/);
  for (const line of lines) {
    if (!line.includes('|')) continue;
    const cols = line
      .split('|')
      .map((x) => x.trim())
      .filter(Boolean);
    if (cols.length < 4) continue;
    if (cols[0] === '#' || cols[0].toLowerCase() === 'company') continue;
    const symbol = String(cols[2] || '').toUpperCase().trim();
    const weight = parsePercent(cols[3]);
    if (!symbol || weight == null) continue;
    rows.push({ symbol, company: cols[1] || '', weight });
  }
  return rows;
}

async function fetchWeights(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      accept: 'text/html,application/xhtml+xml',
      referer: 'https://www.slickcharts.com/',
      'accept-language': 'en-US,en;q=0.9'
    }
  });
  if (res.ok) {
    const html = await res.text();
    const rows = parseSlickchartsPage(html);
    if (rows.length) return rows;
  }

  // Fallback via jina proxy mirror when direct fetch is blocked (e.g., HTTP 403).
  const fallbackUrl = FALLBACK_PREFIX + url.replace(/^https?:\/\//, '');
  const fb = await fetch(fallbackUrl, {
    headers: {
      'user-agent': 'Mozilla/5.0',
      accept: 'text/plain,text/markdown'
    }
  });
  if (!fb.ok) {
    throw new Error(`HTTP ${res.status} (direct) and HTTP ${fb.status} (fallback) for ${url}`);
  }
  const text = await fb.text();
  const fallbackRows = parseRowsFromMarkdown(text);
  if (!fallbackRows.length) {
    throw new Error(`No rows parsed from direct or fallback response for ${url}`);
  }
  return fallbackRows;
}

function argValue(name) {
  const idx = process.argv.findIndex((a) => a === name);
  if (idx < 0) return null;
  return process.argv[idx + 1] || null;
}

async function main() {
  const outPathArg = argValue('--out');
  const outPath = outPathArg
    ? path.resolve(process.cwd(), outPathArg)
    : path.resolve(process.cwd(), 'data', 'index-weights.json');
  const dowFileArg = argValue('--dow-file');
  const sp500FileArg = argValue('--sp500-file');
  const nasdaqFileArg = argValue('--nasdaq-file');
  const fileByKey = {
    dowjones: dowFileArg ? path.resolve(process.cwd(), dowFileArg) : null,
    sp500: sp500FileArg ? path.resolve(process.cwd(), sp500FileArg) : null,
    nasdaq100: nasdaqFileArg ? path.resolve(process.cwd(), nasdaqFileArg) : null
  };

  const fetchedAt = new Date().toISOString();
  const payload = { fetchedAt, sources: {} };

  for (const src of SOURCES) {
    const fromFile = fileByKey[src.key];
    let items = [];
    if (fromFile) {
      console.log(`Reading ${src.key} from file ${fromFile} ...`);
      const text = fs.readFileSync(fromFile, 'utf8');
      items = parseSlickchartsPage(text);
      if (!items.length) items = parseRowsFromMarkdown(text);
    } else {
      console.log(`Fetching ${src.url} ...`);
      items = await fetchWeights(src.url);
    }
    if (!items.length) {
      throw new Error(`No rows parsed for ${src.key}.`);
    }
    payload.sources[src.key] = items;
    console.log(`  ${src.key}: ${items.length} rows`);
  }

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  console.log(`Saved weights to ${outPath}`);
}

main().catch((err) => {
  console.error('Failed to fetch Slickcharts weights:', err.message);
  console.error(
    'Tip: if remote fetch is blocked, save each page source locally and run with --dow-file <path> --sp500-file <path> --nasdaq-file <path>.'
  );
  process.exit(1);
});

