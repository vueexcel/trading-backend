#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const fetch = require('node-fetch');

const SOURCES = [
  {
    key: 'dowjones',
    urls: [
      'https://www.slickcharts.com/dowjones',
      'https://slickcharts.com/dowjones',
      'https://www.slickcharts.com/dowjones/'
    ]
  },
  {
    key: 'sp500',
    urls: [
      'https://www.slickcharts.com/sp500',
      'https://slickcharts.com/sp500',
      'https://www.slickcharts.com/sp500/'
    ]
  },
  {
    key: 'nasdaq100',
    urls: [
      'https://www.slickcharts.com/nasdaq100',
      'https://slickcharts.com/nasdaq100',
      'https://www.slickcharts.com/nasdaq100/'
    ]
  }
];
const FALLBACK_PREFIX = 'https://r.jina.ai/http://';

/** Latest-only snapshot table (user-specified name). */
const TICKER_WEIGHTS_TABLE_ID = 'Tickeer_Weights';

function loadBigQueryClient() {
  if (process.env.SKIP_BIGQUERY_WEIGHTS_SYNC === '1') {
    return null;
  }
  try {
    // eslint-disable-next-line global-require, import/no-dynamic-require
    return require('../config/bigquery');
  } catch (e) {
    console.warn('BigQuery sync skipped (credentials/config):', e.message);
    return null;
  }
}

/**
 * Replaces all rows in `Tickeer_Weights` with the current scrape (latest only).
 */
async function syncLatestWeightsToBigQuery(payload) {
  const bigquery = loadBigQueryClient();
  if (!bigquery) return;

  const projectId =
    process.env.GOOGLE_CLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    (await bigquery.getProjectId());
  const datasetId = process.env.BIGQUERY_DATASET || 'sp500data1';
  const tableFqn = `\`${projectId}.${datasetId}.${TICKER_WEIGHTS_TABLE_ID}\``;

  const ddl = `
    CREATE TABLE IF NOT EXISTS ${tableFqn} (
      source STRING NOT NULL,
      ticker STRING NOT NULL,
      weight FLOAT64 NOT NULL,
      company STRING,
      fetched_at TIMESTAMP NOT NULL
    )
    CLUSTER BY source, ticker
  `;
  await bigquery.query({ query: ddl });
  await bigquery.query({ query: `TRUNCATE TABLE ${tableFqn}` });

  const rows = [];
  const sources = payload.sources && typeof payload.sources === 'object' ? payload.sources : {};
  for (const [sourceKey, items] of Object.entries(sources)) {
    if (!Array.isArray(items)) continue;
    for (const r of items) {
      const ticker = String(r.symbol || '')
        .trim()
        .toUpperCase();
      const w = Number(r.weight);
      if (!ticker || !Number.isFinite(w)) continue;
      rows.push({
        source: sourceKey,
        ticker,
        weight: w,
        company: String(r.company || ''),
        fetched_at: payload.fetchedAt
      });
    }
  }

  if (!rows.length) {
    throw new Error('No weight rows to sync to BigQuery.');
  }

  const table = bigquery.dataset(datasetId).table(TICKER_WEIGHTS_TABLE_ID);
  await table.insert(rows);
  console.log(`Synced ${rows.length} rows to BigQuery ${datasetId}.${TICKER_WEIGHTS_TABLE_ID} (latest only).`);
}

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

/** Normalize symbol cell to plain ticker (Slickcharts may wrap as `[AAPL](url)` in HTML). */
function normalizeSymbolCell(cell) {
  const s = String(cell || '').trim();
  const m = s.match(/^\[([A-Za-z0-9.\-]+)\]\s*\(/i);
  if (m) return m[1].toUpperCase();
  const plain = s.replace(/[^A-Za-z0-9.\-]/g, '').toUpperCase();
  return plain || s.toUpperCase().trim();
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
    const symbol = normalizeSymbolCell(cells[2]);
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
    const symbol = normalizeSymbolCell(cols[2]);
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

function readExistingPayload(outPath) {
  try {
    if (!fs.existsSync(outPath)) return null;
    const raw = fs.readFileSync(outPath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    if (!parsed.sources || typeof parsed.sources !== 'object') return null;
    return parsed;
  } catch {
    return null;
  }
}

function boolFromArgOrEnv(flag, envKey, defaultValue = false) {
  if (process.argv.includes(flag)) return true;
  const raw = String(process.env[envKey] || '').trim().toLowerCase();
  if (raw === '1' || raw === 'true' || raw === 'yes') return true;
  if (raw === '0' || raw === 'false' || raw === 'no') return false;
  return defaultValue;
}

async function fetchWeightsWithSourceFallbacks(urls) {
  const attempts = [];
  for (const u of urls) {
    try {
      const rows = await fetchWeights(u);
      if (rows.length) return rows;
      attempts.push(`${u} => parsed 0 rows`);
    } catch (e) {
      attempts.push(`${u} => ${e.message}`);
    }
  }
  throw new Error(`All source URLs failed: ${attempts.join(' | ')}`);
}

async function main() {
  const outPathArg = argValue('--out');
  const outPath = outPathArg
    ? path.resolve(process.cwd(), outPathArg)
    : path.resolve(process.cwd(), 'data', 'index-weights.json');
  const allowStale = boolFromArgOrEnv('--allow-stale', 'ALLOW_STALE_WEIGHTS_ON_FAILURE', true);
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
  const existingPayload = readExistingPayload(outPath);
  const staleSources = [];

  for (const src of SOURCES) {
    const fromFile = fileByKey[src.key];
    let items = [];
    if (fromFile) {
      console.log(`Reading ${src.key} from file ${fromFile} ...`);
      const text = fs.readFileSync(fromFile, 'utf8');
      items = parseSlickchartsPage(text);
      if (!items.length) items = parseRowsFromMarkdown(text);
    } else {
      const urls = Array.isArray(src.urls) && src.urls.length ? src.urls : [];
      console.log(`Fetching ${src.key} from ${urls[0] || 'configured source'} ...`);
      try {
        items = await fetchWeightsWithSourceFallbacks(urls);
      } catch (e) {
        if (allowStale) {
          const staleItems = existingPayload?.sources?.[src.key];
          if (Array.isArray(staleItems) && staleItems.length) {
            items = staleItems;
            staleSources.push(src.key);
            console.warn(
              `[warn] Using last saved weights for ${src.key} because remote fetch failed: ${e.message}`
            );
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      }
    }
    if (!items.length) {
      throw new Error(`No rows parsed for ${src.key}.`);
    }
    payload.sources[src.key] = items;
    console.log(`  ${src.key}: ${items.length} rows`);
  }

  if (staleSources.length) {
    console.warn(
      `[warn] Proceeding with stale data for: ${staleSources.join(', ')}. ` +
        'Run with --allow-stale disabled (or ALLOW_STALE_WEIGHTS_ON_FAILURE=0) to fail hard instead.'
    );
  }

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  console.log(`Saved weights to ${outPath}`);

  await syncLatestWeightsToBigQuery(payload);
}

main().catch((err) => {
  console.error('Failed to fetch Slickcharts weights:', err.message);
  console.error(
    'Tip: if remote fetch is blocked, save each page source locally and run with --dow-file <path> --sp500-file <path> --nasdaq-file <path>.'
  );
  process.exit(1);
});

