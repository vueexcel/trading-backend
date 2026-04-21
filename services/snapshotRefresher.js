const analyticsData = require('../analyticsData');

const ENABLE = process.env.ENABLE_MARKET_SNAPSHOT_REFRESH === '1';
const INTERVAL_MS = Number(process.env.MARKET_SNAPSHOT_REFRESH_MS || 300000);

let timer = null;
let running = false;

async function runOnce() {
  if (running) return;
  running = true;
  const startedAt = Date.now();
  try {
    const info = await analyticsData.buildMarketSnapshots();
    const elapsed = Date.now() - startedAt;
    console.log(
      `[snapshot-refresher] build ok in ${elapsed}ms (asOf=${info.asOfDate}, snapshotTs=${info.snapshotTs})`
    );
  } catch (err) {
    console.error('[snapshot-refresher] build failed:', err?.message || err);
  } finally {
    running = false;
  }
}

function startSnapshotRefresher() {
  if (!ENABLE) {
    console.log('[snapshot-refresher] disabled (ENABLE_MARKET_SNAPSHOT_REFRESH != 1)');
    return;
  }
  const ms = Number.isFinite(INTERVAL_MS) && INTERVAL_MS > 0 ? INTERVAL_MS : 300000;
  void runOnce();
  timer = setInterval(() => {
    void runOnce();
  }, ms);
  if (typeof timer?.unref === 'function') timer.unref();
  console.log(`[snapshot-refresher] started (${ms}ms interval)`);
}

module.exports = { startSnapshotRefresher };
