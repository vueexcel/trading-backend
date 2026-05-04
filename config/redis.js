const IoRedis = require('ioredis');
const { Redis: UpstashRedis } = require('@upstash/redis');

/**
 * Native Redis (TCP) — Railway plugin sets `REDIS_URL` (often `redis://` or `rediss://`).
 * Values are stored JSON-encoded so behaviour matches the legacy Upstash path.
 */
function createNativeRedisAdapter(redisUrl) {
  const client = new IoRedis(redisUrl, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true
  });
  client.on('error', (err) => {
    console.warn('[redis] connection error:', err.message);
  });

  return {
    async get(key) {
      const s = await client.get(key);
      if (s == null) return null;
      try {
        return JSON.parse(s);
      } catch {
        return s;
      }
    },
    async set(key, value, opts) {
      const ttl = opts && opts.ex;
      const payload = typeof value === 'string' ? value : JSON.stringify(value);
      if (ttl != null && Number.isFinite(Number(ttl))) {
        await client.set(key, payload, 'EX', Number(ttl));
      } else {
        await client.set(key, payload);
      }
    },
    incr(key) {
      return client.incr(key);
    }
  };
}

function createUpstashAdapter() {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  const up = new UpstashRedis({ url, token });
  return {
    get: (key) => up.get(key),
    set: (key, value, opts) => up.set(key, value, opts),
    incr: (key) => up.incr(key)
  };
}

/** Unified cache client or null if Redis is not configured. */
let redis = null;

if (process.env.REDIS_URL) {
  redis = createNativeRedisAdapter(process.env.REDIS_URL);
} else if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
  redis = createUpstashAdapter();
}

module.exports = redis;
