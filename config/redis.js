const IoRedis = require('ioredis');
const { Redis: UpstashRedis } = require('@upstash/redis');

function hasUpstashEnv() {
  return !!(process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN);
}

function withTimeout(promise, ms, label = 'operation') {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
    )
  ]);
}

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
    ping: () => client.ping(),
    disconnect: () => {
      try {
        client.disconnect();
      } catch {
        /* ignore */
      }
    },
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

/**
 * When both Railway `REDIS_URL` and Upstash REST credentials exist:
 * try TCP first (production Railway). If unreachable (e.g. local `.env` with internal Railway host), use Upstash.
 */
async function connectNativeOrFallbackUpstash(redisUrl) {
  const native = createNativeRedisAdapter(redisUrl);
  const timeoutMs = Number(process.env.REDIS_CONNECT_TIMEOUT_MS || 5000);
  try {
    await withTimeout(native.ping(), timeoutMs, 'redis ping');
    console.log('[redis] using TCP (REDIS_URL)');
    return native;
  } catch (err) {
    console.warn('[redis] REDIS_URL unreachable (' + err.message + '), falling back to Upstash REST');
    native.disconnect();
    return createUpstashAdapter();
  }
}

let clientPromise = null;

function getResolvedClient() {
  if (clientPromise) return clientPromise;

  const url = process.env.REDIS_URL;
  const upstashOk = hasUpstashEnv();

  if (!url && !upstashOk) {
    clientPromise = Promise.resolve(null);
  } else if (url && upstashOk) {
    clientPromise = connectNativeOrFallbackUpstash(url);
  } else if (url) {
    clientPromise = Promise.resolve(createNativeRedisAdapter(url));
  } else {
    clientPromise = Promise.resolve(createUpstashAdapter());
  }

  return clientPromise;
}

/** Facade: lazy-connects once; Railway TCP preferred when Upstash is also configured. */
const redisFacade = {
  async get(key) {
    const c = await getResolvedClient();
    if (!c) return null;
    return c.get(key);
  },
  async set(key, value, opts) {
    const c = await getResolvedClient();
    if (!c) return;
    return c.set(key, value, opts);
  },
  async incr(key) {
    const c = await getResolvedClient();
    if (!c) return;
    return c.incr(key);
  }
};

const hasRedisUrl = !!process.env.REDIS_URL;
const hasUpstash = hasUpstashEnv();
module.exports = !hasRedisUrl && !hasUpstash ? null : redisFacade;
