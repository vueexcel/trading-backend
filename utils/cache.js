const crypto = require('crypto');
const zlib = require('zlib');
const redis = require('../config/redis');

function stableStringify(input) {
  if (input === null || input === undefined) return '';
  if (typeof input !== 'object') return String(input);
  if (Array.isArray(input)) return `[${input.map(stableStringify).join(',')}]`;
  const keys = Object.keys(input).sort();
  return `{${keys.map((k) => `${k}:${stableStringify(input[k])}`).join(',')}}`;
}

function makeCacheKey(prefix, payload) {
  const raw = stableStringify(payload || {});
  const hash = crypto.createHash('sha1').update(raw).digest('hex');
  return `${prefix}:${hash}`;
}

async function getCache(key) {
  if (!redis) return null;
  try {
    const raw = await redis.get(key);
    if (!raw) return null;
    if (typeof raw === 'object' && raw.__encoding === 'gzip-base64' && typeof raw.data === 'string') {
      const json = zlib.gunzipSync(Buffer.from(raw.data, 'base64')).toString('utf8');
      return JSON.parse(json);
    }
    return raw;
  } catch (error) {
    if (process.env.CACHE_DEBUG === '1') {
      console.warn('Cache get failed:', error.message);
    }
    return null;
  }
}

async function setCache(key, value, ttlSeconds) {
  if (!redis) return false;
  try {
    const json = JSON.stringify(value);
    const gz = zlib.gzipSync(Buffer.from(json, 'utf8')).toString('base64');
    const payload = { __encoding: 'gzip-base64', data: gz };
    await redis.set(key, payload, { ex: ttlSeconds });
    return true;
  } catch (error) {
    if (process.env.CACHE_DEBUG === '1') {
      console.warn('Cache set failed:', error.message);
    }
    return false;
  }
}

async function getVersion(namespace) {
  if (!redis) return 1;
  try {
    const key = `cache:version:${namespace}`;
    const value = await redis.get(key);
    const num = Number(value);
    return Number.isFinite(num) && num > 0 ? num : 1;
  } catch (error) {
    return 1;
  }
}

async function bumpVersion(namespace) {
  if (!redis) return 1;
  try {
    const key = `cache:version:${namespace}`;
    const value = await redis.incr(key);
    return Number(value) || 1;
  } catch (error) {
    return 1;
  }
}

module.exports = {
  makeCacheKey,
  getCache,
  setCache,
  getVersion,
  bumpVersion
};
