#!/usr/bin/env node
/**
 * One-time helper: read service-account.json and print GOOGLE_SERVICE_ACCOUNT_B64=... for .env
 * Usage: node scripts/print-google-credentials-b64.js
 */
const fs = require('fs');
const path = require('path');
const { encodeServiceAccountToBase64 } = require('../utils/googleCredentials');

const file = path.join(__dirname, '..', 'service-account.json');
if (!fs.existsSync(file)) {
    console.error('service-account.json not found next to project root.');
    process.exit(1);
}
const json = fs.readFileSync(file, 'utf8');
const b64 = encodeServiceAccountToBase64(json);
console.log('Paste into .env (single line, no spaces around =):');
console.log('');
console.log('GOOGLE_SERVICE_ACCOUNT_B64=' + b64);
console.log('');
console.log('Then remove or keep local service-account.json; BigQuery will prefer the env var when set.');
