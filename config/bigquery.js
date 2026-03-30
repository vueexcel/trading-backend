const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');
const { decodeServiceAccountFromBase64 } = require('../utils/googleCredentials');

const SERVICE_ACCOUNT_PATH = path.join(__dirname, '../service-account.json');

/** Prefer explicit name; ACCESS_TOKEN kept for backward compatibility */
const b64FromEnv =
    process.env.GOOGLE_SERVICE_ACCOUNT_B64 || process.env.ACCESS_TOKEN;

let bigquery;

if (b64FromEnv) {
    let credentials;
    try {
        credentials = decodeServiceAccountFromBase64(b64FromEnv);
    } catch (e) {
        throw new Error(
            'Invalid GOOGLE_SERVICE_ACCOUNT_B64 (or ACCESS_TOKEN): ' + e.message
        );
    }
    const projectId =
        process.env.GOOGLE_CLOUD_PROJECT || credentials.project_id;
    if (!projectId) {
        throw new Error(
            'Set GOOGLE_CLOUD_PROJECT or ensure the service account JSON includes project_id'
        );
    }
    bigquery = new BigQuery({
        projectId,
        credentials,
        location: 'US'
    });
} else if (fs.existsSync(SERVICE_ACCOUNT_PATH)) {
    bigquery = new BigQuery({
        keyFilename: SERVICE_ACCOUNT_PATH,
        projectId:
            process.env.GOOGLE_CLOUD_PROJECT || 'extended-byway-454621-s6',
        location: 'US'
    });
} else {
    throw new Error(
        'Google credentials missing: set GOOGLE_SERVICE_ACCOUNT_B64 in .env or add service-account.json'
    );
}

module.exports = bigquery;
