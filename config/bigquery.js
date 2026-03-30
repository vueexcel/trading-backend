// const { BigQuery } = require('@google-cloud/bigquery');
// const path = require('path');

// // Initialize BigQuery with the key file
// const bigquery = new BigQuery({
//   keyFilename: path.join(__dirname, '../service-account.json'),
//   projectId: 'extended-byway-454621-s6', // <-- REPLACE THIS
// });

// module.exports = bigquery;
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');
const os = require('os');

let bigquery;

if (process.env.ACCESS_TOKEN) {
  // Decode base64 service account from env var, similar to Python
  const b64Json = process.env.ACCESS_TOKEN;
  const decoded = Buffer.from(b64Json, 'base64');
  const tempDir = os.tmpdir();
  const tempFile = path.join(tempDir, 'service-account-temp.json');
  fs.writeFileSync(tempFile, decoded);
  bigquery = new BigQuery({
    keyFilename: tempFile,
    projectId: process.env.GOOGLE_CLOUD_PROJECT,
    location: 'US'
  });
} else {
  // Use the existing service-account.json file
  bigquery = new BigQuery({
    keyFilename: path.join(__dirname, '../service-account.json'),
    projectId: 'extended-byway-454621-s6',
    location: 'US'
  });
}

module.exports = bigquery;