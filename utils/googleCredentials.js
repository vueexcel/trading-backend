/**
 * Encode service account JSON to Base64 for GOOGLE_SERVICE_ACCOUNT_B64 in .env.
 * @param {string | object} input - Raw JSON string or parsed object
 * @returns {string}
 */
function encodeServiceAccountToBase64(input) {
    const str = typeof input === 'string' ? input : JSON.stringify(input);
    return Buffer.from(str, 'utf8').toString('base64');
}

/**
 * Decode Base64 from env back to the service account object used by @google-cloud/* clients.
 * @param {string} b64
 * @returns {object}
 */
function decodeServiceAccountFromBase64(b64) {
    if (b64 == null || String(b64).trim() === '') {
        throw new Error('Base64 string is empty');
    }
    const json = Buffer.from(String(b64).trim(), 'base64').toString('utf8');
    return JSON.parse(json);
}

module.exports = {
    encodeServiceAccountToBase64,
    decodeServiceAccountFromBase64
};
