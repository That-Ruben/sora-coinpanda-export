const crypto = require('crypto');
const { dispatchExport } = require('./_github');

// A SORA (SS58 prefix 69) address: base58, always starts with "cn".
const ADDRESS_RE = /^cn[1-9A-HJ-NP-Za-km-z]{45,49}$/;

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  const { address } = req.body || {};
  if (typeof address !== 'string' || !ADDRESS_RE.test(address)) {
    return res.status(400).json({ error: 'not a valid SORA address' });
  }

  const jobId = crypto.randomUUID();
  try {
    await dispatchExport(address, jobId);
  } catch (e) {
    return res.status(502).json({ error: `could not start export: ${e.message}` });
  }
  res.status(202).json({ jobId });
};
