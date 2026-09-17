const { findRun } = require('./_github');

module.exports = async (req, res) => {
  const jobId = req.query.jobId;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  let run;
  try {
    run = await findRun(jobId);
  } catch (e) {
    return res.status(502).json({ error: `could not check status: ${e.message}` });
  }

  // Dispatch is async: the run can take a few seconds to appear at all.
  if (!run) return res.json({ state: 'pending' });

  if (run.status !== 'completed') return res.json({ state: 'running', runUrl: run.html_url });

  if (run.conclusion === 'success') return res.json({ state: 'done', runUrl: run.html_url });

  return res.json({ state: 'failed', conclusion: run.conclusion, runUrl: run.html_url });
};
