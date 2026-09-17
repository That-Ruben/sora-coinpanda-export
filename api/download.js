const AdmZip = require('adm-zip');
const { gh, findRun, findArtifact } = require('./_github');

module.exports = async (req, res) => {
  const jobId = req.query.jobId;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  try {
    const run = await findRun(jobId);
    if (!run || run.status !== 'completed' || run.conclusion !== 'success') {
      return res.status(409).json({ error: 'export is not finished successfully yet' });
    }

    const artifact = await findArtifact(run.id, jobId);
    if (!artifact) return res.status(404).json({ error: 'artifact not found (may have expired)' });

    // GitHub Actions artifacts are always served zipped, even for a single file,
    // and the download endpoint needs the same auth token as the rest of the API.
    const zipRes = await gh(`/repos/${run.repository.owner.login}/${run.repository.name}/actions/artifacts/${artifact.id}/zip`);
    const zipBuf = Buffer.from(await zipRes.arrayBuffer());
    const zip = new AdmZip(zipBuf);
    const csvEntry = zip.getEntries().find((e) => e.entryName.endsWith('.csv'));
    if (!csvEntry) return res.status(500).json({ error: 'artifact did not contain a csv' });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${jobId}_sora_import.csv"`);
    res.status(200).send(csvEntry.getData());
  } catch (e) {
    res.status(502).json({ error: `could not fetch export: ${e.message}` });
  }
};
