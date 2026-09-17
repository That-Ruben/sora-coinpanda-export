// Shared helpers for talking to the GitHub Actions REST API.
// Requires env vars: GITHUB_TOKEN (repo scope + workflow), GITHUB_OWNER, GITHUB_REPO.
// The workflow file is .github/workflows/export.yml on the branch in GITHUB_REF (default: main).

const API = 'https://api.github.com';

function repo() {
  const { GITHUB_OWNER, GITHUB_REPO } = process.env;
  if (!GITHUB_OWNER || !GITHUB_REPO) throw new Error('GITHUB_OWNER / GITHUB_REPO env vars not set');
  return `${GITHUB_OWNER}/${GITHUB_REPO}`;
}

async function gh(path, options = {}) {
  const res = await fetch(`${API}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${process.env.GITHUB_TOKEN}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...options.headers,
    },
  });
  if (!res.ok) throw new Error(`GitHub API ${path} -> HTTP ${res.status}: ${await res.text()}`);
  return res;
}

async function dispatchExport(address, jobId) {
  await gh(`/repos/${repo()}/actions/workflows/export.yml/dispatches`, {
    method: 'POST',
    body: JSON.stringify({ ref: process.env.GITHUB_REF || 'main', inputs: { address, job_id: jobId } }),
  });
}

// The run's display name is templated from the inputs (see run-name in export.yml),
// so we find our run by matching job_id against recent runs' names instead of
// needing the run id the dispatch call doesn't return.
async function findRun(jobId) {
  const res = await gh(`/repos/${repo()}/actions/workflows/export.yml/runs?per_page=20`);
  const { workflow_runs } = await res.json();
  return workflow_runs.find((r) => (r.name || '').includes(jobId)) || null;
}

async function findArtifact(runId, jobId) {
  const res = await gh(`/repos/${repo()}/actions/runs/${runId}/artifacts`);
  const { artifacts } = await res.json();
  return artifacts.find((a) => a.name === jobId) || null;
}

module.exports = { gh, repo, dispatchExport, findRun, findArtifact };
