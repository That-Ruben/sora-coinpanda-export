# SORA export web app

Type a SORA address, get back a Coinpanda-ready CSV. The scan itself runs as a
GitHub Actions job (it takes minutes to over an hour, far past what a Vercel
serverless function can run), and this Vercel-hosted frontend just triggers
that job and hands back the result.

## How it fits together

- `exporter/` — the chain-scanning script (`sora-chain-export.js`), unchanged
  from the standalone tool.
- `.github/workflows/export.yml` — runs the exporter on `workflow_dispatch`
  with `address` and `job_id` inputs, uploads `out.csv` as an artifact named
  after `job_id`.
- `api/start.js` — accepts `{ address }`, generates a `job_id`, triggers the
  workflow via the GitHub API.
- `api/status.js` — polled by the frontend; finds the run by matching
  `job_id` against recent runs' names (the run's display name is templated
  from the inputs, since the dispatch call itself doesn't return a run id).
- `api/download.js` — once the run succeeds, downloads the (zipped) artifact
  from GitHub server-side, unzips it, and streams the plain CSV back.
- `index.html` — the form + polling UI.


## Notes

- `exporter/meta_cache/` is committed so the workflow doesn't need to
  re-download SORA runtime metadata on every run.
- Artifacts expire after 7 days (`retention-days: 7` in the workflow) — the
  frontend's "Recent exports" list will still show a job as done past that
  point, but the download will 404.
- A run can take up to the job's `timeout-minutes` (currently 350) before
  GitHub kills it; very large wallets may need this raised.
