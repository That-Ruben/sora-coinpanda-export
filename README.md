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

## One-time setup

1. Push this repo to GitHub (public is fine — the chain data behind these
   CSVs is already public; nothing here needs to be private).
2. Create a GitHub Personal Access Token (fine-grained, scoped to this repo)
   with **Actions: read and write** permission.
3. Deploy this repo to Vercel, and set these environment variables in the
   Vercel project settings:
   - `GITHUB_TOKEN` — the token from step 2
   - `GITHUB_OWNER` — your GitHub username/org
   - `GITHUB_REPO` — this repo's name
   - `GITHUB_REF` — branch to run the workflow from (defaults to `main` if unset)

## Notes

- `exporter/meta_cache/` is committed so the workflow doesn't need to
  re-download SORA runtime metadata on every run.
- GitHub Actions artifacts on the free tier expire (`retention-days: 1` in
  the workflow) — download promptly once a job finishes.
- A run can take up to the job's `timeout-minutes` (currently 350) before
  GitHub kills it; very large wallets may need this raised.
