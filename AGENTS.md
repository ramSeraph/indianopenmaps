# Dataset preparation and publication

Use the repository's existing tools when adding datasets. The user specifically
prefers this workflow over manual route edits, individual uploads, or new publishing
wrappers.

## Existing tools

Run from the repository root using the checkout's Python environment:

```bash
python/.venv/bin/iomaps admin prepare --help
python/.venv/bin/iomaps admin make-release --help
```

The `admin` group is hidden from the top-level help. Its implementations are
`python/iomaps/commands/admin_prepare.py` and `admin_release.py`; these replace the
older `utils/prepare.py` and `utils/make_release.py` scripts. Setup and command
examples are in [python/README.md](python/README.md#preparing-and-publishing-datasets).

`prepare` creates the compressed original GeoJSONL, PMTiles, GeoParquet, and ID
analysis. It also handles files that need splitting. `make-release` uploads assets,
updates the dataset's release notes section, then commits and pushes the route
through the local checkout. It verifies GitHub content, the Cloudflare build, and
live endpoints, and saves a receipt. Let the command handle the whole publication.
The implementation is split between `admin_release.py` (CLI and plans),
`admin_publish.py` (Git and releases), and `admin_verify.py` (live checks).

## Sync before local work

Fetch and compare the local branch with upstream before preparing the next dataset
or making a local commit. The old publisher committed routes through GitHub's API,
bypassing the checkout and leaving both local `main` and cached `origin/main`
behind. The current publisher uses local Git and checks upstream automatically,
but other contributors can still advance the remote. Fetch before assuming the
local history or listing is current.

```bash
git status --short --branch
git fetch origin
git log --oneline HEAD..origin/main
git diff --name-only HEAD origin/main
git merge --ff-only origin/main
```

Inspect incoming changes and preserve existing work. A fast-forward can retain
unrelated local edits, as it did when syncing the Delhi route. If histories have
diverged or upstream overlaps local changes, resolve that deliberately without
discarding edits. Sync first, then commit local tooling or documentation changes.
Push authorized local commits before publishing a dataset. The publisher refuses
unrelated unpushed commits or divergent history, rather than pushing them implicitly.
It allows unrelated staged and unstaged edits, and commits only the listing.
Existing edits to the listing must be resolved before publication.

## Workflow

1. Sync with upstream as described above. Read similar entries in
   `worker/src/routes/listing.json`, their Git history,
   and the existing data release. Follow their repository, release tag, filename,
   category, and metadata conventions. Confirm an existing release before
   proposing a new one. Check GitHub too if the local checkout is behind.
2. Choose the final basename first. Rename the working GeoJSONL copy before
   running `prepare`, so the archive member, tile layer, and derived filenames
   agree. Keep working data in ignored `scratch/<dataset>/` and preserve the
   supplied original. If the basename changes, regenerate from the renamed input.
3. Run `prepare` with the source URL and source name. Validate feature counts,
   coordinates, properties, and the ID field; compare with the source service
   where possible. Review any fixes or dropped features reported by the tool.
   Record source geometry problems rather than silently changing them. Check that
   the chosen ID exists in the PMTiles too: GeoParquet conversion may introduce
   `OGC_FID`, which is absent from the tiles. Use `--promoteid` for the source ID.
4. Use `make-release --dry-run --owner <owner>` with all intended publication
   arguments. This is an offline JSON preview containing the exact filenames,
   destinations, route entry, license Markdown, release-note Markdown, and asset
   SHA-256 hashes. Save it under `scratch/` for review. Show the actual license
   text, not a paraphrase. Old previews without a schema version/hash manifest
   must be regenerated; they cannot be executed.
5. When the user requests review, finish preparation and show the concrete preview
   before publishing. Once publication is authorized, execute that exact preview:
   `python/.venv/bin/iomaps admin make-release --plan scratch/<dataset>/release-preview.json`.
   Do not ask for the same approval again. Dataset flags cannot override a saved
   plan. Use `--checkout /path/to/indianopenmaps` if running outside the checkout.
   Do not manually add the listing entry or implement another publishing workflow.
6. Check the generated `release-preview.receipt.json`. The command verifies asset
   hashes, notes, the remote route, the Cloudflare build, live `/api/routes`, route
   `tiles.json`, and a sample tile against the local archive. `--verify-timeout`
   controls the deployment wait. `--no-verify` explicitly skips live checks, so do
   not call the dataset live based on that run. It still checks GitHub content.

**`--no-uploads` is not a dry run.** It requires all planned assets to exist with
matching hashes, then still updates notes and commits/pushes routes.

Publication is not atomic. After interruption, rerun the same `--plan` with the
same receipt path. Matching assets are verified and skipped, notes are replaced
within stable dataset boundaries, and a recorded unpushed route commit can be
retried. Differing assets are never overwritten. An incomplete remote asset or
conflicting local work needs inspection. If intentionally changing a reviewed
plan after an attempt, review it again and use a new `--receipt` path.

## Remote preparation with local route commits

For datasets that do not fit locally, use the same tools on the data server.
Keep the working data, `TMPDIR`, and caches on its data volume, and run long
preparation jobs durably with logs. Check permissions and download completeness
first. Stabilize the input before processing; label incomplete snapshots when
the user has authorized publishing them. Preserve the source and choose the
published basename before preparation. A hard link saves space only when the
source cannot be modified for the duration of the job.

Run `make-release --plan ... --phase assets` on the server, with verification
enabled. This needs data-repository credentials but no Git checkout or route
push credentials. Copy only the exact plan and its `.receipt.json` to this
checkout, then run `make-release --plan ... --phase routes` here. Do not rewrite
the asset paths or copy large assets locally. The local phase validates remote
hashes/notes again, commits and pushes the listing through local Git, and verifies
the live tile using the checksum in the server receipt. See the Python README
for headless module invocations and examples. Keep credentials out of logs and
handoff files; do not copy local SSH keys to the server.

## Authentication

The release utility reads `GH_TOKEN`, then `GITHUB_TOKEN`, then
`.secrets/gh_token.txt` or the legacy `utils/token.txt` in the working directory or
enclosing checkout. The repo token and `gh` CLI login can differ. Never print or
commit credentials. Use `--owner ramSeraph` explicitly for this project's releases.
The token needs Contents read/write on the data repository and read access to the
route repository for verification. Route pushes use the checkout's existing Git
credentials and need push access to `origin`. Successful authentication alone does
not prove upload/push permissions. A sandbox DNS failure is not an invalid token.

## City land-use precedent

The Tamil Nadu DTCP Master Plan is the template used for Delhi:

| Field | Delhi precedent |
| --- | --- |
| Data repository | `ramSeraph/indian_land_features` |
| Existing release | `urban-land-use` |
| Template basename | `TNGIS_DTCP_Master_Plan` |
| Delhi basename | `DDA_Delhi_Master_Plan` |
| Name | `DDA Delhi Land Use Plan` |
| Route | `/not-so-open/urban/regulation/landuse/delhi/dda/` |
| Categories | `landuse`, `delhi` |
| Promote ID | `fid` (`objectid` repeats in the Delhi source) |

Use `regulation` for new routes. The Tamil Nadu route's historical `regultion`
spelling should not be copied; changing an existing published URL is a separate task.
Delhi's 4,722 features were published successfully; do not republish that dataset
just to test tooling. Its source is
https://gis.dda.org.in/server/rest/services/Hosted/MAP_1_LANDUSE_PLAN_FOR_DELHI_UPDATED/FeatureServer/9.
