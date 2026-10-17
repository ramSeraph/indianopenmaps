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
analysis. It also handles files that need splitting. `make-release` discovers the
related assets, uploads them to an existing GitHub release, appends release notes,
and commits the route to `indianopenmaps/main`. Let that command do all three jobs.
It updates GitHub directly; it does not update the local checkout's listing.

## Sync before local work

Fetch and compare the local branch with upstream before preparing the next dataset
or making a local commit. `make-release` creates a route commit on GitHub's `main`
through the API, bypassing this checkout. A successful publication can therefore
leave both local `main` and the cached `origin/main` behind. Local history and the
listing are not current until upstream has been fetched and integrated.

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
Repeat this check after a publication before the next local commit; a release
utility run does not perform a local `git pull`.

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
   Record source geometry problems rather than silently changing them.
4. Use `make-release --dry-run --owner <owner>` with all intended publication
   arguments. This is an offline JSON preview containing the exact filenames,
   destinations, route entry, license Markdown, and release-note Markdown. Save
   it under `scratch/` for review. Show the actual license text, not a paraphrase.
5. When the user requests review, finish preparation and show the concrete preview
   before publishing. Once publication is authorized, run the same command without
   `--dry-run`; do not ask for the same approval again. Do not manually add the
   listing entry or implement another upload/release/route workflow.
6. Verify uploaded sizes/checksums, release notes, and the remote route. Check the
   Cloudflare Workers build and live `/api/routes`, route `tiles.json`, and a tile
   before claiming the dataset is live. Do not deploy unrelated local changes.

**`--no-uploads` is not a dry run.** It still updates release notes and commits
routes. After an interrupted or failed publication, inspect remote state before
retrying: the workflow is not atomic and notes can be appended twice.

## Authentication

The release utility reads `GH_TOKEN`, then `GITHUB_TOKEN`, then
`.secrets/gh_token.txt` or the legacy `utils/token.txt` in the working directory or
enclosing checkout. The repo token and `gh` CLI login can differ. Never print or
commit credentials. Use `--owner ramSeraph` explicitly for this project's releases.
The token needs access to both the data repository and `ramSeraph/indianopenmaps`,
with Contents read/write permissions. Successful authentication alone does not
prove upload permissions. A sandbox DNS failure is not proof of an invalid token.

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
