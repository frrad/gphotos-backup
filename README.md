# Google Photos Takeout to Borg

A restartable command-line pipeline for downloading Google Photos Takeout parts
from Google Drive and importing extracted files into an existing Borg repository.
Requires Python 3.11+, Borg 1.2.x, and rclone. No Python runtime dependencies.

## Setup

Keep credentials and the actual configuration outside the public checkout and
outside the Borg repository. Create a private directory and copy the synthetic
example there, then edit its paths:

```sh
install -d -m 700 ~/.config/gphotos-backup
install -m 600 config.example.json ~/.config/gphotos-backup/config.json
```

Configure an rclone remote named `takeout` using `rclone config`. Select Google
Drive and the `drive.readonly` scope. Store its config outside both repositories,
with mode 0600, and set `rclone_config` to that path. Follow rclone's current
Google OAuth client setup guidance: https://rclone.org/drive/ . Never paste
tokens or client secrets into chat, shell arguments, or this repository.

The `remote` setting is an rclone folder path such as `takeout:Takeout`. Listing
is recursive. Only filenames matching `takeout-*.zip`, `takeout-*.tgz`, or
`takeout-*.tar.gz` are considered. A source Drive ID and MD5 are required.
Duplicate archive names are rejected rather than silently picking one file.

```sh
python3 gphotos_backup.py plan
python3 gphotos_backup.py run
python3 gphotos_backup.py status
python3 gphotos_backup.py --limit 1 run
```

Use `--config /path/to/private/config.json` before the action for a different
configuration. `plan` lists source totals and existing-name counts, and records
the private source manifest in SQLite; it does not download, extract, or create
archives. `status` reads only local state. Commands print summary counts rather
than private filenames. Raw external-tool diagnostics are suppressed because
they can contain authentication values.
Use `--match 'takeout-YYYYMMDD*'` to select an export by archive-name glob and
`--limit 1` for a pilot import. Limits apply to the sorted selected source list,
including completed parts, so omit the limit to process an entire export.

Keep `cleanup` false for the first run. Once satisfied, set it true to remove
identity-specific local staging only after successful Borg creation, metadata
verification, and a durable completion record. Remote files are never deleted.
Older archives without this pipeline's provenance marker require manual review;
an existing name is not automatically considered a successful import.

## Design

For each Takeout part: download, verify, extract, create a Borg archive, verify
completion, persist state, then clean up local staging. Retain media and JSON
sidecars. Process each part separately with stable relative archive paths.

Use bounded download concurrency and one extraction/import worker with one Borg
writer. Downloads can overlap an import. Preserve inputs on failure or warnings.
Completed parts must remain recorded even after local staging is removed.
The default is two download workers (maximum four). The reserve defaults to
20 GiB. Space checks conservatively reserve concurrent download/extraction bytes
and room for a wholly new part in Borg. Insufficient space stops the run with
inputs retained; free space and rerun. It is not a filesystem quota and cannot
control unrelated writers.

Suggested local layout (BORG_REPO is an existing repository):

```
<code-checkout>/           public Git repository: source and sanitized docs
<BORG_REPO>/pipeline/     private state, manifests, and logs
<BORG_REPO>/staging/      private downloads and extracted parts
<private-config-dir>/    credentials, outside both repositories
```

Borg does not automatically archive the pipeline or staging directories. Use
explicit extracted input paths; never back up the repository into itself.

Checkpoint policy initially follows Borg's default (30 minutes in Borg 1.2.4).
An interrupted import restarts its scan and reuses committed chunks. Setting
`--checkpoint-interval 0` disables periodic checkpoints, while a graceful SIGINT
may still request one. A checkpoint is a partial backup, not a completed import.

## Recovery and verification

- Rerun `run` after interruption. Verified downloads are reused. Incomplete
  downloads may restart from zero; byte-range resume is not implemented.
- Extraction restarts from the retained source part, replacing that part's
  extraction directory. Links, devices, duplicate file paths, and escaping paths
  are rejected. Media and JSON sidecars are preserved.
- Borg keeps its default chunking, compression, and checkpoint policy. Stable
  relative paths are stored, without random staging-directory prefixes.
- Every new archive has a source-fingerprint comment. Verification checks that
  marker plus the extracted file count and logical byte total using `borg info`.
  This is metadata verification, not a full readback or `borg check --verify-data`.
- If creation returned zero but verification/completion was interrupted, the next
  run reconciles the existing archive and completes cleanup without downloading.
- A warning/error with an existing archive, or a crash between Borg committing
  and recording its return code, requires manual review. No automatic deletion
  or acceptance occurs. Missing previously completed archives also stop the run.
- A process lock serializes pipeline runs. Ctrl-C asks child processes to stop;
  Borg can finish a graceful checkpoint before the command exits.
- Private state lives in `<BORG_REPO>/pipeline/state.sqlite3`. Protect and back up
  that state separately: it is not included in photo archives automatically.

## Tests

Use persistent scratch space for integration tests:

```sh
TMPDIR=/path/to/scratch python3 -m unittest discover -s tests -v
```

Tests use synthetic files and temporary repositories. Borg/rclone integration
tests are skipped when the corresponding executables are unavailable; they must
run before using a new installation on live backups.

## Privacy

This is a public repository. Never commit credentials, OAuth configuration,
runtime state, real photo inventories, Drive IDs, or logs. Store authentication
outside this checkout and outside the Borg repository. Examples and fixtures
must contain synthetic values only. Review staged changes before publishing.
