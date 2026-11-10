# Agent instructions

This repository is PUBLIC. It contains code and sanitized documentation for a
Google Takeout to Borg pipeline. Treat every committed byte as publicly visible.

## Credentials and private data

- Keep OAuth client secrets, refresh/access tokens, rclone config, Borg keys,
  passphrases, and dotenv files outside this checkout. Use the user's existing
  credential store or a private config directory, with restrictive permissions.
- Never print credentials or put them in command arguments, logs, examples,
  tests, fixtures, Git remotes, commits, issues, or pull requests.
- Do not commit real Drive IDs, account details, photo filenames, album names,
  inventories, runtime databases, logs, or downloaded/extracted content.
- Examples and tests must use synthetic data and obvious placeholders.
- Review the full staged diff and tracked file list before committing or pushing;
  .gitignore is only a second line of defense.

## Storage and import design

- The existing Borg repository is live backup storage. Use Borg commands to
  manage its contents; never edit its internal config, data, indexes, or locks.
- Runtime state and staging may live in dedicated pipeline/ and staging/
  subdirectories of that Borg repository. Those directories are ordinary files,
  not automatically backed up by Borg. Never archive the repository directory.
- Keep credentials outside the Borg repository as well as outside this checkout.
- Stage on persistent disk, not /tmp (which may be a small RAM filesystem).
- Support both ZIP and .tgz Takeout parts, preserve media and JSON sidecars, and
  extract each part separately. Reject extraction paths that escape staging.
- Use one Borg writer, bounded download concurrency, and a free-space threshold.
- Never delete a downloaded part or its extracted content after a failed or
  warning-producing import. Verify the archive and persist completion first.
- Use durable state keyed by source identity/checksum and deterministic archive
  names. Reconcile existing archives after interruption before retrying.
- Borg checkpoints permit chunk reuse on a fresh scan, not exact-offset resume.
  Do not add a custom five-minute interval by default; use Borg's default unless
  the user chooses a different policy.
- Do not delete remote Drive files, prune archives, compact, repair, or destroy
  the repository as part of normal ingestion.

## Operating and monitoring

- Read the private runtime configuration before changing an active run. Example
  defaults are not the live settings; preserve intentional concurrency, cleanup,
  and space-reserve choices. Worker settings are loaded at process startup.
- The importer supports 1–4 download workers and one extraction/import worker.
  Space reservations stop work before breaching the configured reserve, but are
  not a filesystem quota and cannot account for unrelated writers.
- Keep cleanup disabled for a pilot; validate a restore before enabling it.
  Enabled cleanup removes the completed part's entire local staging directory,
  including its ZIP/TGZ and extracted files. Keep durable completion state and
  leave Drive originals untouched.
- Run long imports in a persistent BB terminal. Check the existing process
  before launching another. To restart, request a graceful Ctrl-C and confirm
  exit before launching its replacement; allow Borg to finish checkpointing.
  Verified downloads can be reused, but partial downloads may restart from zero.
- Use the read-only `status` action while an import is running. Do not acquire
  the pipeline writer lock just to monitor progress. When replacing an import
  terminal, update any scheduled monitor to watch the replacement.
- Progress updates should report aggregate completed parts, current stage, free
  space, and measured download throughput without exposing private inventories.
  When providing an ETA, distinguish download completion from verified Borg
  import completion; use observed rates and state uncertainty or stalls.
- Archive provenance, file counts, and logical sizes are metadata verification,
  not a full restore or data-integrity check. Existing names alone are not proof
  of success. Warning-producing or uncertain Borg results require review; never
  force completion or delete staging merely to get a retry past that safeguard.

## Development

Keep the orchestrator small and its stages independently restartable. Tests
should cover recovery, failure handling, and cleanup boundaries with synthetic
inputs and temporary Borg repositories. Never test mutations on live backups.
