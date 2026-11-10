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

## Development

Keep the orchestrator small and its stages independently restartable. Tests
should cover recovery, failure handling, and cleanup boundaries with synthetic
inputs and temporary Borg repositories. Never test mutations on live backups.
