# Google Photos Takeout to Borg

A planned, restartable pipeline for downloading Google Photos Takeout exports
from Google Drive and importing extracted files into an existing Borg repository.
Only project scaffolding exists so far; the pipeline is not implemented yet.

## Design

For each Takeout part: download, verify, extract, create a Borg archive, verify
completion, persist state, then clean up local staging. Retain media and JSON
sidecars. Process each part separately with stable relative archive paths.

Use bounded download concurrency and one extraction/import worker with one Borg
writer. Downloads can overlap an import. Preserve inputs on failure or warnings.
Completed parts must remain recorded even after local staging is removed.

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

## Privacy

This is a public repository. Never commit credentials, OAuth configuration,
runtime state, real photo inventories, Drive IDs, or logs. Store authentication
outside this checkout and outside the Borg repository. Examples and fixtures
must contain synthetic values only. Review staged changes before publishing.
