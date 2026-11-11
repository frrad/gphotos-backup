"""Small, restartable Takeout importer. Runtime inputs never belong in Git."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import threading
import zipfile


class Failure(Exception):
    pass


CHECKOUT = Path(__file__).resolve().parent
PART_NAME = re.compile(r"(takeout-[A-Za-z0-9_-]+)\.(zip|tgz|tar\.gz)$")
STOP = threading.Event()


def command(args, cwd=None):
    # External tools can echo auth/config values. Never relay raw diagnostics.
    with subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, start_new_session=True) as process:
        try:
            while True:
                if STOP.is_set():
                    process.send_signal(signal.SIGINT)
                    process.communicate()
                    raise Failure("Stopped; inputs retained")
                try:
                    output, _ = process.communicate(timeout=0.25)
                    break
                except subprocess.TimeoutExpired:
                    pass
        except KeyboardInterrupt:
            process.send_signal(signal.SIGINT)
            process.communicate()
            raise
        if process.returncode:
            raise Failure(f"{Path(args[0]).name} failed (exit {process.returncode}); inputs retained")
        return output


def digest(path, algorithm="md5"):
    h = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


@dataclass(frozen=True)
class Part:
    path: str
    source_id: str
    size: int
    md5: str
    archive: str

    @property
    def identity(self):
        value = json.dumps([self.source_id, self.size, self.md5], separators=(",", ":"))
        return hashlib.sha256(value.encode()).hexdigest()


class Config:
    def __init__(self, path):
        path = Path(path).expanduser().resolve()
        if path.is_relative_to(CHECKOUT):
            raise Failure("Keep the actual configuration outside the public checkout")
        data = json.loads(path.read_text())
        self.repo = Path(data["repository"]).expanduser().resolve()
        if path.is_relative_to(self.repo):
            raise Failure("Keep configuration and credentials outside the Borg repository")
        if self.repo.is_relative_to(CHECKOUT) or CHECKOUT.is_relative_to(self.repo):
            raise Failure("Code checkout and Borg repository must be separate")
        self.remote = data["remote"].rstrip("/")
        self.auth = Path(data["rclone_config"]).expanduser().resolve()
        if self.auth.is_relative_to(CHECKOUT) or self.auth.is_relative_to(self.repo):
            raise Failure("Keep rclone credentials outside both repositories")
        self.workers = data.get("download_workers", 2)
        if type(self.workers) is not int or not 1 <= self.workers <= 4:
            raise Failure("download_workers must be between 1 and 4")
        self.reserve = int(float(data.get("reserve_gib", 20)) * 1024**3)
        if self.reserve < 0:
            raise Failure("reserve_gib must be nonnegative")
        self.cleanup = data.get("cleanup", False)
        if type(self.cleanup) is not bool:
            raise Failure("cleanup must be true or false")
        self.state = self.repo / "pipeline"
        self.staging = self.repo / "staging"

    def prepare(self):
        if not (self.repo / "config").is_file() or not (self.repo / "data").is_dir():
            raise Failure("Expected an existing local Borg repository")
        for directory in (self.state, self.staging):
            if directory.is_symlink():
                raise Failure("Runtime directories must not be symlinks")
            directory.mkdir(mode=0o700, exist_ok=True)
            directory.chmod(0o700)

    def rclone(self, *args):
        if not self.auth.is_file() or self.auth.stat().st_mode & 0o077:
            raise Failure("rclone config must exist outside the repositories with mode 0600")
        return command(["rclone", "--config", str(self.auth), *args])


def discover(config):
    rows = json.loads(config.rclone("lsjson", config.remote, "--recursive", "--files-only", "--hash"))
    parts, paths, names = [], set(), set()
    for row in rows:
        path = row["Path"]
        match = PART_NAME.fullmatch(PurePosixPath(path).name)
        if not match:
            continue
        safe_path(path)
        name = match[1]
        hashes = {name.lower(): value for name, value in row.get("Hashes", {}).items()}
        md5 = hashes.get("md5", "").lower()
        if not re.fullmatch(r"[0-9a-f]{32}", md5) or not row.get("ID"):
            raise Failure("A Takeout part has no Drive ID or MD5; refusing unverified download")
        if path in paths or name in names:
            raise Failure("Duplicate Takeout paths or archive names; narrow the configured Drive folder")
        if type(row["Size"]) is not int or row["Size"] <= 0:
            raise Failure("Invalid source size")
        paths.add(path)
        names.add(name)
        parts.append(Part(path, row["ID"], row["Size"], md5, name))
    return sorted(parts, key=lambda p: p.archive)


@contextmanager
def locked(config):
    config.prepare()
    with (config.state / "run.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Failure("Another pipeline process is running") from None
        yield


class State:
    def __init__(self, config):
        self.db = sqlite3.connect(config.state / "state.sqlite3")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS parts (
            identity TEXT PRIMARY KEY, archive TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL, status TEXT NOT NULL, files INTEGER, bytes INTEGER)""")
        self.db.commit()

    def register(self, part):
        old = self.db.execute("SELECT identity FROM parts WHERE archive=?", (part.archive,)).fetchone()
        if old:
            if old[0] != part.identity:
                raise Failure("Source content changed for a previously known archive name")
            return
        if self.db.execute("SELECT 1 FROM parts WHERE identity=?", (part.identity,)).fetchone():
            raise Failure("Source is already tracked under a different archive name; review required")
        self.db.execute("INSERT OR IGNORE INTO parts VALUES (?, ?, ?, 'pending', NULL, NULL)",
                        (part.identity, part.archive, json.dumps(part.__dict__)))
        self.db.commit()

    def get(self, part):
        return self.db.execute("SELECT status, files, bytes FROM parts WHERE identity=?", (part.identity,)).fetchone()

    def set(self, part, status, counts=None):
        if counts is None:
            self.db.execute("UPDATE parts SET status=? WHERE identity=?", (status, part.identity))
        else:
            self.db.execute("UPDATE parts SET status=?, files=?, bytes=? WHERE identity=?",
                            (status, *counts, part.identity))
        self.db.commit()


class Space:
    """Reserve bytes for concurrent downloads/extraction before starting them."""
    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.reserved = 0

    @contextmanager
    def claim(self, size):
        with self.lock:
            if shutil.disk_usage(self.config.staging).free < self.config.reserve + self.reserved + size:
                raise Failure("Insufficient staging space; completed work retained, rerun after freeing space")
            self.reserved += size
        try:
            yield
        finally:
            with self.lock:
                self.reserved -= size


def download(config, part, space):
    folder = config.staging / part.identity
    folder.mkdir(mode=0o700, exist_ok=True)
    dest = folder / "source"
    if dest.is_file() and dest.stat().st_size == part.size and digest(dest) == part.md5:
        return dest
    with space.claim(part.size):
        print(f"Downloading a {part.size / 1024**3:.2f} GiB part", flush=True)
        temporary = folder / "downloading"
        config.rclone("copyto", config.remote + "/" + part.path, str(temporary), "--checksum",
                      "--transfers", "1", "--multi-thread-streams", "0")
        if temporary.stat().st_size != part.size or digest(temporary) != part.md5:
            raise Failure("Downloaded part failed size/checksum verification; inputs retained")
        temporary.replace(dest)
    return dest


def safe_path(name):
    path = PurePosixPath(name)
    if not name or "\x00" in name or "\\" in name or path.is_absolute() or ".." in path.parts or ":" in name:
        raise Failure("Unsafe path in source archive")
    return path


def extract(source, folder, part, space):
    target = folder / "extracted"
    marker = folder / "extracted.json"
    # Re-extract after interruption; never trust a partially extracted directory.
    if target.exists():
        shutil.rmtree(target)
    marker.unlink(missing_ok=True)
    target.mkdir(mode=0o700)
    is_zip = part.path.endswith(".zip")
    opener = zipfile.ZipFile if is_zip else tarfile.open
    # TGZ uses a streaming reader: avoid decompressing each 50 GB part twice.
    context = opener(source) if is_zip else opener(source, mode="r|gz")
    with context as archive:
        entries = archive.infolist() if is_zip else archive
        files, total, seen = 0, 0, set()
        for member in entries:
            name = member.filename if is_zip else member.name
            rel = safe_path(name)
            directory = member.is_dir() if is_zip else member.isdir()
            if is_zip:
                mode = member.external_attr >> 16
                if stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR):
                    raise Failure("Links and special files are not allowed in Takeout parts")
            elif not (member.isfile() or member.isdir()):
                raise Failure("Links and special files are not allowed in Takeout parts")
            if not directory:
                if rel in seen:
                    raise Failure("Duplicate file path inside Takeout part")
                seen.add(rel)
                files += 1
            dest = target.joinpath(*rel.parts)
            if directory:
                dest.mkdir(parents=True, exist_ok=True)
                continue
            size = member.file_size if is_zip else member.size
            with space.claim(size):
                dest.parent.mkdir(parents=True, exist_ok=True)
                reader = archive.open(member) if is_zip else archive.extractfile(member)
                with reader, dest.open("xb") as writer:
                    shutil.copyfileobj(reader, writer, length=1024 * 1024)
                if dest.stat().st_size != size:
                    raise Failure("Extracted file size mismatch; inputs retained")
            total += size
            # Stable timestamps improve readability; never restore ownership/mode.
            if not is_zip:
                os.utime(dest, (member.mtime, member.mtime))
        if not files:
            raise Failure("Takeout part contains no files")
    if not (target / "Takeout").is_dir():
        raise Failure("Expected a Takeout directory in the extracted part")
    marker.write_text(json.dumps({"files": files, "bytes": total}))
    return target, (files, total)


def archive_map(config):
    rows = json.loads(command(["borg", "list", "--json", str(config.repo)]))
    return {a["name"]: a for a in rows["archives"]}


def verify(config, part, counts):
    info = json.loads(command(["borg", "info", "--json", f"{config.repo}::{part.archive}"]))
    archive = info["archives"][0]
    if archive.get("comment") != "gphotos-backup:v1:" + part.identity:
        raise Failure("Archive name already exists without matching provenance; manual review required")
    if counts is None or archive["stats"]["nfiles"] != counts[0]:
        raise Failure("Archive file count/size did not match extracted inputs")
    # Borg's original_size includes metadata chunks as well as file contents.
    # Compare payload sizes from the item listing, never the archive total.
    listing = command(["borg", "list", "--json-lines", "--format", "{size}",
                       f"{config.repo}::{part.archive}"])
    files, size = 0, 0
    for line in listing.splitlines():
        item = json.loads(line)
        if not item.get("healthy") or item["type"] not in ("-", "d"):
            raise Failure("Archive contains unhealthy or unexpected items; inputs retained")
        if item["type"] == "-":
            files += 1
            size += item["size"]
    if (files, size) != counts:
        raise Failure("Archive payload file count/size did not match extracted inputs")


def cleanup(config, part):
    # Never delete a source outside our own identity-specific staging folder.
    if config.cleanup:
        shutil.rmtree(config.staging / part.identity, ignore_errors=False)


def ingest(config, part, source, state, existing, space):
    folder = source.parent
    print("Extracting verified part", flush=True)
    target, counts = extract(source, folder, part, space)
    state.set(part, "extracted", counts)
    print(f"Importing {counts[0]} files ({counts[1] / 1024**3:.2f} GiB) into Borg", flush=True)
    # Conservatively allow enough room for a wholly new part in Borg too.
    with space.claim(counts[1]):
        state.set(part, "creating")
        try:
            command(["borg", "create", "--json", "--comment", "gphotos-backup:v1:" + part.identity,
                     f"{config.repo}::{part.archive}", "."], cwd=target)
        except Failure:
            state.set(part, "review")
            raise
        state.set(part, "created")
    # A nonzero exit (including a warning) raises before completion or cleanup.
    verify(config, part, counts)
    state.set(part, "complete", counts)
    existing[part.archive] = {}
    cleanup(config, part)


def run(config, parts, state, existing):
    pending = []
    for part in parts:
        state.register(part)
        status, files, size = state.get(part)
        if part.archive in existing:
            verify(config, part, (files, size) if files is not None else None)
            # An archive made by a warning-producing create is deliberately not
            # accepted on the next run, even if its statistics happen to match.
            if status not in ("created", "complete"):
                raise Failure("Unconfirmed previous Borg result; manual review required before cleanup")
            state.set(part, "complete")
            folder = config.staging / part.identity
            if folder.exists():
                cleanup(config, part)
        elif status == "complete":
            raise Failure("A completed archive is missing from Borg; manual review required")
        else:
            pending.append(part)
    space = Space(config)
    # At most workers parts are downloading/waiting while the current part imports.
    pool = ThreadPoolExecutor(max_workers=config.workers)
    futures = {}
    try:
        for index, part in enumerate(pending):
            while len(futures) < config.workers:
                next_index = index + len(futures)
                if next_index >= len(pending):
                    break
                p = pending[next_index]
                futures[next_index] = pool.submit(download, config, p, space)
            source = futures.pop(index).result()
            ingest(config, part, source, state, existing, space)
            print(f"Imported {index + 1}/{len(pending)} parts", flush=True)
    except BaseException:
        STOP.set()
        raise
    finally:
        for future in futures.values():
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)


def main(argv=None):
    STOP.clear()
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="~/.config/gphotos-backup/config.json")
    parser.add_argument("--match", default="takeout-*", help="Select source archive names with a shell-style glob")
    parser.add_argument("--limit", type=int, help="Process at most this many selected source parts")
    parser.add_argument("action", choices=("plan", "run", "status"))
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    try:
        config = Config(args.config)
        if args.action == "status":
            # Readers must remain available while an import holds the writer lock.
            path = config.state / "state.sqlite3"
            if not path.exists():
                print("{}")
            else:
                with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
                    rows = db.execute("SELECT status, count(*) FROM parts GROUP BY status").fetchall()
                    print(json.dumps(dict(rows)))
            return 0
        with locked(config):
            state = State(config)
            try:
                parts = discover(config)
                parts = [p for p in parts if fnmatch.fnmatchcase(p.archive, args.match)]
                if args.limit:
                    parts = parts[:args.limit]
                existing = archive_map(config)
                for part in parts:
                    state.register(part)
                if args.action == "plan":
                    count = sum(p.archive in existing for p in parts)
                    print(json.dumps({"source_parts": len(parts), "source_bytes": sum(p.size for p in parts),
                                      "existing_archive_names": count, "new_parts": len(parts) - count}))
                    print("Existing names require provenance review; plan does not import or clean up.")
                else:
                    run(config, parts, state, existing)
            finally:
                state.db.close()
        return 0
    except KeyboardInterrupt:
        print("Interrupted; inputs retained. Rerun to reconcile completed stages.", file=sys.stderr)
        return 130
    except Failure as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, sqlite3.Error, tarfile.TarError, zipfile.BadZipFile) as exc:
        # The exception type is safe to show; its message may contain private paths.
        print(f"Configuration, input, or storage error ({type(exc).__module__}.{type(exc).__qualname__}); "
              "private diagnostics suppressed and inputs retained.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
