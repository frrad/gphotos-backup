import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
from unittest.mock import patch
import zipfile

import gphotos_backup as pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        pipeline.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / "config").touch()
        (self.repo / "data").mkdir()
        self.auth = self.root / "rclone.conf"
        self.auth.touch(mode=0o600)
        self.config_path = self.root / "config.json"
        self.config_path.write_text(json.dumps({"repository": str(self.repo), "remote": str(self.root / "remote"),
                                               "rclone_config": str(self.auth), "reserve_gib": 0, "cleanup": True}))
        self.config = pipeline.Config(self.config_path)
        self.config.prepare()
        self.state = pipeline.State(self.config)
        self.addCleanup(self.state.db.close)
        self.space = pipeline.Space(self.config)

    def part(self, suffix="zip", contents=None):
        remote = self.root / "remote"
        remote.mkdir(exist_ok=True)
        number = "001" if suffix == "zip" else "002"
        source = remote / ("takeout-20000101T000000Z-" + number + "." + suffix)
        contents = contents or {"Takeout/Google Photos/album/photo.jpg": b"synthetic photo" * 100,
                                "Takeout/Google Photos/album/photo.jpg.json": b'{"synthetic": true}'}
        if suffix == "zip":
            with zipfile.ZipFile(source, "w") as archive:
                for name, data in contents.items():
                    archive.writestr(name, data)
        else:
            with tarfile.open(source, "w:gz") as archive:
                for name, data in contents.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
        part = pipeline.Part(source.name, "synthetic-id", source.stat().st_size,
                             pipeline.digest(source), source.name.split(".")[0])
        self.state.register(part)
        return part, source

    def initialize_borg(self):
        shutil.rmtree(self.repo)
        subprocess.run(["borg", "init", "--encryption=none", str(self.repo)], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.config.prepare()
        # Reopen the database removed with the placeholder repository.
        self.state.db.close()
        self.state = pipeline.State(self.config)
        self.addCleanup(self.state.db.close)

    def test_traversal_and_tar_links_are_rejected(self):
        for suffix in ("zip", "tgz"):
            with self.subTest(suffix=suffix):
                part, source = self.part(suffix, {"../escape": b"bad"})
                folder = self.config.staging / part.identity
                folder.mkdir(exist_ok=True)
                with self.assertRaises(pipeline.Failure):
                    pipeline.extract(source, folder, part, self.space)
                self.assertFalse((self.config.staging / "escape").exists())
        with tarfile.open(source, "w:gz") as archive:
            member = tarfile.TarInfo("Takeout/link")
            member.type = tarfile.SYMTYPE
            member.linkname = "/etc/passwd"
            archive.addfile(member)
        folder = self.config.staging / part.identity
        folder.mkdir(exist_ok=True)
        with self.assertRaises(pipeline.Failure):
            pipeline.extract(source, folder, part, self.space)

    def test_interrupted_extraction_is_rebuilt(self):
        part, source = self.part()
        folder = self.config.staging / part.identity
        (folder / "extracted").mkdir(parents=True)
        (folder / "extracted" / "incomplete").write_text("partial")
        target, counts = pipeline.extract(source, folder, part, self.space)
        self.assertFalse((target / "incomplete").exists())
        self.assertEqual(counts[0], 2)

    def test_source_change_and_duplicate_names_are_rejected(self):
        part, _ = self.part()
        changed = pipeline.Part(part.path, part.source_id, part.size + 1, part.md5, part.archive)
        with self.assertRaises(pipeline.Failure):
            self.state.register(changed)
        row = {"Path": part.path, "ID": "synthetic-id", "Size": part.size, "Hashes": {"md5": part.md5}}
        with patch.object(self.config, "rclone", return_value=json.dumps([row])):
            self.assertEqual(pipeline.discover(self.config), [part])
        with patch.object(self.config, "rclone", return_value=json.dumps([row, row])):
            with self.assertRaises(pipeline.Failure):
                pipeline.discover(self.config)

    def test_low_space_stops_before_download(self):
        part, _ = self.part()
        self.config.reserve = shutil.disk_usage(self.config.staging).free + 1
        with patch.object(self.config, "rclone") as copy:
            with self.assertRaises(pipeline.Failure):
                pipeline.download(self.config, part, self.space)
            copy.assert_not_called()

    def test_download_checksum_failure_keeps_original(self):
        part, source = self.part()
        def corrupt_copy(*args):
            Path(args[2]).write_bytes(b"corrupt")
        with patch.object(self.config, "rclone", side_effect=corrupt_copy):
            with self.assertRaises(pipeline.Failure):
                pipeline.download(self.config, part, self.space)
        self.assertTrue(source.exists())
        self.assertFalse((self.config.staging / part.identity / "source").exists())

    def test_bounded_parallel_downloads_and_serial_imports(self):
        parts = [pipeline.Part(f"takeout-synthetic-{i:03}.zip", f"synthetic-{i}", i + 1,
                               "0" * 32, f"takeout-synthetic-{i:03}") for i in range(5)]
        barrier = threading.Barrier(2)
        downloaded, imported = [], []
        lock = threading.Lock()
        active = 0
        maximum = 0
        def fetch(config, part, space):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            if part in parts[:2]:
                barrier.wait(timeout=5)
            with lock:
                active -= 1
                downloaded.append(part)
            return self.root / part.archive
        def ingest(config, part, source, state, existing, space):
            imported.append(part)
        with patch.object(pipeline, "download", side_effect=fetch), patch.object(pipeline, "ingest", side_effect=ingest):
            pipeline.run(self.config, parts, self.state, {})
        self.assertEqual(maximum, 2)
        self.assertCountEqual(downloaded, parts)
        self.assertEqual(imported, parts)

    def test_warning_preserves_inputs_and_blocks_automatic_acceptance(self):
        part, source = self.part()
        folder = self.config.staging / part.identity
        folder.mkdir()
        local = folder / "source"
        shutil.copyfile(source, local)
        with patch.object(pipeline, "command", side_effect=pipeline.Failure("borg warning")):
            with self.assertRaises(pipeline.Failure):
                pipeline.ingest(self.config, part, local, self.state, {}, self.space)
        self.assertTrue(local.exists())
        self.assertTrue((folder / "extracted").exists())
        self.assertEqual(self.state.get(part)[0], "review")
        with patch.object(pipeline, "verify"):
            with self.assertRaises(pipeline.Failure):
                pipeline.run(self.config, [part], self.state, {part.archive: {}})

    @unittest.skipUnless(shutil.which("borg") and shutil.which("rclone"), "requires Borg and rclone")
    def test_real_download_import_restore_and_rerun(self):
        self.initialize_borg()
        for suffix in ("zip", "tgz"):
            part, remote = self.part(suffix)
            pipeline.STOP.clear()
            pipeline.run(self.config, [part], self.state, pipeline.archive_map(self.config))
            self.assertEqual(self.state.get(part)[0], "complete")
            self.assertFalse((self.config.staging / part.identity).exists())
            restore = self.root / ("restore-" + suffix)
            restore.mkdir()
            pipeline.command(["borg", "extract", str(self.repo) + "::" + part.archive], cwd=restore)
            self.assertEqual((restore / "Takeout/Google Photos/album/photo.jpg").read_bytes(), b"synthetic photo" * 100)
            pipeline.STOP.clear()
            with patch.object(pipeline, "download", side_effect=AssertionError("must not redownload")):
                pipeline.run(self.config, [part], self.state, pipeline.archive_map(self.config))

    @unittest.skipUnless(shutil.which("borg"), "requires Borg")
    def test_commit_recovery_and_missing_archive_detection(self):
        self.initialize_borg()
        part, source = self.part()
        folder = self.config.staging / part.identity
        folder.mkdir()
        local = folder / "source"
        shutil.copyfile(source, local)
        with patch.object(pipeline, "verify", side_effect=pipeline.Failure("verification interrupted")):
            with self.assertRaises(pipeline.Failure):
                pipeline.ingest(self.config, part, local, self.state, {}, self.space)
        self.assertEqual(self.state.get(part)[0], "created")
        pipeline.run(self.config, [part], self.state, pipeline.archive_map(self.config))
        self.assertEqual(self.state.get(part)[0], "complete")
        self.assertFalse(folder.exists())
        with self.assertRaises(pipeline.Failure):
            pipeline.run(self.config, [part], self.state, {})


if __name__ == "__main__":
    unittest.main()
