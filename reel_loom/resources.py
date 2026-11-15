import hashlib
from .errors import LoomError


class Resources:
    def __init__(self, db, blobs, imprints):
        self.db, self.blobs, self.imprints = db, blobs, imprints

    def sweep(self, dry_run=True):
        with self.db.write_unit() as db:
            protected = {row[0] for row in db.execute("SELECT blob FROM assets UNION SELECT blob FROM artifacts UNION SELECT blob FROM imprints")}
            unreferenced = db.execute("SELECT hash,size FROM blobs ORDER BY hash").fetchall()
            candidates = [dict(row) for row in unreferenced if row["hash"] not in protected]
            removed = []
            if not dry_run:
                for row in candidates:
                    path = self.blobs.path(row["hash"])
                    path.unlink(missing_ok=True)
                    db.execute("DELETE FROM blobs WHERE hash=?", (row["hash"],))
                    removed.append(row["hash"])
                self.db.event(db, "resources", "swept", {"removed": removed})
            return {"dry_run": dry_run, "protected_count": len(protected), "candidates": candidates,
                    "candidate_bytes": sum(row["size"] for row in candidates), "removed": removed}

    def reconcile(self):
        # Reconciliation is strictly read-only: a storage anomaly can never be
        # promoted into a new delivery fact. Frozen imprint records are never
        # rewritten; every registered blob is re-hashed and every imprint
        # package is validated against its own frozen manifest.
        with self.db.read() as db:
            rows = db.execute("SELECT hash,size FROM blobs ORDER BY hash").fetchall()
            imprints = [self.imprints.get(row["id"], db)
                        for row in db.execute("SELECT id FROM imprints ORDER BY created,id").fetchall()]
        missing, bad_size, bad_digest = [], [], []
        for row in rows:
            key = row["hash"]
            path = self.blobs.path(key)
            if not path.is_file():
                missing.append(key)
                continue
            if path.stat().st_size != row["size"]:
                bad_size.append(key)
                continue
            hasher = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    hasher.update(chunk)
            if hasher.hexdigest() != key:
                bad_digest.append(key)
        known = {row["hash"] for row in rows}
        orphaned = []
        for folder in self.blobs.root.iterdir():
            if not folder.is_dir() or len(folder.name) != 2:
                continue
            for path in folder.iterdir():
                key = folder.name + path.name
                if path.is_file() and len(key) == 64 and key not in known:
                    orphaned.append({"hash": key, "bytes": path.stat().st_size})
        damaged_blobs = set(missing) | set(bad_size) | set(bad_digest)
        imprint_issues = []
        for imprint in imprints:
            issue = self._check_imprint(imprint, damaged_blobs)
            if issue is not None:
                imprint_issues.append(issue)
        return {"consistent": not (missing or bad_size or bad_digest or imprint_issues),
                "missing": missing,
                "bad_size": bad_size,
                "bad_digest": bad_digest,
                "unregistered_blobs": orphaned,
                "imprint_issues": imprint_issues}

    def _check_imprint(self, imprint, damaged_blobs):
        entry = {"id": imprint["id"], "blob": imprint["blob"]}
        if imprint["blob"] in damaged_blobs:
            entry["reason"] = "package_digest_mismatch"
            return entry
        path = self.blobs.path(imprint["blob"])
        try:
            self.imprints.check_package(imprint, path)
        except LoomError as exc:
            entry["reason"] = exc.code
            return entry
        return None
