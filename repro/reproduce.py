"""Reproduce/verify the handoff between imprint review, resource reconcile and
interruption recovery when storage bytes are replaced underneath.

Drives the real domain/persistence stack and simulates a storage-disk swap by
overwriting the content-addressed blob file of a frozen delivery package,
including same-size replacements that size-only checks fail to notice.
"""
import json
import shutil
import sys
import tempfile
import zipfile
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, "/workspace")

from reel_loom.application import Application
from reel_loom.config import Settings
from reel_loom.errors import LoomError


def make_sample(app, folder: Path) -> bytes:
    sample = folder / "sample.mp4"
    app.processes.run([
        "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=3",
        "-f", "lavfi", "-i", "sine=frequency=659:sample_rate=48000:duration=3",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest",
        str(sample),
    ])
    return sample.read_bytes()


def build_delivery(root: Path):
    settings = replace(Settings.from_env(), root=root, render_timeout=30)
    app = Application(settings, start_workers=False)
    with tempfile.TemporaryDirectory(prefix="repro-in-") as work:
        content = make_sample(app, Path(work))
        asset = app.assets.ingest(content, "harbor-tone")
        slices = [{"asset": asset["id"], "in": 0.2, "out": 1.2},
                  {"asset": asset["id"], "in": 1.6, "out": 2.8}]
        reel = app.reels.create({"title": "回声样片", "slices": slices})
        app.looks.put("preview", {"kind": "video", "width": 320, "height": 180})
        ticket = app.tickets.enqueue({"reel": reel["id"], "look": "preview"})
        app.renderer.run_once()
        ticket = app.tickets.get(ticket["id"])
        imprint = app.imprints.create(ticket["id"], {"cue_tracks": []})
        return app, imprint


def same_size_variant(data: bytes) -> bytes:
    variant = bytearray(data)
    variant[-1] ^= 0xFF
    return bytes(variant)


def expect_error(action, code, label):
    try:
        action()
    except LoomError as exc:
        assert exc.code == code, (label, exc.code)
        print(" OK  %-44s -> %s" % (label, exc.code))
        return
    raise AssertionError(label + " should have failed with " + code)


def main():
    root = Path(tempfile.mkdtemp(prefix="repro-loom-"))
    try:
        app, imprint = build_delivery(root)
        imprint_id = imprint["id"]
        blob_hash = imprint["blob"]
        blob_path = app.blobs.path(blob_hash)
        original_bytes = blob_path.read_bytes()
        original_manifest_text = json.dumps(imprint["manifest"], sort_keys=True)
        print("== imprint frozen ==")
        print(" id:", imprint_id, " blob:", blob_hash, " size:", len(original_bytes))
        assert app.imprints.verify(imprint_id)["valid"]

        # Scenario A: same-size replacement must be detected by verify and
        # reconcile, and must not become a new delivery fact.
        blob_path.write_bytes(same_size_variant(original_bytes))
        print("\n== A: same-size package swap ==")
        expect_error(lambda: app.imprints.verify(imprint_id), "blob_corrupt",
                     "verify after equal-length swap")
        status = app.resources.reconcile()
        print(" reconcile:", json.dumps(status, ensure_ascii=False))
        assert not status["consistent"]
        assert blob_hash in status["bad_digest"], status
        assert any(i["id"] == imprint_id for i in status["imprint_issues"]), status
        current = app.imprints.get(imprint_id)
        assert current["blob"] == blob_hash and current["manifest"] == imprint["manifest"]
        print(" OK  frozen record untouched by reconcile")

        # Scenario B: foreign but structurally valid zip at the same path.
        blob_path.write_bytes(original_bytes)  # restore content address first
        with zipfile.ZipFile(blob_path, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("manifest.json", json.dumps({
                "schema": 1, "ticket": "foreign", "fingerprint": "0" * 64,
                "snapshot": {}, "files": [
                    {"name": "foreign.mp4", "blob": "f" * 64,
                     "size": 3, "kind": "rendition"}],
                "caption": None}))
            zf.writestr("foreign.mp4", b"abc")
        print("\n== B: foreign valid zip at content address ==")
        report = app.resources.reconcile()
        print(" reconcile:", json.dumps(report, ensure_ascii=False))
        assert not report["consistent"]
        after = app.imprints.get(imprint_id)
        assert after["blob"] == blob_hash, "blob pointer must not be rewritten"
        assert json.dumps(after["manifest"], sort_keys=True) == original_manifest_text
        print(" OK  reconcile reported anomaly without rewriting frozen record")
        expect_error(lambda: app.imprints.verify(imprint_id), "blob_corrupt",
                     "verify on foreign package")

        # Scenario C: restart no longer mutates anything; the historical
        # identity survives the restart and the anomaly stays an anomaly.
        print("\n== C: service restart while storage is still foreign ==")
        app.close()
        settings = replace(Settings.from_env(), root=root, render_timeout=30)
        with Application(settings, start_workers=False) as restarted:
            again = restarted.imprints.get(imprint_id)
            assert again["blob"] == blob_hash
            assert json.dumps(again["manifest"], sort_keys=True) == original_manifest_text
            print(" OK  identity preserved across restart:", again["id"])
            report = restarted.resources.reconcile()
            assert not report["consistent"] and report["imprint_issues"]
            print(" OK  restart reconcile still read-only:",
                  report["imprint_issues"][0]["reason"])
            expect_error(lambda: restarted.imprints.verify(imprint_id), "blob_corrupt",
                         "verify after restart")

        # Scenario D: tampered DB row (manifest overwritten while its package
        # is intact) must not keep impersonating the original delivery.
        root2 = Path(tempfile.mkdtemp(prefix="repro-loom-db-"))
        try:
            app2, imprint2 = build_delivery(root2)
            iid = imprint2["id"]
            with app2.db.write_unit() as db:
                db.execute("UPDATE imprints SET manifest=? WHERE id=?",
                           (json.dumps({"schema": 1, "tampered": True}, sort_keys=True), iid))
            print("\n== D: frozen manifest row overwritten in database ==")
            expect_error(lambda: app2.imprints.verify(iid), "imprint_record_tampered",
                         "verify detects rewritten catalog record")
            report = app2.resources.reconcile()
            assert any(i["id"] == iid and i["reason"] == "imprint_record_tampered"
                       for i in report["imprint_issues"]), report
            print(" OK  reconcile flags record tamper:",
                  [i for i in report["imprint_issues"] if i["id"] == iid][0])
            app2.close()
        finally:
            shutil.rmtree(root2, ignore_errors=True)

        # Scenario E: historical imprint stays independently verifiable after
        # its original source media goes offline (asset blob removed on disk).
        root3 = Path(tempfile.mkdtemp(prefix="repro-loom-hist-"))
        try:
            app3, imprint3 = build_delivery(root3)
            iid, pkg_blob = imprint3["id"], imprint3["blob"]
            source_blob = imprint3["manifest"]["files"][0]["blob"]
            # Take the original source rendition offline, leaving the package.
            app3.blobs.path(source_blob).unlink()
            print("\n== E: source media offline, package intact ==")
            result = app3.imprints.verify(iid)
            assert result["valid"] and result["blob"] == pkg_blob
            print(" OK  historical imprint verifies without online source:",
                  result["verified_files"])
            report = app3.resources.reconcile()
            assert source_blob in report["missing"]
            assert not any(i["id"] == iid for i in report["imprint_issues"]), report
            print(" OK  reconcile reports missing source, package itself sound")
            app3.close()
        finally:
            shutil.rmtree(root3, ignore_errors=True)

        print("\nALL SCENARIOS BEHAVE CORRECTLY")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
