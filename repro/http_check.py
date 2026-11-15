"""HTTP-level check of the verify/reconcile/download handoff after a swap."""
import json
import shutil
import sys
import tempfile
import threading
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, "/workspace")

from reel_loom.application import Application
from reel_loom.api import Api
from reel_loom.client import Client
from reel_loom.config import Settings
from reel_loom.http import MediaHttpServer
from reel_loom.errors import LoomError


def main():
    root = Path(tempfile.mkdtemp(prefix="repro-http-"))
    settings = replace(Settings.from_env(), root=root, render_timeout=30)
    app = Application(settings, start_workers=False)
    server = MediaHttpServer(("127.0.0.1", 0), Api(app))
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={"poll_interval": .02}, daemon=True)
    thread.start()
    client = Client(f"http://127.0.0.1:{server.server_port}/loom/v1")
    try:
        with app.blobs.temporary(".mp4") as sample:
            app.processes.run([
                "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=3",
                "-f", "lavfi", "-i", "sine=frequency=659:sample_rate=48000:duration=3",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac",
                "-shortest", str(sample)])
            content = sample.read_bytes()
        asset = client.ingest(content, "harbor-tone")
        reel = client.create_reel("回声样片", [
            {"asset": asset["id"], "in": .2, "out": 1.2},
            {"asset": asset["id"], "in": 1.6, "out": 2.8}])
        client.put_look("preview", {"kind": "video", "width": 320, "height": 180})
        ticket = client.render(reel["id"], "preview")
        app.renderer.run_once()
        ticket = client.wait(ticket["id"], 5)
        imprint = client.imprint(ticket["id"])
        iid = imprint["id"]

        v1 = client.request("GET", f"/imprints/{iid}/verify")
        v2 = client.request("GET", f"/imprints/{iid}/verify")
        assert v1["valid"] and v2["valid"]
        verify_events = app.db.events(iid)
        verify_events = [e for e in verify_events if e["kind"] == "imprint_verified"]
        assert len(verify_events) == 1, ("duplicate verify events", len(verify_events))
        print("OK repeated verify writes a single audit event")

        status = client.request("GET", "/resources")
        assert status["consistent"], status
        print("OK healthy reconcile:", {k: status[k] for k in
              ("consistent", "missing", "bad_size", "bad_digest", "imprint_issues")})

        path = app.blobs.path(imprint["blob"])
        raw = bytearray(path.read_bytes())
        raw[-1] ^= 0x01  # equal-length in-place tamper
        path.write_bytes(bytes(raw))

        for call in (lambda: client.request("GET", f"/imprints/{iid}/verify"),
                     lambda: client.request("GET", f"/imprints/{iid}/content")):
            try:
                call()
            except LoomError as exc:
                assert exc.code == "blob_corrupt", exc.code
            else:
                raise AssertionError("tampered package was served as valid")
        print("OK verify endpoint and content download both reject the swap")

        status = client.request("GET", "/resources")
        assert not status["consistent"]
        assert imprint["blob"] in status["bad_digest"]
        assert status["imprint_issues"][0]["id"] == iid
        print("OK reconcile flags the swap without rewriting anything:",
              json.dumps(status["imprint_issues"], ensure_ascii=False))

        # Reconcile must stay read-only across repeated calls.
        before = app.imprints.get(iid)
        client.request("GET", "/resources")
        client.request("GET", "/resources")
        after = app.imprints.get(iid)
        assert before["blob"] == after["blob"] and before["manifest"] == after["manifest"]
        print("OK repeated reconcile never mutates the frozen imprint")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        app.close()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
