"""The HTTP interface: routing, security refusals, header validation."""
import json, os, sys, threading, unittest, urllib.error, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import VrambleTest


class Http(VrambleTest):

    def setUp(self):
        super().setUp()
        from http.server import ThreadingHTTPServer
        g = self.vramble
        g.ARB, g.QUEUE, g.CATALOG = self.arb, self.waiting, self.catalog
        g.SUBMIT_TOKEN = "submit-secret"      # free-argv submit: on for the tests, off by default
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), g.H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown()
        super().tearDown()

    def call(self, path, data=None, method="GET", headers=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        req = urllib.request.Request(url, method=method,
                                     data=None if data is None else json.dumps(data).encode(),
                                     headers={"Content-Type": "application/json",
                                              "X-Vramble-Submit": "submit-secret", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            body = e.read()
            try:
                return e.code, json.loads(body or b"{}")
            except Exception:
                return e.code, {"text": body.decode("utf-8", "replace")}

    def test_status_is_readable(self):
        c, r = self.call("/status")
        self.assertEqual(c, 200)
        self.assertIn("queue", r)
        self.assertIsNone(r["holder"])

    def test_service_request_enters_the_queue(self):
        c, r = self.call("/api/requests", {"service": "echo", "prompt": "a lighthouse"}, "POST")
        self.assertEqual(c, 200)
        self.assertEqual(r["state"], "queued")
        self.assertEqual(r["service"], "echo")

    def test_bad_param_is_a_400_that_explains(self):
        c, r = self.call("/api/requests", {"service": "echo", "prompt": "x", "colour": "blue"}, "POST")
        self.assertEqual(c, 400)
        self.assertIn("colour", r["error"])

    def test_no_commands_from_a_web_page(self):
        c, r = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/echo", "hello"]}, "POST",
                           {"Origin": "https://example.invalid"})
        self.assertEqual(c, 403, "a fetch() from a browser must not be able to enqueue commands")
        c, _ = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/echo", "hello"]}, "POST",
                           {"Referer": "https://example.invalid/x"})
        self.assertEqual(c, 403)

    def test_without_origin_a_job_can_be_queued(self):
        c, r = self.call("/api/jobs", {"kind": "comfy", "note": "p", "argv": ["/bin/echo", "hello"]}, "POST")
        self.assertEqual(c, 200)
        self.assertTrue(r["id"].startswith("j"))

    def test_non_numeric_wait_header_is_a_400(self):
        c, r = self.call("/v1/chat/completions", {"model": "x", "messages": []}, "POST",
                           {"X-Wait": "presto"})
        self.assertEqual(c, 400, "a nonsense header must not leave a ghost job in the queue")
        self.assertEqual(len(self.waiting.listing(10, "queued")), 0)

    def test_token_when_configured(self):
        self.vramble.TOKEN = "secret"
        try:
            c, _ = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/true"]}, "POST")
            self.assertEqual(c, 403)
            c, _ = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/true"]}, "POST",
                               {"X-Vramble": "secret"})
            self.assertEqual(c, 200)
        finally:
            self.vramble.TOKEN = ""

    def test_free_argv_submit_is_closed_by_default(self):
        """/api/jobs runs a command chosen by the caller: without a configured token it must not
        answer at all — a client running LLM-written code is not allowed to reach the GPU that way."""
        self.vramble.SUBMIT_TOKEN = ""
        try:
            c, r = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/true"]}, "POST")
            self.assertEqual(c, 403)
            self.assertIn("closed", r["error"])
            c, _ = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/true"]}, "POST",
                               {"X-Vramble-Submit": "guessed"})
            self.assertEqual(c, 403, "no guessed token may open the endpoint")
        finally:
            self.vramble.SUBMIT_TOKEN = "submit-secret"
        c, _ = self.call("/api/requests", {"service": "echo", "prompt": "x"}, "POST")
        self.assertEqual(c, 200, "the catalog stays open: it only runs commands declared in the YAML")

    def test_a_negative_content_length_is_a_400(self):
        """int() accepts -1, and rfile.read(-1) then blocks until the client closes: one free hung
        thread per request. Only non-numeric values used to be rejected."""
        import socket
        s = socket.create_connection(("127.0.0.1", self.port), 5)
        try:
            s.sendall(b"POST /api/requests HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n")
            s.settimeout(5)
            answer = s.recv(200).decode("utf-8", "replace")
        finally:
            s.close()
        self.assertIn(" 400 ", answer, "a negative length must be refused, not read")


    def test_cancel_over_http(self):
        _, j = self.call("/api/jobs", {"kind": "comfy", "argv": ["/bin/true"]}, "POST")
        c, r = self.call(f"/api/jobs/{j['id']}/cancel", {}, "POST")
        self.assertEqual(c, 200)
        self.assertTrue(r["ok"])

    def test_services_listing_endpoint(self):
        c, r = self.call("/api/services")
        self.assertEqual(c, 200)
        self.assertIn("echo", r)

    def test_lease_and_release_over_http(self):
        c, r = self.call("/lease", {"activity": "comfy", "note": "test"}, "POST")
        self.assertEqual(c, 200)
        c2, _ = self.call("/release", {"token": r["token"]}, "POST")
        self.assertEqual(c2, 200)
        self.assertIsNone(self.arb.state()["holder"])

    def test_busy_replies_409_with_the_holder(self):
        self.arb.acquire("research", note="training")
        c, r = self.call("/lease", {"activity": "comfy"}, "POST")
        self.assertEqual(c, 409)
        self.assertEqual(r["holder"]["activity"], "research")
        self.assertIn("training", r["holder"]["note"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
