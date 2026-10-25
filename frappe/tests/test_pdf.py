# Copyright (c) 2018, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
# //// Neoffice — needed for the new Chrome-path CDP wait_for_event tests below (3a34694 "fix(pdf): one way to wait for a CDP event, and the first tests of the chrome path")
import asyncio
import io

from pypdf import PdfReader

import frappe
import frappe.utils.pdf as pdfgen
from frappe.core.doctype.file.test_file import make_test_image_file
from frappe.tests.utils import FrappeTestCase


def blank_pdf() -> bytes:
	from pypdf import PdfWriter

	writer = PdfWriter()
	writer.add_blank_page(width=72, height=72)
	stream = io.BytesIO()
	writer.write(stream)
	return stream.getvalue()


class TestPdf(FrappeTestCase):
	@property
	def html(self):
		return """<style>
			.print-format {
			 margin-top: 0mm;
			 margin-left: 10mm;
			 margin-right: 0mm;
			}
			</style>
			<p>This is a test html snippet</p>
			<div class="more-info">
				<a href="http://test.com">Test link 1</a>
				<a href="/about">Test link 2</a>
				<a href="login">Test link 3</a>
				<img src="/assets/frappe/test.jpg">
			</div>
			<div style="background-image: url('/assets/frappe/bg.jpg')">
				Please mail us at <a href="mailto:test@example.com">email</a>
			</div>"""

	def runTest(self):
		self.test_read_options_from_html()

	def test_read_options_from_html(self):
		_, html_options = pdfgen.read_options_from_html(self.html)
		self.assertTrue(html_options["margin-top"] == "0")
		self.assertTrue(html_options["margin-left"] == "10mm")
		self.assertTrue(html_options["margin-right"] == "0")

		html_1 = """<style>
			.print-format {
				margin-top: 0mm;
				margin-left: 10mm;
			}
			.print-format .more-info {
				margin-right: 15mm;
			}
			.print-format, .more-info {
				margin-bottom: 20mm;
			}
			</style>
			<div class="more-info">Hello</div>
		"""
		_, options = pdfgen.read_options_from_html(html_1)

		self.assertTrue(options["margin-top"] == "0")
		self.assertTrue(options["margin-left"] == "10mm")
		self.assertTrue(options["margin-bottom"] == "20mm")
		# margin-right was for .more-info (child of .print-format)
		# so it should not be extracted into options
		self.assertFalse(options.get("margin-right"))

	def test_empty_style(self):
		html = """<style></style>
			<div class="more-info">Hello</div>
		"""
		_, options = pdfgen.read_options_from_html(html)
		self.assertTrue(options)

	def test_print_styles_are_read_without_fetching_css_imports(self):
		import threading
		from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

		requested = []

		class StylesheetHandler(BaseHTTPRequestHandler):
			def do_GET(self):
				requested.append(self.path)

		server = ThreadingHTTPServer(("127.0.0.1", 0), StylesheetHandler)
		threading.Thread(target=server.serve_forever, daemon=True).start()
		self.addCleanup(server.server_close)
		self.addCleanup(server.shutdown)
		self.addCleanup(frappe.set_user, "Administrator")
		frappe.set_user("test@example.com")

		pdfgen.read_options_from_html(
			f'<style>@import url("http://127.0.0.1:{server.server_port}/print.css");</style><p>row</p>'
		)
		self.assertEqual(requested, [])

	def test_pdf_encryption(self):
		password = "qwe"
		pdf = pdfgen.get_pdf(self.html, options={"password": password})
		reader = PdfReader(io.BytesIO(pdf))
		self.assertTrue(reader.is_encrypted)
		self.assertTrue(reader.decrypt(password))

	def test_smart_shrinking_is_opt_in(self):
		from unittest.mock import patch

		captured = {}

		def capture_options(html, options=None, verbose=True):
			captured.clear()
			captured.update(options or {})
			return blank_pdf()

		with (
			patch.object(pdfgen.pdfkit, "from_string", side_effect=capture_options),
			patch.object(pdfgen, "get_wkhtmltopdf_version", return_value="0.12.6"),
		):
			pdfgen.get_pdf(self.html)
			self.assertIn("disable-smart-shrinking", captured)

			pdfgen.get_pdf(self.html, smart_shrinking=True)
			self.assertNotIn("disable-smart-shrinking", captured)

	def test_report_pdf_is_scaled_to_fit_the_page(self):
		from unittest.mock import patch

		from frappe.utils import print_format

		with patch.object(print_format, "get_pdf", return_value=blank_pdf()) as get_report_pdf:
			print_format.report_to_pdf("<table><tr><td>a wide report</td></tr></table>")

		self.assertTrue(get_report_pdf.call_args.kwargs["smart_shrinking"])

	def test_pdf_generation_as_a_user(self):
		frappe.set_user("Administrator")
		pdf = pdfgen.get_pdf(self.html)
		self.assertTrue(pdf)

	def test_private_images_in_pdf(self):
		with make_test_image_file(private=True) as file:
			html = f""" <div>
				<img src="{file.file_url}" class='responsive'>
				<img src="{file.unique_url}" class='responsive'>
			</div>
			"""

			pdf = pdfgen.get_pdf(html)

		# If image was actually retrieved then size will be  in few kbs, else bytes.
		self.assertGreaterEqual(len(pdf), 10_000)


# //// Neoffice — added class (no upstream equivalent). The Chrome PDF generator had NO
# //// automated coverage at all, which is how the same defect shipped twice: a caller
# //// waits for a CDP event, asyncio.wait_for CANCELS the future on timeout, and the
# //// caller dereferences it anyway -> a bare CancelledError surfacing as an HTTP 500
# //// with asyncio in the traceback and no cause (#291, 2026-09-08).
# //// These tests need no Chromium: a future that never resolves is the whole fixture.
class TestChromePdfEventTimeouts(FrappeTestCase):
	def _client(self):
		"""A CDP client that is never connected — __init__ opens no socket."""
		from frappe.utils.pdf_generator.cdp_connection import CDPSocketClient

		client = CDPSocketClient("ws://127.0.0.1:1/never-connected")
		self.addCleanup(asyncio.set_event_loop, None)
		self.addCleanup(client.loop.close)
		return client

	def test_wait_for_event_or_throw_reports_the_timeout_and_cleans_up(self):
		client = self._client()
		never_resolves = client.loop.create_future()
		dropped = []

		with self.assertRaises(frappe.ValidationError):
			client.wait_for_event_or_throw(
				never_resolves, timeout=0.01, cleanup=lambda: dropped.append("listener")
			)

		# asyncio.wait_for cancels the future it was waiting on — this IS the trap.
		self.assertTrue(never_resolves.cancelled())
		self.assertEqual(dropped, ["listener"], "cleanup must run before we raise")

	def test_get_pdf_stream_id_raises_a_message_not_a_cancelled_future(self):
		"""Regression for #291: latent path, but the same cancelled future."""
		from frappe.utils.pdf_generator.page import Page

		client = self._client()
		client.wait_for_event = lambda event, timeout=15: False  # the timed-out path

		cancelled = client.loop.create_future()
		cancelled.cancel()  # exactly what wait_for leaves behind

		page = Page.__new__(Page)  # no browser, no tab: only the wait path is under test
		page.session = client
		page.wait_for_pdf = cancelled

		# Without the guard this raises asyncio.CancelledError, which is not an Exception
		# subclass on 3.8+ and so escapes every handler up to the HTTP 500.
		with self.assertRaises(frappe.ValidationError):
			page.get_pdf_stream_id()


# //// Neoffice — added class (no upstream equivalent). The Chrome generator now inlines the private
# //// images as wkhtmltopdf does (get_chrome_pdf, neoffice-maintenance#1117): outside a web request
# //// its page has no session cookie, and a franking stamp, private because it carries the
# //// recipient's address, vanished from a PDF rendered by a background job. No Chromium here: the
# //// browser is a fake that keeps the HTML it is handed.
class TestChromePdfPrivateImages(FrappeTestCase):
	def test_a_private_image_reaches_the_chrome_page_inline(self):
		from unittest.mock import patch

		handed = []

		class Browser:
			def __init__(self, generator, print_format, html, options):
				handed.append(html)

		with (
			make_test_image_file(private=True) as file,
			patch("frappe.utils.pdf_generator.browser.Browser", Browser),
			patch("frappe.utils.pdf_generator.chrome_pdf_generator.ChromePDFGenerator"),
			patch("frappe.utils.pdf_generator.pdf_merge.PDFTransformer"),
		):
			pdfgen.get_chrome_pdf(None, f'<div><img src="{file.file_url}"></div>', {}, None, "chrome")

		self.assertEqual(len(handed), 1)
		self.assertIn("data:image/", handed[0])
		self.assertNotIn("/private/files/", handed[0])


# //// Neoffice — added test for the adaptation of upstream 7277896261 (the Chrome PDF stream read in large chunks).
class TestChromePdfStreamChunks(FrappeTestCase):
	"""The PDF Chromium draws is read from its stream in large chunks, whatever size it answers with, and a chunk
	always fits the websocket's message limit once Chromium has base64-encoded it."""

	def _page_that_answers(self, payload: bytes, at_most: int | None = None):
		import base64
		from unittest import mock

		calls = []

		def send(method, params=None):
			calls.append((method, params))
			if method == "IO.read":
				piece = payload[params["offset"] : params["offset"] + params["size"]]
				if at_most:
					piece = piece[:at_most]
				return {
					"data": base64.b64encode(piece).decode(),
					"base64Encoded": True,
					"eof": params["offset"] + len(piece) >= len(payload),
				}, None
			return {}, None

		return mock.Mock(send=send), calls

	def _read(self, page):
		from frappe.utils.pdf_generator.page import Page

		return Page.get_pdf_from_stream(page, "stream-1", raw=True)

	# //// Neoffice — added test: a 1 MB chunk closed the websocket on a PDF over 768 KB.
	def test_a_chunk_fits_the_websocket_message_limit_once_encoded(self):
		"""The connection is the websockets library's default, 1 MiB a message: an encoded chunk over it closes the
		connection (1009) and the print never answers (a dunning of 5.8 MB did)."""
		from frappe.utils.pdf_generator.page import PDF_STREAM_CHUNK

		websocket_limit = 2**20
		encoded = (PDF_STREAM_CHUNK + 2) // 3 * 4
		self.assertLess(encoded + 4096, websocket_limit)  # room for the JSON around it
		self.assertGreaterEqual(PDF_STREAM_CHUNK, 256 * 1024)  # and still far fewer round trips than 4 KB

	def test_a_document_of_300_kb_is_read_in_one_round_trip(self):
		payload = bytes(range(256)) * 1200  # 300 KB
		page, calls = self._page_that_answers(payload)
		self.assertEqual(self._read(page), payload)
		reads = [params for method, params in calls if method == "IO.read"]
		self.assertEqual(len(reads), 1)
		# //// Neoffice — the size asked is the module's constant, not a literal.
		from frappe.utils.pdf_generator.page import PDF_STREAM_CHUNK

		self.assertEqual(reads[0]["size"], PDF_STREAM_CHUNK)
		self.assertEqual([m for m, _ in calls].count("IO.close"), 1)

	# //// Neoffice — added test: a PDF of several chunks.
	def test_a_large_document_is_read_chunk_by_chunk(self):
		from frappe.utils.pdf_generator.page import PDF_STREAM_CHUNK

		payload = bytes(range(256)) * 4800  # 1.2 MB
		page, calls = self._page_that_answers(payload)
		self.assertEqual(self._read(page), payload)
		offsets = [params["offset"] for method, params in calls if method == "IO.read"]
		self.assertEqual(offsets, [0, PDF_STREAM_CHUNK, 2 * PDF_STREAM_CHUNK])

	def test_a_shorter_answer_moves_the_offset_by_what_came_back(self):
		payload = bytes(range(256)) * 1400  # 350 KB, answered 100 KB at a time
		page, calls = self._page_that_answers(payload, at_most=100 * 1024)
		self.assertEqual(self._read(page), payload)
		offsets = [params["offset"] for method, params in calls if method == "IO.read"]
		self.assertEqual(offsets, [0, 102400, 204800, 307200])

	def test_an_error_from_chromium_is_raised(self):
		from unittest import mock

		page = mock.Mock(send=lambda method, params=None: (None, "boom"))
		with self.assertRaises(RuntimeError):
			self._read(page)


# //// Neoffice — added test for the loopback DevTools socket, which no longer offers permessage-deflate (cdp_connection.py).
class TestChromePdfSocketCompression(FrappeTestCase):
	"""Chromium on this machine is talked to without compression, one on another machine as the library does."""

	def test_a_chromium_on_this_machine_gets_no_compression(self):
		from frappe.utils.pdf_generator.cdp_connection import _compression_for

		for url in (
			"ws://127.0.0.1:9222/devtools/browser/abc",
			"ws://localhost:9222/devtools/browser/abc",
			"ws://[::1]:9222/devtools/browser/abc",
		):
			self.assertIsNone(_compression_for(url), url)

	def test_a_chromium_on_another_machine_keeps_the_library_default(self):
		from frappe.utils.pdf_generator.cdp_connection import _compression_for

		for url in (
			"ws://chromium.internal:9222/devtools/browser/abc",
			"wss://10.0.0.12:9222/devtools/browser/abc",
			"ws://127.0.0.1.example.com:9222/devtools/browser/abc",
		):
			self.assertEqual(_compression_for(url), "deflate", url)

	def test_the_client_asks_websockets_for_no_extension_on_loopback(self):
		"""Through the real library: a server that offers permessage-deflate (the library's default, as Chromium
		does) ends up with NO extension on the connection of the client."""
		import websockets

		from frappe.utils.pdf_generator.cdp_connection import CDPSocketClient

		client = CDPSocketClient(
			"ws://127.0.0.1:1/devtools/browser/x"
		)  # replaced below, once the port is known

		async def negotiated():
			async def handler(connection):
				await connection.wait_closed()

			async with websockets.serve(handler, "127.0.0.1", 0) as server:
				port = server.sockets[0].getsockname()[1]
				client.websocket_url = f"ws://127.0.0.1:{port}/devtools/browser/x"
				await client._connect()
				try:
					return [type(extension).__name__ for extension in client.connection.protocol.extensions]
				finally:
					await client.connection.close()

		try:
			self.assertEqual(client.loop.run_until_complete(negotiated()), [])
		finally:
			client.loop.close()
			asyncio.set_event_loop(None)


# //// Neoffice — added test: the shared Chromium is always up, the generator only waits for a restart and never starts it.
class TestChromePdfSharedChromiumIsAlwaysUp(FrappeTestCase):
	def _generator(self):
		from frappe.utils.pdf_generator.chrome_pdf_generator import ChromePDFGenerator

		generator = object.__new__(ChromePDFGenerator)  # no __init__: it would read the site config
		generator._remote_port = 9222
		return generator

	def test_a_live_chromium_answers_at_once_without_sleeping(self):
		from unittest import mock

		generator = self._generator()
		with (
			mock.patch.object(generator, "fetch_devtools_url", return_value="ws://127.0.0.1:9222/x") as fetch,
			mock.patch("frappe.utils.pdf_generator.chrome_pdf_generator.time.sleep") as sleep,
		):
			self.assertEqual(generator._wait_for_shared_chromium(), "ws://127.0.0.1:9222/x")
		fetch.assert_called_once_with(9222)
		sleep.assert_not_called()

	def test_a_restart_in_progress_is_waited_for(self):
		from unittest import mock

		generator = self._generator()
		answers = [None, None, "ws://127.0.0.1:9222/after-restart"]
		with (
			mock.patch.object(generator, "fetch_devtools_url", side_effect=answers),
			mock.patch("frappe.utils.pdf_generator.chrome_pdf_generator.time.sleep"),
		):
			self.assertEqual(generator._wait_for_shared_chromium(), "ws://127.0.0.1:9222/after-restart")

	def test_a_chromium_that_is_down_is_never_started_from_here(self):
		"""The service used to be started through sudo on the first print after an idle stop. Nothing starts it
		now: systemd keeps it up, and a dead endpoint after 15 s is reported once."""
		from unittest import mock

		generator = self._generator()
		clock = iter(range(0, 1000, 5))  # every call to time.time() is 5 s later: the 15 s window ends fast
		with (
			mock.patch.object(generator, "fetch_devtools_url", return_value=None),
			mock.patch("frappe.utils.pdf_generator.chrome_pdf_generator.time.sleep"),
			mock.patch(
				"frappe.utils.pdf_generator.chrome_pdf_generator.time.time", side_effect=lambda: next(clock)
			),
			mock.patch("frappe.utils.pdf_generator.chrome_pdf_generator.subprocess.run") as run,
			mock.patch("frappe.utils.pdf_generator.chrome_pdf_generator.subprocess.Popen") as popen,
			mock.patch("frappe.log_error") as log_error,
		):
			self.assertIsNone(generator._wait_for_shared_chromium())
		run.assert_not_called()
		popen.assert_not_called()
		log_error.assert_called_once()

	def test_no_idle_stop_leftover_remains_in_the_generator(self):
		from frappe.utils.pdf_generator.chrome_pdf_generator import ChromePDFGenerator

		for gone in ("_touch_activity", "ACTIVITY_FILE", "_ensure_remote_service"):
			self.assertFalse(hasattr(ChromePDFGenerator, gone), gone)
