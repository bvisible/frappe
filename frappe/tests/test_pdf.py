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


# //// Neoffice — added test for the backport of upstream 7277896261 (the Chrome PDF stream read in 1 MB chunks).
class TestChromePdfStreamChunks(FrappeTestCase):
	"""The PDF Chromium draws is read from its stream in large chunks, whatever size it answers with."""

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

	def test_a_document_of_300_kb_is_read_in_one_round_trip(self):
		payload = bytes(range(256)) * 1200  # 300 KB
		page, calls = self._page_that_answers(payload)
		self.assertEqual(self._read(page), payload)
		reads = [params for method, params in calls if method == "IO.read"]
		self.assertEqual(len(reads), 1)
		self.assertEqual(reads[0]["size"], 1024 * 1024)
		self.assertEqual([m for m, _ in calls].count("IO.close"), 1)

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
