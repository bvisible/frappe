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

	def test_pdf_encryption(self):
		password = "qwe"
		pdf = pdfgen.get_pdf(self.html, options={"password": password})
		reader = PdfReader(io.BytesIO(pdf))
		self.assertTrue(reader.is_encrypted)
		self.assertTrue(reader.decrypt(password))

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
