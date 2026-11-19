# //// Neoffice — added file (no upstream equivalent). Guards neoffice-maintenance#3092: get_print renders a PDF with the
# //// engine of the format it prints (the doctype's default when the call names none), and that engine belongs to the
# //// call: it no longer sticks to the next print of the same request or job.
"""Which PDF engine get_print uses.

A mailer attaches a document with frappe.attach_print and names no format: printview then renders the doctype's default
format. When that format is written for Chrome (the Oslo invoices: CSS variables, grid, flex), wkhtmltopdf ignores its
CSS, and the client receives an invoice with the logo at full size and cut, the address over the metadata and the Swiss
QR bill stacked. Mailers print as Administrator on purpose (cloud/order_billing.py sets it, attach_print skips the print
permission), so these tests do too: what is under test is the engine, not who may print.
"""

from unittest import mock

import frappe
from frappe.tests.utils import FrappeTestCase

CHROME_FORMAT = "_Test Print Engine Chrome"


class TestPrintEngine(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if not frappe.db.exists("Print Format", CHROME_FORMAT):
			frappe.get_doc(
				{
					"doctype": "Print Format",
					"name": CHROME_FORMAT,
					"doc_type": "ToDo",
					"module": "Desk",
					"standard": "No",
					"custom_format": 1,
					"print_format_type": "Jinja",
					"pdf_generator": "chrome",
					"html": "<p>{{ doc.description }}</p>",
				}
			).insert(ignore_permissions=True)
		cls.todo = frappe.get_doc({"doctype": "ToDo", "description": "Print engine"}).insert(
			ignore_permissions=True
		)

	def setUp(self):
		self.engines = []
		self.form_dict = frappe.local.form_dict
		frappe.local.form_dict = frappe._dict()

	def tearDown(self):
		frappe.local.form_dict = self.form_dict

	def print_pdf(self, **kwargs):
		"""get_print as a PDF, with both engines replaced by fakes that say which one ran."""

		def chrome(print_format, html, options, output, pdf_generator=None):
			if pdf_generator != "chrome":
				return None
			self.engines.append(("chrome", print_format))
			return b"%PDF chrome"

		def wkhtmltopdf(html, options=None, output=None):
			self.engines.append(("wkhtmltopdf", None))
			return b"%PDF wkhtmltopdf"

		with (
			mock.patch("frappe.utils.pdf.get_chrome_pdf", new=chrome),
			mock.patch("frappe.utils.pdf.get_pdf", new=wkhtmltopdf),
		):
			return frappe.get_print("ToDo", self.todo.name, as_pdf=True, **kwargs)

	def test_a_print_that_names_no_format_uses_the_engine_of_the_default_format(self):
		"""The mailer's call (attach_print with no format): Chrome, because the default format is a Chrome one."""
		with mock.patch.object(frappe.get_meta("ToDo"), "default_print_format", CHROME_FORMAT):
			self.print_pdf()
		self.assertEqual(self.engines, [("chrome", CHROME_FORMAT)])

	def test_a_named_format_keeps_its_engine(self):
		self.print_pdf(print_format=CHROME_FORMAT)
		self.print_pdf(print_format="Standard")
		self.assertEqual(self.engines, [("chrome", CHROME_FORMAT), ("wkhtmltopdf", None)])

	def test_the_engine_of_a_print_does_not_stick_to_the_next_one(self):
		"""A job that prints a Standard document first, then a Chrome invoice: each gets its own engine."""
		self.print_pdf(print_format="Standard")
		self.assertNotIn("pdf_generator", frappe.local.form_dict)
		self.print_pdf(print_format=CHROME_FORMAT)
		self.assertNotIn("pdf_generator", frappe.local.form_dict)
		self.assertEqual(self.engines, [("wkhtmltopdf", None), ("chrome", CHROME_FORMAT)])

	def test_the_engine_a_request_asks_for_still_comes_first(self):
		"""As upstream: an engine the request carries (download_pdf's parameter) wins, and stays the request's."""
		frappe.local.form_dict.pdf_generator = "wkhtmltopdf"
		self.print_pdf(print_format=CHROME_FORMAT)
		self.assertEqual(self.engines, [("wkhtmltopdf", None)])
		self.assertEqual(frappe.local.form_dict.pdf_generator, "wkhtmltopdf")
