# //// Neoffice — added file (no upstream equivalent): the name a download is sent under
# //// (frappe.utils.response.add_content_disposition). gunicorn 23 refused a name written on a Mac (« a » and U+0300
# //// for « à ») and the browser got a 502: a webmail's PDF could not be opened.
from urllib.parse import unquote

from gunicorn.http.wsgi import HEADER_VALUE_RE
from werkzeug.datastructures import Headers

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import response as responses

# « Scanner 20 nov. 2026 à 15.09.pdf » as a Mac writes it: the « à » in two characters
MAC_NAME = "Scanner 20 nov. 2026 à 15.09.pdf"


def escaped(name):
	"""A name as a caller hands it over to come back whole (the response decodes it with « unicode-escape »)."""
	return name.encode("unicode-escape").decode("ascii")


def written_before(kind, name):
	"""The header as it was written before, for a name the server always could send."""
	headers = Headers()
	headers.add("Content-Disposition", kind, filename=name)
	return headers["Content-Disposition"]


class TestADownloadNameTheServerCanSend(FrappeTestCase):
	def setUp(self):
		self.kept = dict(frappe.local.response)

	def tearDown(self):
		frappe.local.response.clear()
		frappe.local.response.update(self.kept)

	def disposition(self, way="as_raw", **response):
		frappe.local.response.update({"filecontent": b"%PDF-1.4", "result": "", **response})
		value = getattr(responses, way)().headers["Content-Disposition"]
		# What gunicorn checks before it sends a header: otherwise it closes the connection (a 502 in the browser)
		self.assertTrue(HEADER_VALUE_RE.fullmatch(value), repr(value))
		return value

	def test_a_name_written_on_a_mac_is_sent_with_its_accent_put_together(self):
		value = self.disposition(filename=escaped(MAC_NAME), display_content_as="inline")
		self.assertEqual(value, written_before("inline", "Scanner 20 nov. 2026 à 15.09.pdf"))

	def test_a_name_beyond_latin_1_goes_as_utf_8_beside_a_plain_one(self):
		name = "Facture 12 € Œuvre \U0001f600.pdf"
		value = self.disposition(filename=escaped(name))
		self.assertTrue(value.startswith("attachment; filename="), value)
		self.assertIn("Facture 12", value.split(";")[1])
		self.assertEqual(unquote(value.split("filename*=UTF-8''", 1)[1]), name)

	def test_a_plain_name_and_a_latin_1_name_are_written_as_before(self):
		for name in ("report.pdf", "Relevé de compte.pdf"):
			self.assertEqual(self.disposition(filename=escaped(name)), written_before("attachment", name))

	def test_every_download_way_writes_a_name_the_server_can_send(self):
		for way in ("as_raw", "as_pdf", "as_binary"):
			self.assertNotIn("̀", self.disposition(way, filename=escaped(MAC_NAME)))
		self.disposition("as_csv", doctype=escaped("Note €"))
		self.disposition("as_txt", doctype=escaped("Note €"))
