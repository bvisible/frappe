# //// Neoffice — added file (no upstream equivalent): tests of inline_private_images (neoffice-maintenance#1856).
# Copyright (c) 2026, Neoffice and contributors
# License: MIT. See LICENSE

"""`inline_private_images` looks each image source up once, and never looks up an image that is already inline.

A dunning of 107 invoices draws 208 images: the logo 108 times and 100 QR codes that are `data:` URIs of about 13 KB.
Every image was looked up one by one (a File query, then urlparse and mimetypes on the 13 KB URI): 0.43 s of the
0.8 s the function took, twice per render. The HTML that comes out must stay the one it was.
"""

from unittest import mock

from bs4 import BeautifulSoup
from frappe.tests.utils import FrappeTestCase

import frappe.utils.pdf as pdfgen

PRIVATE = "/private/files/stamp.png"
INLINE = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg=="


def _old_inline_private_images(html):
	"""The function as it was: every image is looked up, one by one."""
	soup = BeautifulSoup(html, "html.parser")
	for img in soup.find_all("img"):
		if b64 := pdfgen._get_base64_image(img["src"]):
			img["src"] = b64
	return str(soup)


def _lookup(src):
	return "data:image/png;base64,PRIVATE" if src == PRIVATE else None


HTML = (
	"<html><body><p>Facture</p>"
	f'<img src="/files/logo.png"><img src="/files/logo.png"><img src="{INLINE}"><img src="{INLINE}">'
	f'<img src="{PRIVATE}"><img src="{PRIVATE}"><img src="https://example.org/x.png"></body></html>'
)


class TestInlinePrivateImages(FrappeTestCase):
	def test_an_image_that_is_already_inline_is_never_looked_up(self):
		with mock.patch.object(pdfgen, "_get_base64_image", side_effect=_lookup) as lookup:
			pdfgen.inline_private_images(HTML)
		self.assertNotIn(mock.call(INLINE), lookup.call_args_list)

	def test_a_source_used_several_times_is_looked_up_once(self):
		with mock.patch.object(pdfgen, "_get_base64_image", side_effect=_lookup) as lookup:
			pdfgen.inline_private_images(HTML)
		sources = [call.args[0] for call in lookup.call_args_list]
		self.assertEqual(sorted(sources), sorted(set(sources)))
		self.assertEqual(set(sources), {"/files/logo.png", PRIVATE, "https://example.org/x.png"})

	def test_a_private_image_is_inlined_wherever_it_is_used(self):
		with mock.patch.object(pdfgen, "_get_base64_image", side_effect=_lookup):
			html = pdfgen.inline_private_images(HTML)
		soup = BeautifulSoup(html, "html.parser")
		self.assertEqual([i["src"] for i in soup.find_all("img")].count("data:image/png;base64,PRIVATE"), 2)
		self.assertNotIn(PRIVATE, html)

	def test_the_html_is_the_one_the_old_function_gave(self):
		with mock.patch.object(pdfgen, "_get_base64_image", side_effect=_lookup):
			self.assertEqual(pdfgen.inline_private_images(HTML), _old_inline_private_images(HTML))

	def test_the_real_lookup_leaves_an_inline_image_and_a_public_one_alone(self):
		html = f'<img src="{INLINE}"><img src="/files/not-a-private-file.png">'
		self.assertEqual(pdfgen.inline_private_images(html), _old_inline_private_images(html))

	def test_a_page_without_image_is_unchanged_by_the_round_trip_it_always_made(self):
		html = "<p>Rien &amp; rien</p>"
		self.assertEqual(pdfgen.inline_private_images(html), _old_inline_private_images(html))
