# Copyright (c) 2026, Frappe Technologies and contributors
# License: MIT. See LICENSE
# //// Neoffice — added file (no upstream equivalent): the scripts and styles a web block keeps (#2351).

"""A web block's scripts are taken out for deduplication only when they have a body.

`extract_script_and_style_tags` took every <script> out of a rendered web block and kept its `.string`. For a
<script src="…"> - or any tag with no single text node - that is None, and the page wrote <script>None</script> back:
« None is not defined » in the console of every page of a shop whose generated header carried one, and the external
script was never loaded (its src was lost).
"""

import unittest

from frappe.website.doctype.web_page.web_page import extract_script_and_style_tags


class TestAWebBlockKeepsItsScripts(unittest.TestCase):
	def test_an_inline_script_is_taken_out_with_its_body(self):
		html, scripts, _styles = extract_script_and_style_tags(
			"<div>menu</div><script>window.ready = 1;</script>"
		)
		self.assertEqual(scripts, ["window.ready = 1;"])
		self.assertNotIn("<script", html)

	def test_an_external_script_stays_in_place_with_its_src(self):
		html, scripts, _styles = extract_script_and_style_tags(
			'<div>menu</div><script src="/assets/shop/menu.js" defer></script>'
		)
		self.assertNotIn(None, scripts)
		self.assertEqual(scripts, [])
		self.assertIn('src="/assets/shop/menu.js"', html)

	def test_an_empty_script_or_style_writes_no_none(self):
		_html, scripts, styles = extract_script_and_style_tags(
			"<div>menu</div><script></script><style></style>"
		)
		self.assertNotIn(None, scripts)
		self.assertNotIn(None, styles)

	def test_an_inline_style_is_taken_out_with_its_body(self):
		html, _scripts, styles = extract_script_and_style_tags(
			"<div>menu</div><style>.menu{color:red}</style>"
		)
		self.assertEqual(styles, [".menu{color:red}"])
		self.assertNotIn("<style", html)
