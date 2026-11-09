# //// Neoffice — added file (no upstream equivalent). Guards neoffice-maintenance#1386: the
# //// cockpit sheet is served as text/css with no charset, so it must not depend on how the page
# //// that embeds it guesses the encoding.
"""cockpit.css reads the same whatever encoding the embedding document assumes."""

import re
import unittest
from pathlib import Path

CSS = Path(__file__).resolve().parents[1] / "public" / "css" / "cockpit.css"
CONTENT_DECLARATION = re.compile(r"""\bcontent\s*:\s*(?:"[^"]*"|'[^']*')""")


class TestCockpitCssEncoding(unittest.TestCase):
	def setUp(self):
		self.raw = CSS.read_bytes()

	def test_the_sheet_declares_its_charset_in_its_first_bytes(self):
		"""`@charset` only counts as the very first bytes of the file, before any comment."""
		self.assertTrue(
			self.raw.startswith(b'@charset "UTF-8";'),
			'cockpit.css must start with @charset "UTF-8"; (no comment, no BOM before it)',
		)

	def test_generated_content_is_plain_ascii(self):
		"""A raw character in `content:` is read as Windows-1252 by a document that is not UTF-8
		(the assistance console replays a page in an iframe): the breadcrumb separator then shows as
		three odd characters. An ASCII escape (`\\203A`) reads the same under any decoding."""
		text = self.raw.decode("utf-8")
		declarations = CONTENT_DECLARATION.findall(text)
		self.assertTrue(declarations, "no content: declaration found, the pattern is stale")
		for declaration in declarations:
			self.assertTrue(
				declaration.isascii(),
				f"non-ASCII generated content in cockpit.css: {declaration!r}",
			)

	def test_the_breadcrumb_separator_is_the_escaped_chevron(self):
		text = self.raw.decode("utf-8")
		self.assertIn('content: "\\203A";', text)
		self.assertNotIn("›", "".join(CONTENT_DECLARATION.findall(text)))


if __name__ == "__main__":
	unittest.main()
