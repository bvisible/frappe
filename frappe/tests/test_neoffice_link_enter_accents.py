# //// Neoffice — added file (no upstream equivalent). Guards Enter and Tab on a Link field when the word is
# //// typed without its accents: the search lists « Révision » for « revisio » (MariaDB compares without
# //// accents), and input_matches_item refused that row, so Enter did nothing (09.11).
"""Enter or Tab on a row the dropdown of a Link field lists picks it, whatever accents were typed.

The method is read from controls/link.js and run in node, as the desk runs it: nothing of frappe is imported.
"""

import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

LINK_JS = Path(__file__).resolve().parents[1] / "public" / "js" / "frappe" / "form" / "controls" / "link.js"


def method_source() -> str:
	"""The method `input_matches_item` of ControlLink, as the file writes it."""
	text = LINK_JS.read_text(encoding="utf-8")
	match = re.search(r"^\tinput_matches_item\(input, item\) \{\n.*?^\t\}\n", text, re.M | re.S)
	if not match:
		raise AssertionError("input_matches_item is no longer in controls/link.js")
	return match.group(0)


def matches(cases: list) -> list:
	"""What input_matches_item answers for each (input, item); `input` lowercased, as its caller passes it."""
	script = (
		"const cstr = (s) => (s === undefined || s === null ? '' : String(s));\n"
		"const control = {\n\tget_translated: (v) => v,\n" + method_source() + "};\n"
		"const cases = JSON.parse(process.argv[1]);\n"
		"console.log(JSON.stringify(cases.map(([input, item]) => !!control.input_matches_item(input, item))));\n"
	)
	out = subprocess.run(
		[shutil.which("node"), "-e", script, json.dumps(cases)],
		capture_output=True,
		text=True,
		check=True,
		timeout=30,
	)
	return json.loads(out.stdout)


class TestLinkEnterIgnoresAccents(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		if not shutil.which("node"):
			raise AssertionError("node is needed to run the desk's method (the CI installs it)")

	def test_a_word_typed_without_its_accents_picks_the_row_listed(self):
		self.assertEqual(
			matches(
				[
					["revisio", {"label": "Révision chaudière", "value": "TASK-2026-03215"}],
					["cafe", {"value": "Café"}],
					["controle", {"label": "Contrôle des freins"}],
				]
			),
			[True, True, True],
		)

	def test_a_word_typed_with_accents_picks_a_row_written_without(self):
		self.assertEqual(matches([["révision", {"label": "Revision machine"}]]), [True])

	def test_a_word_the_row_does_not_hold_is_still_refused(self):
		self.assertEqual(
			matches([["xyz", {"label": "Révision", "value": "TASK-1", "description": "Groupe"}]]), [False]
		)


if __name__ == "__main__":
	unittest.main()
