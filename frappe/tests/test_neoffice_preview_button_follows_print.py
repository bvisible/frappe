# //// Neoffice — added file (no upstream equivalent). The promoted « Preview / Print » button of the cockpit# //// head stayed on a cancelled invoice while the menu had no Print and the server refused the print# //// (maintenance#1472): it now follows the rule of the menu's Print entry.
"""The Preview / Print button of a form is offered where the menu's Print is, and only there.

The methods are read from form/toolbar.js and run in node, as the desk runs them: nothing of frappe is imported.
"""

import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

TOOLBAR_JS = Path(__file__).resolve().parents[1] / "public" / "js" / "frappe" / "form" / "toolbar.js"


def method_source(name: str) -> str:
	"""A method of the form toolbar, as the file writes it."""
	text = TOOLBAR_JS.read_text(encoding="utf-8")
	match = re.search(r"^\t" + name + r"\(\) \{\n.*?^\t\}\n", text, re.M | re.S)
	if not match:
		raise AssertionError(f"{name} is not in form/toolbar.js")
	return match.group(0)


def hidden(cases: list) -> list:
	"""Whether the button is hidden, for each (docstatus, submittable, print settings or None, is new)."""
	script = (
		"const cint = (v) => parseInt(v || 0, 10) || 0;\n"
		"const cases = JSON.parse(process.argv[1]);\n"
		"const out = cases.map(([docstatus, submittable, settings, isNew]) => {\n"
		"  let hide = null;\n"
		"  const button = { toggleClass: (c, v) => { hide = v; return button; } };\n"
		"  globalThis.document = { body: { classList: { contains: () => true } } };\n"
		"  globalThis.frappe = { model: { is_submittable: () => submittable, get_doc: () => settings, can_print: () => true } };\n"
		"  const toolbar = {\n"
		"    $preview_btn: button,\n"
		"    frm: { doc: { doctype: \"Sales Invoice\", docstatus, __islocal: isNew ? 1 : 0 }, meta: {} },\n"
		+ method_source("setup_preview_button")
		+ ",\n"
		+ method_source("can_print_this_document")
		+ "  };\n"
		"  toolbar.setup_preview_button();\n"
		"  return hide;\n"
		"});\n"
		"console.log(JSON.stringify(out));\n"
	)
	run = subprocess.run(
		[shutil.which("node"), "-e", script, json.dumps(cases)], capture_output=True, text=True, timeout=30
	)
	if run.returncode:
		raise AssertionError(run.stderr[-2000:])
	return json.loads(run.stdout)


class TestPreviewButtonFollowsPrint(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		if not shutil.which("node"):
			raise AssertionError("node is needed to run the desk's method (the CI installs it)")

	def test_a_cancelled_document_has_no_button_unless_the_settings_print_it(self):
		off = {"allow_print_for_cancelled": 0, "allow_print_for_draft": 1}
		on = {"allow_print_for_cancelled": 1, "allow_print_for_draft": 1}
		self.assertEqual(hidden([[2, True, off, False], [2, True, on, False]]), [True, False])

	def test_a_draft_follows_its_own_setting(self):
		self.assertEqual(
			hidden([[0, True, {"allow_print_for_draft": 1}, False], [0, True, {"allow_print_for_draft": 0}, False]]),
			[False, True],
		)

	def test_a_submitted_or_unsubmittable_document_keeps_it_and_a_new_one_never_has_it(self):
		off = {"allow_print_for_cancelled": 0, "allow_print_for_draft": 0}
		self.assertEqual(hidden([[1, True, off, False], [0, False, off, False], [0, False, off, True]]), [False, False, True])

	def test_settings_not_loaded_leave_the_button_to_the_server(self):
		self.assertEqual(hidden([[2, True, None, False]]), [False])


if __name__ == "__main__":
	unittest.main()
