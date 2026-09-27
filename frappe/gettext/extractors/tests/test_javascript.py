from frappe.gettext.extractors.javascript import extract_javascript
from frappe.tests.utils import FrappeTestCase


class TestJavaScript(FrappeTestCase):
	def test_extract_javascript(self):
		code = "let test = `<p>${__('Test')}</p>`;"
		self.assertEqual(
			next(extract_javascript(code)),
			(1, "__", "Test"),
		)

		code = "let test = `<p>${__('Test', null, 'Context')}</p>`;"
		self.assertEqual(
			next(extract_javascript(code)),
			(1, "__", ("Test", None, "Context")),
		)

	# //// Neoffice — added test (maintenance#866): a call in an HTML attribute of a template
	# //// string was never extracted, so its translation was dropped every night.
	def test_extract_javascript_in_an_attribute(self):
		code = 'let test = `<input class="search" placeholder="${__("Search a role")}" title=\'${__("Level")}\'>`;'
		self.assertEqual([message for _lineno, _func, message, _comments in extract_javascript(code)], ["Search a role", "Level"])
