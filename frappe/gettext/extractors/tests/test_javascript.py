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
		self.assertEqual([message for _lineno, _func, message in extract_javascript(code)], ["Search a role", "Level"])

	# //// Neoffice — added test (maintenance#866): babel's lexer cuts a template at the first
	# //// backtick of a nested one; the plain-text pass still finds the calls after it.
	def test_extract_a_call_after_a_nested_template(self):
		import io

		from frappe.gettext.extractors.javascript import extract

		code = (
			'let a = `<b>${x ? `<i>${__("Inside")}</i>` : ""}</b>'
			'<input placeholder="${__("After the nested one")}">`;\n'
			'let b = __(`Dynamic ${x}`);\n'
			"let c = __('It\\'s escaped');\n"
		)
		found = [message for _lineno, _func, message, _comments in extract(io.BytesIO(code.encode()), None, (), {})]
		self.assertIn("After the nested one", found)
		self.assertIn("It's escaped", found)
		# Found once, never twice: the plain-text pass skips what the tokenizer yielded.
		self.assertEqual(len(found), len(set(found)))
