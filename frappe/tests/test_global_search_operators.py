# Copyright (c) 2026, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

# //// Neoffice — added file (no upstream equivalent): the global search must not fail on text that holds a
# //// boolean full-text operator with no word around it (maintenance#1892); see the marker in
# //// frappe/utils/global_search.py, search().

from frappe.tests.utils import FrappeTestCase
from frappe.utils import global_search


class TestGlobalSearchOperators(FrappeTestCase):
	def test_bare_boolean_operators_do_not_break_the_search(self):
		for text in (
			"(test)",
			"<img src=x>",
			"' or 1=1 --",
			"%%%%",
			'"half',
			"+-",
			"~*@",
			"()",
			"a+b",
			"x-(y",
			"SINV-2026-0001",
		):
			with self.subTest(text=text):
				self.assertIsInstance(global_search.search(text), list)

	def test_operators_inside_a_word_are_kept(self):
		self.assertEqual(global_search.search_words("SINV-2026-0001"), "SINV-2026-0001")
		self.assertEqual(global_search.search_words("info@example.com"), "info@example.com")
		self.assertEqual(global_search.search_words("(test)"), "test")
		self.assertEqual(global_search.search_words("<img src=x>"), "img src=x")
		self.assertEqual(global_search.search_words("%%%%"), "")
