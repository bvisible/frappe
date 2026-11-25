# //// Neoffice — added file (no upstream equivalent).
"""A filter on a field the reader may not read is refused on the doctypes an app guards (neoffice-maintenance#3382).

v15 strips a field of a higher permission level from the columns, the order and the grouping of a list, not from its
filters: « target_weight > 68 », then « > 69 », read a member's hidden value by bisection. On the doctypes an app names
in the hook `permlevel_guarded_filters`, the doors a client sends filters through (`frappe.desk.reportview.validate_args`,
under the list and report views and frappe.client; the link search) now refuse such a filter. Every other doctype is
left as it is: the desk filters by such fields on v15 (a company's address, an employee's leaves).

Note stands in for a guarded doctype, its `public` field at level 1. The reader is a stand-in that reads level 0 only
(`get_permitted_fields` answers for it); nothing is written.
"""

from unittest.mock import patch

import frappe
from frappe.desk import reportview, search
from frappe.tests.utils import FrappeTestCase

GUARDED = "Note"
READER = "_test_guarded_filter_reader@example.com"
LEVEL_0 = ["name", "title", "content"]


class TestAFilterOnAGuardedFieldIsRefused(FrappeTestCase):
	def setUp(self):
		self.guarded = [GUARDED]
		real_hooks = frappe.get_hooks

		def hooks(hook=None, *args, **kwargs):
			if hook == "permlevel_guarded_filters":
				return list(self.guarded)
			return real_hooks(hook, *args, **kwargs)

		self.enterContext(patch("frappe.get_hooks", side_effect=hooks))
		self.enterContext(
			patch.object(reportview, "get_permitted_fields", side_effect=lambda *a, **k: LEVEL_0)
		)
		self.enterContext(patch.dict(frappe.local.session, {"user": READER}))

	def check(self, filters, doctype=GUARDED):
		reportview.check_filters_permlevel(frappe._dict(doctype=doctype), filters)

	def test_a_filter_on_a_field_of_the_second_level_is_refused_in_every_form(self):
		for filters in (
			{"public": 1},
			{"public": ["=", 1]},
			[["public", "=", 1]],
			[[GUARDED, "public", "=", 1]],
			[["`tabNote`.`public`", "=", 1]],
		):
			with self.subTest(filters=filters), self.assertRaises(frappe.PermissionError):
				self.check(filters)

	def test_a_field_of_the_first_level_and_a_standard_field_are_left_alone(self):
		self.check({"title": "x"})
		self.check([["name", "=", "x"], [GUARDED, "content", "like", "%x%"]])

	def test_a_doctype_no_app_guards_is_left_as_it_is(self):
		self.guarded = []
		self.check({"public": 1})

	def test_administrator_filters_freely(self):
		with patch.dict(frappe.local.session, {"user": "Administrator"}):
			self.check({"public": 1})

	def test_the_doors_of_a_client_ask_it(self):
		with self.assertRaises(frappe.PermissionError):
			reportview.validate_args(frappe._dict(doctype=GUARDED, fields=["name"], filters={"public": 1}))
		with self.assertRaises(frappe.PermissionError):
			reportview.validate_args(
				frappe._dict(doctype=GUARDED, fields=["name"], or_filters=[["public", "=", 1]])
			)
		with patch("frappe.get_list", side_effect=AssertionError("the search queried before refusing")):
			with self.assertRaises(frappe.PermissionError):
				search.search_widget(GUARDED, "", filters={"public": 1})
