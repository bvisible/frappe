# //// Neoffice — added file (no upstream equivalent). A request that names the documents it holds back from an account
# //// gives that account no right on them (neoffice-maintenance#3421); a request that names none reads the rights it read before.
"""The gate of `frappe.permissions.get_role_permissions`: NORA answers as a person, the roles of the person allow what the
workspaces of the account never gave, and she asks the fork to give no right on those documents for the rest of the request.

Nothing is written: only the roles of the person are read, so they are replaced (the account is nobody's)."""

from unittest.mock import patch

import frappe
import frappe.permissions
from frappe.tests.utils import FrappeTestCase

PERSON = "withheld-test-person@example.com"
OTHER = "withheld-test-other@example.com"
HELD = "Blog Post"  # the role below reads and writes it, at permlevel 0, whoever the owner is
FREE = "Blog Category"  # the same role reads and writes this one too
ROLES = ["Website Manager"]


def rights(doctype, user=PERSON):
	granted = frappe.permissions.get_role_permissions(doctype, user=user)
	return {ptype: granted.get(ptype) for ptype in frappe.permissions.rights}


class TestWhatTheRequestHoldsBack(FrappeTestCase):
	def setUp(self):
		super().setUp()
		self.addCleanup(self._forget_the_table)
		self.enterContext(patch.object(frappe, "get_roles", return_value=ROLES))
		self._forget_the_table()

	@staticmethod
	def _forget_the_table():
		frappe.local.flags.pop("what_was_given", None)
		frappe.local.flags.pop("what_was_given_refused", None)
		frappe.local.role_permissions = {}

	def _hold_back(self, table):
		frappe.local.role_permissions = {}
		frappe.local.flags.what_was_given = table

	def test_the_roles_allow_the_document_before_anything_is_held_back(self):
		granted = rights(HELD)
		self.assertEqual((granted["read"], granted["write"]), (1, 1))
		self.assertEqual(rights(FREE)["read"], 1)

	def test_a_request_that_holds_back_nothing_reads_the_rights_it_read_before(self):
		before = rights(HELD)
		for table in ({}, {PERSON: set()}, {PERSON: frozenset()}, {OTHER: {HELD}}, None):
			with self.subTest(table=table):
				self._hold_back(table)
				self.assertEqual(rights(HELD), before)

	def test_a_document_held_back_gives_the_account_no_right(self):
		self._hold_back({PERSON: {HELD}})
		self.assertTrue(rights(HELD))
		self.assertEqual(set(rights(HELD).values()), {0})
		self.assertFalse(frappe.has_permission(HELD, "read", user=PERSON))
		self.assertFalse(frappe.has_permission(HELD, "write", user=PERSON))

	def test_a_list_of_the_document_is_refused_and_a_list_of_another_is_not(self):
		self._hold_back({PERSON: {HELD}})
		with self.assertRaises(frappe.PermissionError):
			frappe.get_list(HELD, user=PERSON, limit=1)
		self.assertIsInstance(frappe.get_list(FREE, user=PERSON, limit=1), list)

	def test_the_other_documents_keep_their_rights(self):
		before = rights(FREE)
		self._hold_back({PERSON: {HELD}})
		self.assertEqual(rights(FREE), before)

	def test_the_rights_on_a_document_are_the_rights_on_its_doctype(self):
		doc = frappe.new_doc(HELD)
		self.assertEqual(frappe.permissions.get_doc_permissions(doc, user=PERSON)["read"], 1)
		self._hold_back({PERSON: {HELD}})
		self.assertEqual(frappe.permissions.get_doc_permissions(doc, user=PERSON)["read"], 0)
		self.assertFalse(frappe.has_permission(doc=doc, ptype="read", user=PERSON))

	def test_only_the_account_named_is_held_back(self):
		before = rights(HELD, OTHER)
		self._hold_back({PERSON: {HELD}})
		self.assertEqual(rights(HELD, OTHER), before)
		self.assertTrue(frappe.has_permission(HELD, "read", user=OTHER))

	def test_the_administrator_is_never_held_back(self):
		self._hold_back({"Administrator": {HELD}})
		self.assertEqual(set(rights(HELD, "Administrator").values()), {1})

	def test_the_gate_never_writes_in_the_cache_of_rights(self):
		before = rights(HELD)
		self._hold_back({PERSON: {HELD}})
		self.assertEqual(set(rights(HELD).values()), {0})
		frappe.local.flags.pop(
			"what_was_given"
		)  # the request that held it back is over: the cache stays as it was
		self.assertEqual(rights(HELD), before)
		self.assertEqual(rights(HELD), before)

	def test_the_gate_does_not_read_the_cache_either(self):
		self.assertEqual(rights(HELD)["read"], 1)  # the right is now in the cache
		frappe.local.flags.what_was_given = {PERSON: {HELD}}
		self.assertEqual(rights(HELD)["read"], 0)

	def test_the_first_document_refused_is_kept_on_the_request(self):
		self._hold_back({PERSON: {HELD, FREE}})
		self.assertIsNone(frappe.local.flags.get("what_was_given_refused"))
		rights(HELD)
		rights(FREE)
		self.assertEqual(frappe.local.flags.get("what_was_given_refused"), HELD)

	def test_nothing_is_kept_when_no_document_is_refused(self):
		self._hold_back({PERSON: {HELD}})
		rights(FREE)
		rights(HELD, OTHER)
		self.assertIsNone(frappe.local.flags.get("what_was_given_refused"))
		self._hold_back({})
		rights(HELD)
		self.assertIsNone(frappe.local.flags.get("what_was_given_refused"))
