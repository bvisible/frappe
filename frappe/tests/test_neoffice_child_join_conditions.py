# //// Neoffice — added file (no upstream equivalent): a child table joined into its parent's list follows its own
# //// permission_query_conditions hook, in the executed query only (neoffice-maintenance#3221).
"""A list of a parent with a column of a child table follows the child's own permission_query_conditions hook.

The fork puts the child's hook conditions in the ON clause of the child's join (DatabaseQuery.child_join_condition): a child
row that a list of the child itself leaves out is not read through its parent's list (a booking's health answers in the
report view of the bookings). Those conditions are trusted hook conditions, which may hold subqueries: they go in the executed
query only, never in the user part that validate_generated_query checks. In both at first, every list of a parent with a
column of such a child was refused as an « Illegal SQL Query » (osiris, 21.11).

The same holds for the child rows of a Data Export, which upstream reads by a raw query (a booking's health answers left through
it). Played on User and its child table Has Role (Administrator's own rows), with a hook given for the test.
"""

from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

HOOK = "frappe.tests.test_neoffice_child_join_conditions.only_the_kept_role"
KEPT = {"role": None}


def only_the_kept_role(user=None, doctype=None):
	"""A trusted condition with a subquery, as a hook may write one."""
	return f"`tabHas Role`.`role` in (select `name` from `tabRole` where `name` = {frappe.db.escape(KEPT['role'])})"


class TestAChildJoinedIntoItsParentsListFollowsItsOwnHook(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		held = frappe.get_all(
			"Has Role",
			filters={"parent": "Administrator", "parenttype": "User"},
			pluck="role",
			limit_page_length=0,
		)
		if len(held) < 2:
			self.skipTest("Administrator holds fewer than two roles of his own on this site")
		KEPT["role"] = sorted(held)[0]
		self.held = set(held)
		real = frappe.get_hooks

		def hooks(hook=None, default=None, app_name=None):
			got = real(hook, default, app_name)
			if hook == "permission_query_conditions":
				got = dict(got or {})
				got["Has Role"] = [HOOK]
			return got

		given = patch("frappe.get_hooks", side_effect=hooks)
		given.start()
		self.addCleanup(given.stop)

	def listed(self, lister):
		return {
			row.role
			for row in lister(
				"User",
				filters={"name": "Administrator"},
				fields=["name", "`tabHas Role`.`role` as role"],
				limit_page_length=0,
			)
		}

	def test_the_parents_list_reads_only_the_child_rows_its_hook_lets_through(self):
		roles = self.listed(frappe.get_list)
		self.assertEqual(roles, {KEPT["role"]})

	def test_nothing_is_narrowed_under_ignore_permissions(self):
		roles = self.listed(frappe.get_all)
		self.assertEqual(roles, self.held)

	def test_an_export_holds_only_the_child_rows_its_hook_lets_through(self):
		from frappe.core.doctype.data_export.exporter import export_data

		frappe.local.response = frappe._dict()
		export_data(
			doctype="User",
			select_columns=frappe.as_json({"User": ["name"], "Has Role": ["role"]}),
			file_type="CSV",
			template=False,
			with_data=True,
			filters=frappe.as_json({"name": "Administrator"}),
		)
		exported = frappe.local.response.result
		self.assertIn(f'"{KEPT["role"]}"', exported)
		for role in self.held - {KEPT["role"]}:
			self.assertNotIn(f'"{role}"', exported)
