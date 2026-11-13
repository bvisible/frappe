# //// Neoffice — added file (no upstream equivalent). GHSA-mcr4-jc52-ww6x is fixed upstream in v16 only: on v15
# //// save_page let any logged-in account, a portal customer included, overwrite another user's private workspace
# //// by sending `public` as a filter. Remove with the guard in workspace.py's save_page.
import json

import frappe
from frappe.desk.doctype.workspace.workspace import save_page
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase

OWNER = "workspace-owner-test@example.com"
PORTAL = "workspace-portal-test@example.com"
TITLE = "zz Private Workspace Test"
LABEL = f"{TITLE}-{OWNER}"
ORIGINAL = [{"id": "a1", "type": "header", "data": {"text": "mine", "col": 12}}]
FORGED = [{"id": "b2", "type": "header", "data": {"text": "forged", "col": 12}}]


class TestAPrivateWorkspaceIsSavedByItsOwnerOnly(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		self._user(OWNER, "System User")
		self._user(PORTAL, "Website User")
		if frappe.db.exists("Workspace", LABEL):
			frappe.delete_doc("Workspace", LABEL, force=True, ignore_permissions=True)
		frappe.get_doc(
			{
				"doctype": "Workspace",
				"title": TITLE,
				"label": LABEL,
				"for_user": OWNER,
				"public": 0,
				"content": json.dumps(ORIGINAL),
			}
		).insert(ignore_permissions=True)

	def _user(self, email, user_type):
		if not frappe.db.exists("User", email):
			frappe.get_doc(
				{
					"doctype": "User",
					"email": email,
					"first_name": "Workspace test",
					"user_type": user_type,
					"send_welcome_email": 0,
				}
			).insert(ignore_permissions=True)
		frappe.db.set_value("User", email, "user_type", user_type)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.delete_doc("Workspace", LABEL, force=True, ignore_permissions=True)
		for email in (OWNER, PORTAL):
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)

	def _content(self):
		return json.loads(frappe.db.get_value("Workspace", LABEL, "content"))

	def test_a_portal_account_cannot_overwrite_a_private_workspace_through_the_public_filter(self):
		with as_role(PORTAL):
			with self.assertRaises(frappe.PermissionError):
				save_page(LABEL, json.dumps(["!=", 1]), "{}", json.dumps(FORGED))
		self.assertEqual(self._content(), ORIGINAL)

	def test_its_owner_still_saves_it(self):
		with as_role(OWNER):
			save_page(TITLE, 0, "{}", json.dumps(FORGED))
		self.assertEqual(self._content(), FORGED)
