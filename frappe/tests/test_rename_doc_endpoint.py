# //// Neoffice — added file (no upstream equivalent): frappe.rename_doc is a whitelisted method, and our
# //// fork had given it an ignore_permissions keyword (7d254db9dc) that some of our apps passed. It has
# //// upstream's signature again; internal callers that must skip permissions go through
# //// frappe.model.rename_doc.rename_doc, which is not whitelisted (neoffice-maintenance#1758).
# //// These tests keep the endpoint on upstream's signature and check who may rename through it.

import inspect
from unittest.mock import patch

import frappe
from frappe.model.rename_doc import rename_doc as model_rename_doc
from frappe.tests.utils import FrappeTestCase

ROLE = "_Test Rename Endpoint Role"
RENAMED = "_Test Rename Endpoint Role Renamed"
PORTAL_USER = "_test_rename_endpoint_portal@example.com"


class TestRenameDocEndpoint(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		with patch("frappe.sendmail"):
			if not frappe.db.exists("User", PORTAL_USER):
				frappe.get_doc(
					{
						"doctype": "User",
						"email": PORTAL_USER,
						"first_name": "Rename Endpoint",
						"user_type": "Website User",
						"send_welcome_email": 0,
					}
				).insert(ignore_permissions=True)

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		with patch("frappe.sendmail"):
			if frappe.db.exists("User", PORTAL_USER):
				frappe.delete_doc("User", PORTAL_USER, force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def setUp(self):
		frappe.set_user("Administrator")
		self.remove_test_roles()
		frappe.get_doc({"doctype": "Role", "role_name": ROLE, "desk_access": 0}).insert()

	def tearDown(self):
		frappe.set_user("Administrator")
		self.remove_test_roles()
		frappe.db.commit()

	@staticmethod
	def remove_test_roles():
		for name in (ROLE, RENAMED):
			if frappe.db.exists("Role", name):
				frappe.delete_doc("Role", name, force=True, ignore_permissions=True)

	def test_endpoint_has_upstreams_signature(self):
		self.assertNotIn("ignore_permissions", inspect.signature(frappe.rename_doc).parameters)

	def test_endpoint_refuses_the_permission_keyword(self):
		with self.assertRaises(TypeError):
			frappe.rename_doc("Role", ROLE, RENAMED, ignore_permissions=True)
		self.assertTrue(frappe.db.exists("Role", ROLE))

	def test_request_arguments_never_reach_the_endpoint_as_a_bypass(self):
		# frappe.call is how the HTTP handler runs a whitelisted method with the request's arguments
		for user in ("Guest", PORTAL_USER):
			with self.subTest(user=user):
				frappe.set_user(user)
				with self.assertRaises(frappe.ValidationError):
					frappe.call(
						frappe.rename_doc,
						doctype="Role",
						old=ROLE,
						new=RENAMED,
						ignore_permissions=1,
						show_alert=0,
						rebuild_search=0,
					)
				frappe.set_user("Administrator")
				self.assertTrue(frappe.db.exists("Role", ROLE))
				self.assertFalse(frappe.db.exists("Role", RENAMED))

	def test_a_user_with_write_permission_renames_through_the_endpoint(self):
		frappe.call(frappe.rename_doc, doctype="Role", old=ROLE, new=RENAMED, show_alert=0, rebuild_search=0)
		self.assertTrue(frappe.db.exists("Role", RENAMED))

	def test_internal_code_still_renames_without_the_users_permission(self):
		frappe.set_user(PORTAL_USER)
		model_rename_doc(
			"Role", ROLE, RENAMED, ignore_permissions=True, show_alert=False, rebuild_search=False
		)
		frappe.set_user("Administrator")
		self.assertTrue(frappe.db.exists("Role", RENAMED))
