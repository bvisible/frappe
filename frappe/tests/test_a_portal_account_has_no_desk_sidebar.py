# //// Neoffice — added file (no upstream equivalent). neoffice-maintenance#2637: a portal account (Website User) never
# //// reaches the desk, and the desk's sidebar API answered it with the names of the site's workspaces.
import frappe
from frappe.desk.desktop import get_workspace_sidebar_items
from frappe.tests.utils import FrappeTestCase

PORTAL = "desk-sidebar-portal-test@example.com"


class TestAPortalAccountHasNoDeskSidebar(FrappeTestCase):
	def setUp(self):
		if not frappe.db.exists("User", PORTAL):
			frappe.get_doc(
				{
					"doctype": "User",
					"email": PORTAL,
					"first_name": "Portal",
					"user_type": "Website User",
					"send_welcome_email": 0,
				}
			).insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.delete_doc("User", PORTAL, force=True, ignore_permissions=True)

	def test_a_portal_account_gets_no_workspace(self):
		self.assertEqual(frappe.db.get_value("User", PORTAL, "user_type"), "Website User")
		frappe.set_user(PORTAL)
		sidebar = get_workspace_sidebar_items()
		self.assertEqual(sidebar["pages"], [])
		self.assertFalse(sidebar["has_access"])

	def test_a_desk_account_still_gets_its_workspaces(self):
		frappe.set_user("Administrator")
		self.assertTrue(get_workspace_sidebar_items()["pages"])
