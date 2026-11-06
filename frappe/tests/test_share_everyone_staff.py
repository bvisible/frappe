# //// Neoffice — added file (no upstream equivalent): a share « with everyone » reaches the staff, never a portal
# //// account (frappe/share.py, everyone_reaches; maintenance#515).
import frappe
import frappe.share
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase

PORTAL = "np-test-share-portal@example.com"
STAFF = "np-test-share-staff@example.com"


def _user(email, user_type, roles=()):
	if not frappe.db.exists("User", email):
		frappe.get_doc(
			{
				"doctype": "User",
				"email": email,
				"first_name": "Share Test",
				"user_type": user_type,
				"send_welcome_email": 0,
			}
		).insert(ignore_permissions=True)
	if roles:
		frappe.get_doc("User", email).add_roles(*roles)


class TestShareEveryoneStaff(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		_user(PORTAL, "Website User")
		_user(STAFF, "System User", roles=("Blogger",))
		cls.todo = frappe.get_doc({"doctype": "ToDo", "description": "np-test shared with everyone"}).insert(
			ignore_permissions=True
		)
		frappe.share.add_docshare(
			"ToDo", cls.todo.name, everyone=1, read=1, flags={"ignore_share_permission": True}
		)

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		frappe.db.delete("DocShare", {"share_doctype": "ToDo", "share_name": cls.todo.name})
		frappe.delete_doc("ToDo", cls.todo.name, force=True, ignore_permissions=True)
		for email in (PORTAL, STAFF):
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def test_the_staff_reach_a_share_with_everyone(self):
		with as_role(STAFF):
			self.assertIn(self.todo.name, frappe.share.get_shared("ToDo"))
			self.assertIn("ToDo", frappe.share.get_shared_doctypes())
			self.assertTrue(frappe.has_permission("ToDo", "read", self.todo.name))

	def test_a_portal_account_does_not(self):
		with as_role(PORTAL):
			self.assertNotIn(self.todo.name, frappe.share.get_shared("ToDo"))
			self.assertNotIn("ToDo", frappe.share.get_shared_doctypes())
			self.assertFalse(frappe.has_permission("ToDo", "read", self.todo.name))

	def test_nor_does_a_visitor(self):
		self.assertFalse(frappe.share.everyone_reaches("Guest"))
		self.assertNotIn(self.todo.name, frappe.share.get_shared("ToDo", user="Guest"))
