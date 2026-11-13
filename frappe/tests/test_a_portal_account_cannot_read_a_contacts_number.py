# //// Neoffice — added file (no upstream equivalent). neoffice-maintenance#2646: GHSA-r8gf-89pc-pmv3 is fixed
# //// upstream in v16 only; on v15 any logged-in account, a portal customer included, read the number of any
# //// contact through the method of the SMS dialog. Remove with the backport in sms_settings.py.
import frappe
from frappe.core.doctype.sms_settings.sms_settings import get_contact_number
from frappe.tests.utils import FrappeTestCase

PORTAL = "contact-number-portal-test@example.com"


class TestAPortalAccountCannotReadAContactsNumber(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		self.todo = frappe.get_doc({"doctype": "ToDo", "description": "contact number test"}).insert(
			ignore_permissions=True
		)
		self.linked = self._contact("Linked", "+41790000001", link=True)
		self.other = self._contact("Other", "+41790000002", link=False)
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

	def _contact(self, name, mobile, link):
		contact = frappe.get_doc({"doctype": "Contact", "first_name": f"zz-2646 {name}"})
		contact.append("phone_nos", {"phone": mobile, "is_primary_mobile_no": 1})
		if link:
			contact.append("links", {"link_doctype": "ToDo", "link_name": self.todo.name})
		return contact.insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")
		for doc in (self.linked, self.other, self.todo):
			frappe.delete_doc(doc.doctype, doc.name, force=True, ignore_permissions=True)
		frappe.delete_doc("User", PORTAL, force=True, ignore_permissions=True)

	def test_a_portal_account_is_refused(self):
		self.assertEqual(frappe.db.get_value("User", PORTAL, "user_type"), "Website User")
		frappe.set_user(PORTAL)
		with self.assertRaises(frappe.PermissionError):
			get_contact_number(self.linked.name, "ToDo", self.todo.name)

	def test_a_contact_not_linked_to_the_document_gives_no_number(self):
		self.assertEqual(get_contact_number(self.other.name, "ToDo", self.todo.name), "")

	def test_a_contact_linked_to_the_document_still_gives_its_number(self):
		self.assertEqual(get_contact_number(self.linked.name, "ToDo", self.todo.name), "+41790000001")
