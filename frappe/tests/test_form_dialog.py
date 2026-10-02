# //// Neoffice — added file (no upstream equivalent): the access check of the form dialog
# //// (frappe/desk/form_dialog.py), which decides whether a link opens its document's form in a
# //// dialog. Only frappe's own doctypes are used, so that it runs on a bare frappe site too.
import frappe
from frappe.desk.form_dialog import get_access, get_boot_settings
from frappe.tests.utils import FrappeTestCase

DESK_USER = "form-dialog-desk-test@example.com"
PORTAL_USER = "form-dialog-portal-test@example.com"


def make_user(email, user_type):
	if not frappe.db.exists("User", email):
		frappe.get_doc(
			{
				"doctype": "User",
				"email": email,
				"first_name": "Form dialog test",
				"user_type": user_type,
				"send_welcome_email": 0,
			}
		).insert(ignore_permissions=True)


def make_todo(allocated_to):
	return frappe.get_doc(
		{
			"doctype": "ToDo",
			"description": "Form dialog test",
			"allocated_to": allocated_to,
		}
	).insert(ignore_permissions=True)


class TestFormDialog(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		make_user(DESK_USER, "System User")
		make_user(PORTAL_USER, "Website User")
		cls.admin_todo = make_todo("Administrator")
		cls.own_todo = make_todo(DESK_USER)
		cls.error_log = frappe.get_doc(
			{"doctype": "Error Log", "method": "Form dialog test", "error": "test"}
		).insert(ignore_permissions=True)

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		for doctype, name in (
			("ToDo", cls.admin_todo.name),
			("ToDo", cls.own_todo.name),
			("Error Log", cls.error_log.name),
			("User", DESK_USER),
			("User", PORTAL_USER),
		):
			frappe.delete_doc(doctype, name, force=True, ignore_permissions=True)
		super().tearDownClass()

	def tearDown(self):
		frappe.set_user("Administrator")

	def test_a_reader_may_open_the_document(self):
		self.assertEqual(get_access("ToDo", self.admin_todo.name)["status"], "ok")
		frappe.set_user(DESK_USER)
		self.assertEqual(get_access("ToDo", self.own_todo.name)["status"], "ok")

	def test_a_missing_document_or_doctype_is_not_found(self):
		self.assertEqual(get_access("ToDo", "form-dialog-no-such-todo")["status"], "not_found")
		self.assertEqual(get_access("No Such Doctype", "x")["status"], "not_found")
		self.assertEqual(get_access("", "x")["status"], "not_found")

	def test_setup_records_singles_and_child_tables_are_excluded(self):
		self.assertEqual(get_access("DocType", "ToDo")["status"], "excluded")
		self.assertEqual(get_access("System Settings", "System Settings")["status"], "excluded")
		self.assertEqual(get_access("Has Role", "x")["status"], "excluded")

	def test_a_document_the_reader_may_not_read_is_forbidden(self):
		frappe.set_user(DESK_USER)
		# ToDo is readable, but only the reader's own (has_permission hook of ToDo).
		self.assertEqual(get_access("ToDo", self.admin_todo.name)["status"], "forbidden")

	def test_a_doctype_the_reader_may_not_read_says_nothing_of_its_records(self):
		frappe.set_user(DESK_USER)
		self.assertEqual(get_access("Error Log", self.error_log.name)["status"], "forbidden")
		# Not « not_found »: a reader without the doctype learns nothing about its names.
		self.assertEqual(get_access("Error Log", "form-dialog-no-such-log")["status"], "forbidden")

	def test_a_portal_user_is_refused(self):
		frappe.set_user(PORTAL_USER)
		self.assertEqual(get_access("ToDo", self.admin_todo.name)["status"], "forbidden")
		self.assertEqual(get_access("Error Log", self.error_log.name)["status"], "forbidden")

	def test_a_guest_cannot_call_it(self):
		frappe.set_user("Guest")
		with self.assertRaises(frappe.PermissionError):
			frappe.is_whitelisted(get_access)

	def test_boot_settings_follow_the_site_switch_and_the_hook(self):
		settings = get_boot_settings()
		self.assertEqual(settings["enabled"], 1)
		self.assertIn("DocType", settings["exclude"])
		frappe.local.conf.link_dialog = 0
		try:
			self.assertEqual(get_boot_settings()["enabled"], 0)
		finally:
			del frappe.local.conf["link_dialog"]
