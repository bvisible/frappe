# //// Neoffice — added file (no upstream equivalent). GHSA-2c6h-wv85-fxvj (published without a fixed version):
# //// frappe.client.set_value refused a standard field named as `fieldname` but took the same fields given as a dict,
# //// so `name` redirected the save onto another record. Remove with _refuse_changed_standard_fields in client.py.
import json
from datetime import date, datetime

import frappe
from frappe.client import set_value
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase

ACTOR = "set-value-actor-test@example.com"
OTHER = "set-value-other-test@example.com"


class TestSetValueNeverChangesAStandardField(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		for email in (ACTOR, OTHER):
			if not frappe.db.exists("User", email):
				frappe.get_doc(
					{
						"doctype": "User",
						"email": email,
						"first_name": "Set value test",
						"send_welcome_email": 0,
					}
				).insert(ignore_permissions=True)
		with as_role(ACTOR):
			self.mine = frappe.get_doc({"doctype": "ToDo", "description": "mine"}).insert()
		# assigned to the actor too: write access that does not hang on being its owner
		frappe.db.set_value("ToDo", self.mine.name, "allocated_to", ACTOR, update_modified=False)
		self.theirs = frappe.get_doc(
			{"doctype": "ToDo", "description": "theirs", "allocated_to": OTHER, "assigned_by": OTHER}
		).insert(ignore_permissions=True)
		frappe.db.set_value("ToDo", self.theirs.name, "owner", OTHER, update_modified=False)

	def tearDown(self):
		frappe.set_user("Administrator")
		for todo in (self.mine, self.theirs):
			frappe.delete_doc("ToDo", todo.name, force=True, ignore_permissions=True)
		for email in (ACTOR, OTHER):
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)

	def test_a_name_given_in_a_dict_does_not_write_over_another_record(self):
		# what a reader of the other record sends: its name, its timestamp, and the values a save never lets change
		# (owner, creation and any set-only-once field: the save compares them with the record it overwrites)
		constants = [df.fieldname for df in frappe.get_meta("ToDo").get_set_only_once_fields()]
		read = frappe.db.get_value("ToDo", self.theirs.name, ["modified", *constants], as_dict=True)
		forged = {k: str(v) if isinstance(v, date | datetime) else v for k, v in read.items()}
		forged.update({"name": self.theirs.name, "description": "forged"})
		with as_role(ACTOR):
			with self.assertRaises(frappe.ValidationError):
				set_value("ToDo", self.mine.name, json.dumps(forged))
		self.assertEqual(frappe.db.get_value("ToDo", self.theirs.name, "description"), "theirs")

	def test_a_standard_field_echoed_unchanged_still_saves(self):
		with as_role(ACTOR):
			set_value(
				"ToDo", self.mine.name, {"name": self.mine.name, "docstatus": 0, "description": "edited"}
			)
		self.assertEqual(frappe.db.get_value("ToDo", self.mine.name, "description"), "edited")
