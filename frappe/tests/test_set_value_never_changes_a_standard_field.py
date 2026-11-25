# //// Neoffice — added file (no upstream equivalent). GHSA-2c6h-wv85-fxvj (published without a fixed version):
# //// frappe.client.set_value refused a standard field named as `fieldname` but took the same fields given as a dict,
# //// so `name` redirected the save onto another record. Remove with _refuse_changed_standard_fields in client.py.
# //// TestSetValueSubmitsAndCancels: the docstatus a document may still change there (neoffice-maintenance#3348).
import json
from datetime import date, datetime

import frappe
from frappe.client import set_value
from frappe.core.doctype.doctype.test_doctype import new_doctype
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


SUBMITTER = "set-value-submitter-test@example.com"
WRITER = "set-value-writer-test@example.com"
SUBMITS = "Set Value Submits Test"
WRITES = "Set Value Writes Test"


class TestSetValueSubmitsAndCancels(FrappeTestCase):
	"""frappe-ui submits and cancels by set_value({"docstatus": …}): save() checks the move and its right."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		for role in (SUBMITS, WRITES):
			if not frappe.db.exists("Role", role):
				frappe.get_doc({"doctype": "Role", "role_name": role, "desk_access": 1}).insert()
		rights = {"read": 1, "write": 1, "create": 1, "permlevel": 0}
		fields = [{"fieldname": "title", "fieldtype": "Data", "label": "Title"}]
		cls.child = new_doctype(fields=fields, istable=1, permissions=[]).insert().name
		cls.doctype = (
			new_doctype(
				fields=[
					*fields,
					{"fieldname": "rows", "fieldtype": "Table", "label": "Rows", "options": cls.child},
				],
				is_submittable=1,
				permissions=[
					{"role": SUBMITS, **rights, "submit": 1, "cancel": 1},
					{"role": WRITES, **rights},
				],
			)
			.insert()
			.name
		)
		for email, role in ((SUBMITTER, SUBMITS), (WRITER, WRITES)):
			if not frappe.db.exists("User", email):
				frappe.get_doc(
					{
						"doctype": "User",
						"email": email,
						"first_name": "Set value test",
						"send_welcome_email": 0,
						"roles": [{"role": role}],
					}
				).insert()
		frappe.db.commit()  # nosemgrep: the doctype's table is made by DDL, which commits anyway

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		frappe.db.rollback()
		for email in (SUBMITTER, WRITER):
			frappe.delete_doc_if_exists("User", email, force=True)
		frappe.delete_doc("DocType", cls.doctype, force=True)
		frappe.delete_doc("DocType", cls.child, force=True)
		for role in (SUBMITS, WRITES):
			frappe.delete_doc_if_exists("Role", role, force=True)
		frappe.db.commit()  # nosemgrep: the decor of the class goes, as it came
		super().tearDownClass()

	def draft(self, user):
		with as_role(user):
			return frappe.get_doc(
				{"doctype": self.doctype, "title": "draft", "rows": [{"title": "row"}]}
			).insert()

	def docstatus(self, name):
		return frappe.db.get_value(self.doctype, name, "docstatus")

	def test_a_draft_is_submitted_by_whom_may_submit(self):
		doc = self.draft(SUBMITTER)
		with as_role(SUBMITTER):
			set_value(self.doctype, doc.name, {"docstatus": 1})
		self.assertEqual(self.docstatus(doc.name), 1)

	def test_a_submitted_document_is_cancelled_by_whom_may_cancel(self):
		doc = self.draft(SUBMITTER)
		with as_role(SUBMITTER):
			doc.submit()
			set_value(self.doctype, doc.name, json.dumps({"docstatus": 2}))
		self.assertEqual(self.docstatus(doc.name), 2)

	def test_who_may_not_submit_is_refused(self):
		doc = self.draft(WRITER)
		with as_role(WRITER):
			with self.assertRaises(frappe.PermissionError):
				set_value(self.doctype, doc.name, {"docstatus": 1})
		self.assertEqual(self.docstatus(doc.name), 0)

	def test_a_draft_is_never_cancelled_nor_a_submitted_document_put_back(self):
		doc = self.draft(SUBMITTER)
		with as_role(SUBMITTER):
			with self.assertRaises(frappe.ValidationError):
				set_value(self.doctype, doc.name, {"docstatus": 2})
			self.assertEqual(self.docstatus(doc.name), 0)
			doc.reload()
			doc.submit()
			with self.assertRaises(frappe.ValidationError):
				set_value(self.doctype, doc.name, {"docstatus": 0})
		self.assertEqual(self.docstatus(doc.name), 1)

	def test_the_docstatus_of_a_row_is_still_refused(self):
		doc = self.draft(SUBMITTER)
		with as_role(SUBMITTER):
			with self.assertRaises(frappe.ValidationError):
				set_value(self.child, doc.rows[0].name, {"docstatus": 1})
		self.assertEqual(frappe.db.get_value(self.child, doc.rows[0].name, "docstatus"), 0)
