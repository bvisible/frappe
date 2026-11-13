# //// Neoffice — added file (no upstream equivalent). GHSA-p4w2-9mr5-6prj (get_events) and GHSA-mf6r-q34j-pjw7
# //// (reportview get_count) are fixed upstream in v16 only: v16 builds those filters with its query engine, whose values
# //// did not escape a backslash, so a value ending in "\" stepped out of its quotes. v15 builds them with DatabaseQuery,
# //// which escapes values through the database driver. This keeps that true across merges: when a v16 merge brings
# //// the engine here, these tests say whether its fix (f463d84888) came with it.
import json

import frappe
from frappe.desk.doctype.event.event import get_events
from frappe.desk.reportview import get_count, get_filters_cond
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase
from frappe.utils import today

ACTOR = "backslash-filter-test@example.com"
VALUE = "zz backslash filter test\\"


class TestAFilterValueEndingInABackslashStaysAValue(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		if not frappe.db.exists("User", ACTOR):
			frappe.get_doc(
				{"doctype": "User", "email": ACTOR, "first_name": "Backslash test", "send_welcome_email": 0}
			).insert(ignore_permissions=True)
		self.event = frappe.get_doc(
			{"doctype": "Event", "subject": VALUE, "event_type": "Public", "starts_on": today() + " 10:00:00"}
		).insert(ignore_permissions=True)
		with as_role(ACTOR):
			self.todo = frappe.get_doc({"doctype": "ToDo", "description": VALUE}).insert()

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.delete_doc("Event", self.event.name, force=True, ignore_permissions=True)
		frappe.delete_doc("ToDo", self.todo.name, force=True, ignore_permissions=True)
		frappe.delete_doc("User", ACTOR, force=True, ignore_permissions=True)

	def test_the_filter_condition_matches_the_value_and_nothing_else(self):
		with as_role(ACTOR):
			cond = get_filters_cond("ToDo", {"description": VALUE}, [])
		self.assertEqual(frappe.db.sql(f"select count(*) from `tabToDo` where 1=1 {cond}")[0][0], 1)

	def test_get_events_finds_the_event_by_that_value(self):
		with as_role(ACTOR):
			events = get_events(today(), today(), filters=json.dumps({"subject": VALUE}))
		self.assertEqual([e.name for e in events], [self.event.name])

	def test_get_count_counts_the_record_with_that_value(self):
		with as_role(ACTOR):
			# set after as_role: frappe.set_user empties form_dict
			frappe.local.form_dict = frappe._dict(
				doctype="ToDo", filters=json.dumps([["ToDo", "description", "=", VALUE]])
			)
			self.assertEqual(get_count(), 1)
