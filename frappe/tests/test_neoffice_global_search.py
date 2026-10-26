# //// Neoffice — added file (no upstream equivalent): tests of maintenance#1893, the name of a
# //// document indexed only for the doctypes of the Global Search Settings, and the prune of the index.
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils.global_search import prune_global_search

SETTINGS = "frappe.desk.doctype.global_search_settings.global_search_settings.get_doctypes_for_global_search"
PREFIX = "_gs1893-"


def search_rows(doctype):
	return frappe.db.sql_list(
		"select name from `__global_search` where doctype = %s and name like %s", (doctype, PREFIX + "%")
	)


def add_search_row(doctype, name):
	frappe.db.sql(
		"insert into `__global_search` (doctype, name, content, title, route) values (%s, %s, %s, %s, '')",
		(doctype, name, f"ID : {name}", name),
	)


class TestNameInGlobalSearch(FrappeTestCase):
	def test_name_indexed_only_for_the_doctypes_of_the_settings(self):
		with patch(SETTINGS, return_value=["ToDo"]):
			self.assertIn("name", [f.fieldname for f in frappe.get_meta("ToDo").get_global_search_fields()])
			self.assertNotIn(
				"name", [f.fieldname for f in frappe.get_meta("Error Log").get_global_search_fields()]
			)
			self.assertNotIn(
				"name", [f.fieldname for f in frappe.get_meta("Scheduled Job Log").get_global_search_fields()]
			)

	def test_a_child_or_single_doctype_never_gets_the_name(self):
		with patch(SETTINGS, return_value=["Has Role", "System Settings"]):
			self.assertFalse(frappe.get_meta("Has Role").name_in_global_search())
			self.assertFalse(frappe.get_meta("System Settings").name_in_global_search())

	def test_a_log_is_not_queued_for_the_index(self):
		with (
			patch(SETTINGS, return_value=["ToDo"]),
			patch("frappe.utils.global_search.sync_value_in_queue") as queued,
		):
			frappe.get_doc(doctype="Error Log", method=PREFIX + "log", error="x").insert(
				ignore_permissions=True
			)
			self.assertFalse([c for c in queued.call_args_list if c.args[0]["doctype"] == "Error Log"])

	def test_a_doctype_entering_or_leaving_the_settings_is_rebuilt(self):
		settings = frappe.get_single("Global Search Settings")
		listed = {d.document_type for d in settings.allowed_in_global_search}
		if "ToDo" in listed:
			settings.allowed_in_global_search = [
				d for d in settings.allowed_in_global_search if d.document_type != "ToDo"
			]
		else:
			settings.append("allowed_in_global_search", {"document_type": "ToDo"})
		with patch("frappe.enqueue") as enqueue:
			settings.save(ignore_permissions=True)
		rebuilt = [
			c.kwargs.get("doctype")
			for c in enqueue.call_args_list
			if c.args and c.args[0] == "frappe.utils.global_search.rebuild_for_doctype"
		]
		self.assertEqual(rebuilt, ["ToDo"])


class TestPruneGlobalSearch(FrappeTestCase):
	def tearDown(self):
		# prune_global_search() commits, so what the test wrote is removed and committed here
		frappe.db.sql("delete from `__global_search` where name like %s", PREFIX + "%")
		frappe.db.delete("ToDo", {"description": ("like", PREFIX + "%")})
		frappe.db.commit()

	def test_prune_drops_a_doctype_that_indexes_nothing_and_the_missing_documents(self):
		todo = frappe.get_doc(doctype="ToDo", description=PREFIX + "kept").insert(ignore_permissions=True)
		frappe.db.delete("__global_search", {"doctype": "ToDo", "name": todo.name})
		add_search_row("ToDo", todo.name)
		add_search_row("ToDo", PREFIX + "gone")
		add_search_row("Scheduled Job Log", PREFIX + "log")
		with patch(SETTINGS, return_value=["ToDo"]):
			prune_global_search(["ToDo", "Scheduled Job Log"])
		self.assertEqual(search_rows("ToDo"), [])
		self.assertEqual(search_rows("Scheduled Job Log"), [])
		kept = frappe.db.sql_list(
			"select name from `__global_search` where doctype = 'ToDo' and name = %s", todo.name
		)
		self.assertEqual(kept, [todo.name])
