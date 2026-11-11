# //// Neoffice — added file (no upstream equivalent): the history of a document shows a reader only the fields
# //// they may read (neoffice-maintenance#2296, security audit question 15). Each test fails on the code before it.
"""A Version's data holds the old and the new value of every field a save changed. Upstream version-15 handed it whole
to whoever opened the form, the Version form or a list of Version, so a reader without the permlevel of a field read
both of its values in the history. A field at permlevel 1 is changed; a reader of level 0 only does not see it, by any
path, and a reader of level 1 does. Administrator only sets the scenery: the reading is played by the readers."""

from unittest.mock import patch

import frappe
from frappe.client import get as client_get
from frappe.client import get_value as client_get_value
from frappe.custom.doctype.property_setter.property_setter import make_property_setter
from frappe.desk.form.load import get_docinfo, get_versions, getdoc
from frappe.permissions import add_permission
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase

READER_ROLE = "_Test Version History Reader"
LEVEL_1_ROLE = "_Test Version History Level 1 Reader"
LEVEL_0_READER = "test-version-history-level-0@yopmail.com"
LEVEL_1_READER = "test-version-history-level-1@yopmail.com"
OLD_AMOUNT = "18000"
NEW_AMOUNT = "21500"


def _make_reader(email: str, roles: list[str]) -> str:
	user = frappe.new_doc("User")
	user.update({"email": email, "first_name": email.split("@")[0], "send_welcome_email": 0})
	user.set("roles", [{"role": role} for role in roles])
	user.insert(ignore_permissions=True)
	return email


class TestAVersionShowsOnlyWhatItsReaderMayRead(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# our fork sends a mail at once and commits: nothing of this scenery may outlive the rollback
		mail = patch("frappe.sendmail")
		mail.start()
		cls.addClassCleanup(mail.stop)
		cls.addClassCleanup(cls._forget_the_scenery)

		frappe.set_user("Administrator")
		for role in (READER_ROLE, LEVEL_1_ROLE):
			frappe.get_doc({"doctype": "Role", "role_name": role, "desk_access": 1}).insert(
				ignore_if_duplicate=True
			)
		# the description of a ToDo plays the amount of a project phase: a field above permlevel 0
		make_property_setter("ToDo", "description", "permlevel", 1, "Int")
		add_permission("ToDo", READER_ROLE, 0)
		# a rule above level 0 needs the role's own level 0 rule
		add_permission("ToDo", LEVEL_1_ROLE, 0)
		add_permission("ToDo", LEVEL_1_ROLE, 1)
		# the Version form and the lists of Version are read by whoever holds a read right on Version
		add_permission("Version", READER_ROLE, 0)
		frappe.clear_cache(doctype="ToDo")
		frappe.clear_cache(doctype="Version")
		_make_reader(LEVEL_0_READER, [READER_ROLE])
		_make_reader(LEVEL_1_READER, [READER_ROLE, LEVEL_1_ROLE])

		todo = frappe.get_doc(
			{
				"doctype": "ToDo",
				"description": f"Structural work: {OLD_AMOUNT}\nfirst estimate",
				"priority": "Low",
			}
		).insert()
		todo.description = f"Structural work: {NEW_AMOUNT}\nrevised estimate"
		todo.priority = "High"
		todo.save(ignore_version=False)
		cls.todo = todo.name
		cls.version = frappe.db.get_value("Version", {"ref_doctype": "ToDo", "docname": todo.name})

	@classmethod
	def _forget_the_scenery(cls):
		frappe.db.rollback()
		frappe.clear_cache(doctype="ToDo")
		frappe.clear_cache(doctype="Version")
		for email in (LEVEL_0_READER, LEVEL_1_READER):
			frappe.clear_cache(user=email)

	def assertHides(self, payload):
		text = frappe.as_json(payload)
		self.assertNotIn(OLD_AMOUNT, text)
		self.assertNotIn(NEW_AMOUNT, text)

	def assertShows(self, payload):
		text = frappe.as_json(payload)
		self.assertIn(OLD_AMOUNT, text)
		self.assertIn(NEW_AMOUNT, text)

	def changed_fields(self, versions):
		return {
			entry[0] for version in versions for entry in frappe.parse_json(version.data).get("changed", [])
		}

	def test_the_scenery_holds_a_version_of_both_fields(self):
		self.assertTrue(self.version)
		data = frappe.parse_json(frappe.db.get_value("Version", self.version, "data"))
		self.assertLessEqual({"description", "priority"}, {entry[0] for entry in data.changed})

	def test_the_form_history_hides_a_field_its_reader_may_not_read(self):
		with as_role(LEVEL_0_READER):
			versions = get_versions(frappe.get_doc("ToDo", self.todo))

		self.assertTrue(versions)
		self.assertHides(versions)
		# the fix is targeted: what the reader may read, and who changed it, stays
		self.assertIn("priority", self.changed_fields(versions))
		self.assertNotIn("description", self.changed_fields(versions))
		self.assertTrue(all(version.owner for version in versions))

	def test_the_form_history_shows_it_to_a_reader_of_its_level(self):
		with as_role(LEVEL_1_READER):
			versions = get_versions(frappe.get_doc("ToDo", self.todo))

		self.assertShows(versions)
		self.assertLessEqual({"description", "priority"}, self.changed_fields(versions))

	def test_the_docinfo_of_the_form_hides_it(self):
		with as_role(LEVEL_0_READER):
			get_docinfo(doctype="ToDo", name=self.todo)
			versions = frappe.response["docinfo"]["versions"]

		self.assertTrue(versions)
		self.assertHides(versions)

	def test_the_version_read_as_a_document_hides_it(self):
		with as_role(LEVEL_0_READER):
			version = client_get("Version", self.version)
		self.assertHides(version["data"])
		self.assertIn("priority", version["data"])

		with as_role(LEVEL_1_READER):
			version = client_get("Version", self.version)
		self.assertShows(version["data"])

	def test_the_version_form_hides_it_and_its_diff(self):
		with as_role(LEVEL_0_READER):
			frappe.local.response = frappe._dict({"docs": []})
			getdoc("Version", self.version)
			version = frappe.response.docs[-1]
		self.assertHides(version.data)
		self.assertNotIn("description", (version.get_onload() or {}).get("html_diffs") or {})

		with as_role(LEVEL_1_READER):
			frappe.local.response = frappe._dict({"docs": []})
			getdoc("Version", self.version)
			version = frappe.response.docs[-1]
		self.assertShows(version.data)
		self.assertIn("description", version.get_onload().get("html_diffs"))

	def test_a_list_of_version_hides_it(self):
		filters = {"ref_doctype": "ToDo", "docname": self.todo}
		with as_role(LEVEL_0_READER):
			as_dicts = frappe.get_list("Version", filters=filters, fields=["name", "data"])
			as_lists = frappe.get_list("Version", filters=filters, fields=["data", "name"], as_list=True)
			plucked = frappe.get_list("Version", filters=filters, pluck="data")
			aliased = frappe.get_list("Version", filters=filters, fields=["data as history"])
			expression = frappe.get_list("Version", filters=filters, fields=["name", "max(data) as history"])
			value = client_get_value("Version", "data", self.version)

		for rows in (as_dicts, as_lists, plucked, aliased, expression, value):
			self.assertTrue(rows)
			self.assertHides(rows)
		# the doctype the filter needs is selected for it and handed back to nobody
		self.assertEqual(set(as_dicts[0]), {"name", "data"})
		self.assertEqual(len(as_lists[0]), 2)
		self.assertIn("priority", plucked[0])
		self.assertIn("priority", aliased[0].history)

		with as_role(LEVEL_1_READER):
			self.assertShows(frappe.get_list("Version", filters=filters, pluck="data"))
