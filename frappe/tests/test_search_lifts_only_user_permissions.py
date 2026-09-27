# //// Neoffice — added file (no upstream equivalent): tests for neoffice-maintenance#894, the link
# //// search lifts a user's User Permissions only for a field set to ignore them, and nothing else
# //// (frappe.desk.search.may_ignore_user_permissions). Drop at the v16 merge, with that function.
"""A link search that ignores user permissions lifts those, for a field that allows it, and nothing else.

Upstream v15, frappe.desk.search.search_widget took `ignore_user_permissions=1` on trust
from any caller who could read the doctype (or the parent of a child table), and then
listed with ignore_permissions=True: every record, every column the caller picked through
`filter_fields`, past the permission hooks, if_owner and field-level permissions, and every
row of a child table. The tests go through frappe.call, the dispatcher of whitelisted
methods, so that on upstream code the new arguments are dropped as a request would drop
them: each test marked "fails upstream" then fails, rather than erroring.
"""

import json

import frappe
from frappe.core.doctype.doctype.test_doctype import new_doctype
from frappe.permissions import add_user_permission
from frappe.tests.utils import FrappeTestCase, patch_hooks

TARGET = "Test Search Lift Target"
TARGET_ROW = "Test Search Lift Target Row"
FORM = "Test Search Lift Form"
FORM_ROW = "Test Search Lift Form Row"
DOCTYPES = (TARGET, TARGET_ROW, FORM, FORM_ROW)

TARGET_READER = "Test Search Lift Target Reader"
FORM_WRITER = "Test Search Lift Form Writer"
FORM_READER = "Test Search Lift Form Reader"
ROLES = (TARGET_READER, FORM_WRITER, FORM_READER)

# Both read the targets, and a User Permission limits both to "target-mine".
WRITER = "search-lift-writer@example.com"  # can fill the form
LOOKER = "search-lift-looker@example.com"  # can only read it
USERS = {WRITER: (TARGET_READER, FORM_WRITER), LOOKER: (TARGET_READER, FORM_READER)}

# A DocPerm row grants write, create and delete unless told otherwise.
READ_ONLY = {"read": 1, "write": 0, "create": 0, "delete": 0}

MINE = {"target-mine"}
EVERY_TARGET = {"target-mine", "target-other", "target-hooked"}
QUERY_CALLS = []


def hide_the_hooked_target(user=None, doctype=None):
	"""A permission_query_conditions hook on the target, patched in by a test."""
	return f"`tab{TARGET}`.`title` != 'target-hooked'"


@frappe.whitelist()
def record_the_claim(doctype, txt, searchfield, start, page_len, filters, **kwargs):
	"""A custom search query that records the claim it is handed."""
	QUERY_CALLS.append(kwargs.get("ignore_user_permissions"))
	return []


def search(user, doctype=TARGET, **kwargs):
	"""The rows `user` gets from the whitelisted search_widget, dispatched as a request is."""
	frappe.set_user(user)
	try:
		return frappe.call("frappe.desk.search.search_widget", doctype=doctype, txt="", as_dict=1, **kwargs)
	finally:
		frappe.set_user("Administrator")


def names(user, **kwargs):
	return {row.name for row in search(user, **kwargs)}


def claim(field, form=FORM):
	"""The arguments of a search that claims to ignore user permissions for `field` of `form`."""
	return {
		"ignore_user_permissions": 1,
		"reference_doctype": form,
		"form_doctype": form,
		"link_fieldname": field,
	}


class TestSearchLiftsOnlyUserPermissions(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		_drop_fixtures()
		# A class cleanup runs even when setUpClass fails half-way, unlike tearDownClass.
		cls.addClassCleanup(_drop_fixtures)

		for role in ROLES:
			frappe.get_doc({"doctype": "Role", "role_name": role, "desk_access": 1}).insert()

		# DocType creation is DDL and commits: the class cleanup removes all of it explicitly.
		new_doctype(TARGET_ROW, istable=1, permissions=[], fields=[_data("label")]).insert()
		new_doctype(
			TARGET,
			autoname="field:title",
			fields=[
				_data("title"),
				{**_data("secret"), "permlevel": 1},
				{"label": "Rows", "fieldname": "rows", "fieldtype": "Table", "options": TARGET_ROW},
			],
			permissions=[
				{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
				{"role": "System Manager", "read": 1, "permlevel": 1},
				{"role": TARGET_READER, **READ_ONLY},
			],
		).insert()
		links = [
			{**_link("open_target"), "ignore_user_permissions": 1},
			_link("plain_target"),
			{**_link("open_role"), "options": "Role", "ignore_user_permissions": 1},
		]
		new_doctype(FORM_ROW, istable=1, permissions=[], fields=links).insert()
		new_doctype(
			FORM,
			fields=[
				*links,
				{"label": "Lines", "fieldname": "lines", "fieldtype": "Table", "options": FORM_ROW},
			],
			permissions=[
				{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
				{"role": FORM_WRITER, "read": 1, "write": 1, "create": 1},
				{"role": FORM_READER, **READ_ONLY},
			],
		).insert()

		for email, roles in USERS.items():
			frappe.get_doc(
				{
					"doctype": "User",
					"email": email,
					"first_name": email.split("@")[0],
					"send_welcome_email": 0,
					"roles": [{"role": role} for role in roles],
				}
			).insert()

		for title in sorted(EVERY_TARGET):
			frappe.get_doc(
				{
					"doctype": TARGET,
					"title": title,
					"secret": f"secret of {title}",
					"rows": [{"label": f"row of {title}"}],
				}
			).insert()
		for user in USERS:
			add_user_permission(TARGET, "target-mine", user, ignore_permissions=True)
		frappe.db.commit()

	def tearDown(self):
		frappe.set_user("Administrator")

	def test_without_a_claim_the_user_permissions_apply(self):
		self.assertEqual(names(WRITER), MINE)

	def test_a_claim_that_names_no_field_is_refused(self):
		# fails upstream: the bare flag listed every target
		self.assertEqual(names(WRITER, ignore_user_permissions=1, reference_doctype=FORM), MINE)

	def test_a_field_that_does_not_ignore_user_permissions_proves_nothing(self):
		# fails upstream
		self.assertEqual(names(WRITER, **claim("plain_target")), MINE)

	def test_a_field_that_links_to_another_doctype_proves_nothing(self):
		# fails upstream: open_role ignores user permissions, but on Role, not on the target
		self.assertEqual(names(WRITER, **claim("open_role")), MINE)

	def test_a_field_that_ignores_user_permissions_lifts_them_for_a_user_who_can_fill_it(self):
		self.assertEqual(names(WRITER, **claim("open_target")), EVERY_TARGET)
		# the same field in a child table of a form the user can fill
		self.assertEqual(names(WRITER, **claim("open_target", form=FORM_ROW)), EVERY_TARGET)
		self.assertIsNone(frappe.flags.get("ignore_user_permissions_for_doctype"))

		# search_link, what the Link control calls, hands the field on
		frappe.set_user(WRITER)
		found = frappe.call("frappe.desk.search.search_link", doctype=TARGET, txt="", **claim("open_target"))
		self.assertEqual({row["value"] for row in found}, EVERY_TARGET)

	def test_a_user_who_can_only_read_the_form_gets_no_lift(self):
		# fails upstream: reading the target was enough
		self.assertEqual(names(LOOKER, **claim("open_target")), MINE)
		self.assertEqual(names(LOOKER, **claim("open_target", form=FORM_ROW)), MINE)

	def test_the_lift_keeps_the_permission_hooks_of_the_doctype(self):
		# fails upstream: ignore_permissions=True skipped the hook
		hooks = {"permission_query_conditions": {TARGET: [f"{__name__}.hide_the_hooked_target"]}}
		with patch_hooks(hooks):
			self.assertEqual(names(WRITER, **claim("open_target")), {"target-mine", "target-other"})

	def test_the_lift_keeps_field_level_permissions(self):
		# fails upstream: filter_fields returned a permlevel-1 column the user may not read
		rows = search(WRITER, filter_fields=json.dumps(["secret"]), **claim("open_target"))
		self.assertEqual({row.name for row in rows}, EVERY_TARGET)
		self.assertEqual([row.get("secret") for row in rows if row.get("secret")], [])

	def test_the_rows_of_a_child_table_are_not_listed_through_the_search(self):
		# fails upstream: a reader of the parent got every row of the table
		for field in (None, "rows"):
			arguments = {"ignore_user_permissions": 1, "reference_doctype": TARGET}
			if field:
				arguments |= {"form_doctype": TARGET, "link_fieldname": field}
			with self.assertRaises(frappe.PermissionError):
				search(WRITER, doctype=TARGET_ROW, **arguments)

	def test_a_custom_query_is_handed_the_checked_claim(self):
		# fails upstream: the query got the caller's flag as sent
		query = f"{__name__}.record_the_claim"
		QUERY_CALLS.clear()
		search(WRITER, query=query, ignore_user_permissions=1, reference_doctype=FORM)
		search(WRITER, query=query, **claim("open_target"))
		self.assertEqual(QUERY_CALLS, [False, True])


def _data(fieldname):
	return {"label": fieldname.title(), "fieldname": fieldname, "fieldtype": "Data"}


def _link(fieldname):
	return {"label": fieldname.title(), "fieldname": fieldname, "fieldtype": "Link", "options": TARGET}


def _drop_fixtures():
	"""Remove what setUpClass creates, including what a failed earlier run left behind."""
	frappe.set_user("Administrator")
	for doctype in DOCTYPES:
		frappe.delete_doc_if_exists("DocType", doctype, force=True)
		frappe.db.sql_ddl(f"drop table if exists `tab{doctype}`")
	for user in USERS:
		frappe.db.delete("User Permission", {"user": user})
		frappe.delete_doc_if_exists("User", user, force=True)
	for role in ROLES:
		frappe.delete_doc_if_exists("Role", role, force=True)
	frappe.db.commit()
