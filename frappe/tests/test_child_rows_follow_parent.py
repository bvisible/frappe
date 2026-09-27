# //// Neoffice — added file (no upstream equivalent): tests for neoffice-maintenance#894, a list
# //// of child rows follows each row's parent (DatabaseQuery.get_child_row_parent_condition).
# //// Drop together with that change once upstream filters child rows by their parent.
"""A list of child rows shows the rows whose parent the user could list, and no other.

Upstream, frappe.client.get_list / frappe.get_list on a child doctype only checks that
the user may read the parent doctype they name, at doctype level. The parent's own
rules (User Permissions, permission_query_conditions hooks, if_owner, shares) were not
applied, and rows of other parent types came out too. Every test goes through an entry
point a caller uses, and each one marked "fails upstream" fails on that code; the others
guard what must not change.
"""

import json

import frappe
import frappe.share
from frappe.client import get_list as client_get_list
from frappe.client import get_value as client_get_value
from frappe.core.doctype.doctype.test_doctype import new_doctype
from frappe.desk.doctype.number_card.number_card import get_result as number_card_result
from frappe.desk.reportview import get_count as reportview_get_count
from frappe.desk.reportview import get_match_cond
from frappe.permissions import add_user_permission, clear_user_permissions_for_doctype
from frappe.tests.utils import FrappeTestCase, patch_hooks

CHILD = "Test Followed Row"
SLIP = "Test Row Parent Slip"
STRUCTURE = "Test Row Parent Structure"
CLAIM = "Test Row Parent Claim"
CATEGORY = "Test Row Parent Category"
DOCTYPES = (SLIP, STRUCTURE, CLAIM, CATEGORY, CHILD)

READER_ROLE = "Test Row Parent Reader"
STAFF_ROLE = "Test Row Parent Staff"
OUTSIDER_ROLE = "Test Row Parent Outsider"
ROLES = (READER_ROLE, STAFF_ROLE, OUTSIDER_ROLE)
READER = "row-parent-reader@example.com"
STAFF = "row-parent-staff@example.com"
OUTSIDER = "row-parent-outsider@example.com"  # reads the categories, and none of the parents
USERS = {READER: READER_ROLE, STAFF: STAFF_ROLE, OUTSIDER: OUTSIDER_ROLE}

SLIP_ROWS = {"slip-mine-1", "slip-mine-2", "slip-other-1"}
STRUCTURE_ROWS = {"structure-1-1", "structure-2-1"}
CLAIM_ROWS = {"claim-reader-1", "claim-admin-1"}


def slips_with_an_open_note(user=None, doctype=None):
	"""A permission_query_conditions hook on the parent, patched in by a test."""
	return f"`tab{SLIP}`.`note` = 'open'"


def labels_query(user, parent_doctype):
	"""The SQL frappe.get_list would run for `user`, without running it."""
	frappe.set_user(user)
	try:
		return frappe.get_list(CHILD, parent_doctype=parent_doctype, fields=["label"], run=False)
	finally:
		frappe.set_user("Administrator")


def labels(user, parent_doctype, **kwargs):
	"""The labels of the child rows `user` lists through frappe.get_list."""
	frappe.set_user(user)
	try:
		return set(frappe.get_list(CHILD, parent_doctype=parent_doctype, pluck="label", **kwargs))
	finally:
		frappe.set_user("Administrator")


class TestChildRowsFollowParent(FrappeTestCase):
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
		new_doctype(CHILD, istable=1, permissions=[], fields=[_data("label")]).insert()
		new_doctype(
			CATEGORY,
			autoname="field:title",
			fields=[_data("title")],
			permissions=_perms(READER_ROLE, STAFF_ROLE, OUTSIDER_ROLE),
		).insert()
		new_doctype(
			SLIP,
			fields=[
				{"label": "Category", "fieldname": "category", "fieldtype": "Link", "options": CATEGORY},
				_data("note"),
				_table(),
			],
			permissions=_perms(READER_ROLE, STAFF_ROLE),
		).insert()
		# The reader does not read this parent type at all.
		new_doctype(STRUCTURE, fields=[_data("note"), _table()], permissions=_perms(STAFF_ROLE)).insert()
		# The reader reads this one "if owner" only.
		new_doctype(
			CLAIM,
			fields=[_data("note"), _table()],
			permissions=[
				*_perms(STAFF_ROLE),
				{"role": READER_ROLE, "read": 1, "write": 1, "create": 1, "if_owner": 1},
			],
		).insert()

		for email, role in USERS.items():
			frappe.get_doc(
				{
					"doctype": "User",
					"email": email,
					"first_name": email.split("@")[0],
					"send_welcome_email": 0,
					"roles": [{"role": role}],
				}
			).insert()

		for title in ("Mine", "Other"):
			frappe.get_doc({"doctype": CATEGORY, "title": title}).insert()
		cls.slip_mine = _parent(SLIP, ["slip-mine-1", "slip-mine-2"], category="Mine", note="open")
		cls.slip_other = _parent(SLIP, ["slip-other-1"], category="Other", note="hidden")
		cls.structure_1 = _parent(STRUCTURE, ["structure-1-1"])
		cls.structure_2 = _parent(STRUCTURE, ["structure-2-1"])
		cls.claim_reader = _parent(CLAIM, ["claim-reader-1"], owner=READER)
		cls.claim_admin = _parent(CLAIM, ["claim-admin-1"])
		frappe.db.commit()

	def tearDown(self):
		frappe.set_user("Administrator")

	def test_a_table_added_by_custom_field_makes_a_parent_with_its_own_rules(self):
		# fails upstream: the rows of the category the reader may not read came out
		field = frappe.get_doc(
			{
				"doctype": "Custom Field",
				"dt": CATEGORY,
				"fieldname": "extra_rows",
				"label": "Extra Rows",
				"fieldtype": "Table",
				"options": CHILD,
			}
		).insert()
		self.addCleanup(frappe.delete_doc, "Custom Field", field.name)
		self.addCleanup(frappe.db.delete, CHILD, {"parenttype": CATEGORY})
		for title in ("Mine", "Other"):
			category = frappe.get_doc(CATEGORY, title)
			category.append("extra_rows", {"label": f"category-{title.lower()}-1"})
			category.save()

		add_user_permission(CATEGORY, "Mine", READER, ignore_permissions=True)
		self.addCleanup(clear_user_permissions_for_doctype, CATEGORY, READER)
		only_categories = {"parenttype": CATEGORY}
		self.assertEqual(labels(READER, SLIP, filters=only_categories), {"category-mine-1"})
		# and a parent the user reads in full keeps all its rows: the custom table was found
		self.assertEqual(
			labels(STAFF, SLIP, filters=only_categories), {"category-mine-1", "category-other-1"}
		)

	def test_rows_of_a_parent_type_the_user_does_not_read_are_left_out(self):
		# fails upstream. The reader names a parent type they read; the rows of the one they
		# do not read (Structure) and of the claims they do not own stayed in the list upstream.
		expected = SLIP_ROWS | {"claim-reader-1"}
		self.assertEqual(labels(READER, SLIP), expected)

		frappe.set_user(READER)
		listed = client_get_list(CHILD, fields=["label"], parent=SLIP, limit_page_length=0)
		self.assertEqual({row.label for row in listed}, expected)

	def test_a_user_permission_on_the_parent_hides_the_rows_of_its_other_records(self):
		# fails upstream
		add_user_permission(CATEGORY, "Mine", READER, ignore_permissions=True)
		self.addCleanup(clear_user_permissions_for_doctype, CATEGORY, READER)

		self.assertEqual(labels(READER, SLIP, filters={"parenttype": SLIP}), {"slip-mine-1", "slip-mine-2"})

	def test_a_permission_hook_on_the_parent_hides_the_rows_of_the_records_it_filters_out(self):
		# fails upstream
		hooks = {"permission_query_conditions": {SLIP: [f"{__name__}.slips_with_an_open_note"]}}
		with patch_hooks(hooks):
			self.assertEqual(
				labels(READER, SLIP, filters={"parenttype": SLIP}), {"slip-mine-1", "slip-mine-2"}
			)

	def test_a_parent_read_if_owner_shows_the_rows_of_the_users_own_records(self):
		# fails upstream
		self.assertEqual(labels(READER, CLAIM, filters={"parenttype": CLAIM}), {"claim-reader-1"})

	def test_one_shared_record_opens_its_own_rows_not_those_of_the_whole_type(self):
		# fails upstream. The reader lists through a parent they read (Slip) and asks for Structure
		# rows: upstream gave every Structure's rows, although the reader reads no Structure.
		only_structures = {"parenttype": STRUCTURE}
		self.assertEqual(labels(READER, SLIP, filters=only_structures), set())

		frappe.share.add(STRUCTURE, self.structure_1.name, READER, read=1)
		self.addCleanup(frappe.share.remove, STRUCTURE, self.structure_1.name, READER)
		self.assertEqual(labels(READER, SLIP, filters=only_structures), {"structure-1-1"})

	def test_the_count_follows_the_rows(self):
		# fails upstream: 3 instead of 2, through each of the three ways a count is asked
		add_user_permission(CATEGORY, "Mine", READER, ignore_permissions=True)
		self.addCleanup(clear_user_permissions_for_doctype, CATEGORY, READER)
		filters = [[CHILD, "parenttype", "=", SLIP]]

		frappe.set_user(READER)
		counted = frappe.get_list(CHILD, parent_doctype=SLIP, filters=filters, fields=["count(*) as total"])
		self.assertEqual(counted[0].total, 2)

		form_dict = frappe.local.form_dict
		self.addCleanup(setattr, frappe.local, "form_dict", form_dict)
		frappe.local.form_dict = frappe._dict(doctype=CHILD, parent_doctype=SLIP, filters=json.dumps(filters))
		self.assertEqual(reportview_get_count(), 2)

		# a number card on the child table, as a dashboard asks for it
		card = {"document_type": CHILD, "parent_document_type": SLIP, "function": "Count"}
		self.assertEqual(number_card_result(card, filters), 2)

	def test_get_value_on_a_row_of_a_hidden_record_finds_nothing(self):
		# fails upstream
		add_user_permission(CATEGORY, "Mine", READER, ignore_permissions=True)
		self.addCleanup(clear_user_permissions_for_doctype, CATEGORY, READER)
		mine, other = self.slip_mine.rows[0], self.slip_other.rows[0]

		frappe.set_user(READER)
		self.assertEqual(
			client_get_value(CHILD, "label", filters={"name": mine.name}, parent=SLIP),
			{"label": "slip-mine-1"},
		)
		self.assertEqual(client_get_value(CHILD, "label", filters={"name": other.name}, parent=SLIP), {})

	def test_staff_who_read_every_parent_in_full_see_every_row_and_the_query_is_unchanged(self):
		every_row = SLIP_ROWS | STRUCTURE_ROWS | CLAIM_ROWS
		for user in (STAFF, "Administrator"):
			self.assertEqual(labels(user, SLIP), every_row)
			# no condition on the parent at all: the query is the one upstream runs
			query = labels_query(user, SLIP)
			self.assertNotIn("parenttype", query)
			self.assertEqual(query.lower().count("select"), 1)

	def test_a_user_who_reads_no_parent_gets_no_row_from_raw_sql(self):
		# fails upstream: get_match_cond on a child table was empty, so a raw query built on it
		# (as ERPNext's link queries are) returned every row to a user who reads no parent
		frappe.set_user(OUTSIDER)
		rows = frappe.db.sql(f"select label from `tab{CHILD}` where 1=1 {get_match_cond(CHILD)}", pluck=True)
		self.assertEqual(rows, [])

		# while the reader, through the same raw query, gets the rows of what they read
		frappe.set_user(READER)
		rows = frappe.db.sql(f"select label from `tab{CHILD}` where 1=1 {get_match_cond(CHILD)}", pluck=True)
		self.assertEqual(set(rows), SLIP_ROWS | {"claim-reader-1"})

	def test_ignore_permissions_still_lists_every_row(self):
		frappe.set_user(READER)
		listed = frappe.get_list(CHILD, parent_doctype=SLIP, pluck="label", ignore_permissions=True)
		self.assertEqual(set(listed), SLIP_ROWS | STRUCTURE_ROWS | CLAIM_ROWS)


def _data(fieldname):
	return {"label": fieldname.title(), "fieldname": fieldname, "fieldtype": "Data"}


def _table():
	return {"label": "Rows", "fieldname": "rows", "fieldtype": "Table", "options": CHILD}


def _perms(*roles):
	return [
		{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
		# a DocPerm row grants write, create and delete unless told otherwise
		*({"role": role, "read": 1, "write": 0, "create": 0, "delete": 0} for role in roles),
	]


def _parent(doctype, row_labels, owner=None, **values):
	doc = frappe.get_doc({"doctype": doctype, **values, "rows": [{"label": label} for label in row_labels]})
	doc.insert()
	if owner:
		# insert() always makes the session user the owner.
		frappe.db.set_value(doctype, doc.name, "owner", owner, update_modified=False)
	return doc


def _drop_fixtures():
	"""Remove what setUpClass creates, including what a failed earlier run left behind."""
	frappe.set_user("Administrator")
	frappe.db.delete("Custom Field", {"options": CHILD})
	for doctype in DOCTYPES:
		frappe.delete_doc_if_exists("DocType", doctype, force=True)
		frappe.db.sql_ddl(f"drop table if exists `tab{doctype}`")
	for user in USERS:
		frappe.db.delete("User Permission", {"user": user})
		frappe.db.delete("DocShare", {"user": user})
		frappe.delete_doc_if_exists("User", user, force=True)
	for role in ROLES:
		frappe.delete_doc_if_exists("Role", role, force=True)
	frappe.db.commit()
