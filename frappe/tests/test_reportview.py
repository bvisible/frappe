# Copyright (c) 2019, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import frappe
from frappe.desk.reportview import (  # //// Neoffice — see the test at the end
	export_query,
	extract_fieldnames,
	get_distinct_values,
)
from frappe.tests.utils import FrappeTestCase


class TestReportview(FrappeTestCase):
	def test_csv(self):
		from csv import QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONE, QUOTE_NONNUMERIC, DictReader
		from io import StringIO

		frappe.local.form_dict = frappe._dict(
			doctype="DocType",
			file_format_type="CSV",
			fields=("name", "module", "issingle"),
			filters={"issingle": 1, "module": "Core"},
		)

		for delimiter in (",", ";", "\t", "|"):
			frappe.local.form_dict.csv_delimiter = delimiter
			for quoting in (QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONE, QUOTE_NONNUMERIC):
				frappe.local.form_dict.csv_quoting = quoting

				export_query()

				self.assertTrue(frappe.response["filename"].endswith(".csv"))
				self.assertEqual(frappe.response["type"], "binary")
				with StringIO(frappe.response["filecontent"].decode("utf-8")) as result:
					reader = DictReader(result, delimiter=delimiter, quoting=quoting)
					for row in reader:
						self.assertEqual(int(row["Is Single"]), 1)
						self.assertEqual(row["Module"], "Core")

	def test_extract_fieldname(self):
		self.assertEqual(
			extract_fieldnames("count(distinct `tabPhoto`.name) as total_count")[0], "tabPhoto.name"
		)

		self.assertEqual(extract_fieldnames("owner")[0], "owner")
		self.assertEqual(extract_fieldnames("from")[0], "from")

		self.assertEqual(extract_fieldnames("module")[0], "module")

		self.assertEqual(extract_fieldnames("count(`tabPhoto`.name) as total_count")[0], "tabPhoto.name")

		self.assertEqual(extract_fieldnames("count(distinct `tabPhoto`.name)")[0], "tabPhoto.name")

		self.assertEqual(extract_fieldnames("count(`tabPhoto`.name)")[0], "tabPhoto.name")

		self.assertEqual(
			extract_fieldnames("count(distinct `tabJob Applicant`.name) as total_count")[0],
			"tabJob Applicant.name",
		)

		self.assertEqual(
			extract_fieldnames("(1 / nullif(locate('a', `tabAddress`.`name`), 0)) as `_relevance`")[0],
			"tabAddress.name",
		)

		self.assertEqual(
			extract_fieldnames("(1 / nullif(locate('(a)', `tabAddress`.`name`), 0)) as `_relevance`")[0],
			"tabAddress.name",
		)

		self.assertEqual(extract_fieldnames("EXTRACT(MONTH FROM date_column) AS month")[0], "date_column")

		self.assertEqual(extract_fieldnames("COUNT(*) AS count")[0], "*")

		self.assertEqual(
			extract_fieldnames("first_name + ' ' + last_name AS full_name"), ["first_name", "last_name"]
		)

		self.assertEqual(
			extract_fieldnames("CONCAT(first_name, ' ', last_name) AS full_name"),
			["first_name", "last_name"],
		)

		self.assertEqual(
			extract_fieldnames("CONCAT(id, '/', name, '/', age, '/', marks) AS student"),
			["id", "name", "age", "marks"],
		)

		self.assertEqual(extract_fieldnames("tablefield.fiedname")[0], "tablefield.fiedname")

		self.assertEqual(extract_fieldnames("`tabChild DocType`.`fiedname`")[0], "tabChild DocType.fiedname")

		self.assertEqual(extract_fieldnames("sum(1)"), [])

	def test_export_report_via_email(self):
		frappe.local.form_dict = frappe._dict(
			doctype="DocType",
			file_format_type="CSV",
			fields=("name", "module", "issingle"),
			filters={"issingle": 1, "module": "Core"},
			export_in_background=1,
		)

		frappe.db.delete("Email Queue")
		export_query()
		jobs = frappe.get_all(
			"RQ Job",
			filters={"job_name": "frappe.desk.query_report.run_report_view_export_job"},
			fields=["name", "status"],
		)
		email_queue = frappe.get_all("Email Queue")

		self.assertTrue(jobs, "Background job was not enqueued")
		self.assertTrue(email_queue, "Email was not enqueued")

	# //// Neoffice — added test (maintenance#2054): get_distinct_values, our fork's endpoint at the end of
	# //// reportview.py, read with ignore_permissions=True. It must refuse a Website User, serve a permitted
	# //// user, and refuse anything but a bare field the caller may read.
	def test_get_distinct_values_permissions(self):
		website_user = "reportview-distinct-website@example.com"
		if not frappe.db.exists("User", website_user):
			frappe.get_doc(
				doctype="User",
				email=website_user,
				first_name="Distinct",
				user_type="Website User",
				send_welcome_email=0,
			).insert(ignore_permissions=True)

		with self.set_user(website_user):
			self.assertRaises(frappe.PermissionError, get_distinct_values, "Role", "name", 10000)
			self.assertRaises(frappe.PermissionError, get_distinct_values, "DocType", "module")
			# every user's own User record is shared with them, and only that one comes back
			self.assertEqual([row.name for row in get_distinct_values("User", "name", 10000)], [website_user])

		names = {row.name for row in get_distinct_values("Role", "name", 10000)}
		self.assertIn("System Manager", names)
		self.assertEqual(len(get_distinct_values("Role", "name", 1)), 1)

		self.assertRaises(frappe.DataError, get_distinct_values, "Role", "count(name)")
		self.assertRaises(frappe.DataError, get_distinct_values, "Role", "`tabUser`.`email`")
		self.assertRaises(frappe.DataError, get_distinct_values, "Role", "no_such_field")
