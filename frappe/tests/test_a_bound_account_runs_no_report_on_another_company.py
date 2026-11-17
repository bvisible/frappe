# //// Neoffice — added file (no upstream equivalent).
"""An account bound to some companies runs no report on another company (neoffice-maintenance#2838).

A script or query report reads the company its `company` filter names, in SQL, and Frappe filters its result afterwards only on the
Link columns it shows: most financial and stock reports show no company column. An account bound to some companies by a User
Permission on « Company » chose another company in the filter and read its books. That company is now refused; nothing changes
for an account without a bound, for a bound that applies to other doctypes only, nor for a report run without a company filter.

The reader is bound by a User Permission row: the companies need not exist (a Frappe site without ERPNext has none). The report
is scenery, made by Administrator; every run is the reader's.
"""

import json

import frappe
from frappe.desk.query_report import run
from frappe.tests.utils import FrappeTestCase

READER = "test_2838_report_reader@example.com"
OWN = "_Test Company 2838 Own"
OTHER = "_Test Company 2838 Other"


class TestABoundAccountRunsNoReportOnAnotherCompany(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		frappe.get_doc(
			{
				"doctype": "User",
				"email": READER,
				"first_name": "Report Reader 2838",
				"send_welcome_email": 0,
				"roles": [{"role": "Website Manager"}],
			}
		).insert()
		# A report that shows no company column, as most financial reports do; a desk user may run it (Note, « Desk User »)
		self.report = frappe.get_doc(
			{
				"doctype": "Report",
				"ref_doctype": "Note",
				"report_name": "Books Of A Company " + frappe.generate_hash(length=6),
				"report_type": "Query Report",
				"query": "select name from tabNote",
				"is_standard": "No",
			}
		).insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.db.rollback()
		frappe.cache.hdel("user_permissions", READER)

	def bind(self, company, applicable_for=None):
		frappe.get_doc(
			{
				"doctype": "User Permission",
				"name": frappe.generate_hash(length=10),
				"user": READER,
				"allow": "Company",
				"for_value": company,
				"apply_to_all_doctypes": 0 if applicable_for else 1,
				"applicable_for": applicable_for,
			}
		).db_insert()
		frappe.cache.hdel("user_permissions", READER)

	def run_as_the_reader(self, **filters):
		with self.set_user(READER):
			return run(self.report.name, json.dumps(filters))

	def test_a_company_outside_the_bound_is_refused(self):
		self.bind(OWN)
		with self.assertRaisesRegex(frappe.PermissionError, OTHER):
			self.run_as_the_reader(company=OTHER)

	def test_a_list_of_companies_with_one_outside_the_bound_is_refused(self):
		self.bind(OWN)
		with self.assertRaisesRegex(frappe.PermissionError, OTHER):
			self.run_as_the_reader(company=[OWN, OTHER])

	def test_the_bound_company_still_runs(self):
		self.bind(OWN)
		self.assertIn("result", self.run_as_the_reader(company=OWN))
		self.assertIn("result", self.run_as_the_reader(company=[OWN]))

	def test_a_report_without_a_company_filter_still_runs_for_a_bound_account(self):
		self.bind(OWN)
		self.assertIn("result", self.run_as_the_reader())

	def test_nothing_changes_for_an_account_without_a_bound(self):
		self.assertIn("result", self.run_as_the_reader(company=OTHER))

	def test_a_bound_that_applies_to_another_doctype_leaves_the_report_alone(self):
		self.bind(OWN, applicable_for="ToDo")
		self.assertIn("result", self.run_as_the_reader(company=OTHER))
