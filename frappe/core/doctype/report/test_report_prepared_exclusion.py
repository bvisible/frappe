# //// Neoffice — added file (no upstream equivalent): the accounting reports that a slow run must not switch to Prepared
# //// Report mode for the whole site (see DEFAULT_AUTO_PREPARED_REPORT_EXCLUDE in report.py).
from unittest.mock import patch

import frappe
from frappe.core.doctype.report.report import DEFAULT_AUTO_PREPARED_REPORT_EXCLUDE
from frappe.tests.utils import FrappeTestCase


def _report(name, reference_report=None):
	report = frappe.new_doc("Report")
	report.name = name
	report.reference_report = reference_report
	return report


class TestAutoPreparedReportExclusion(FrappeTestCase):
	def test_the_statements_of_the_books_are_never_switched_by_a_slow_run(self):
		for name in (
			"General Ledger",
			"Trial Balance",
			"Account Sheets",
			"Bank Reconciliation Statement",
			"VAT Report",
			"Gross Profit",
			"Balance Sheet",
			"Profit and Loss Statement",
			"Cash Flow",
			"Accounts Receivable",
			"Accounts Payable",
		):
			self.assertIn(name, DEFAULT_AUTO_PREPARED_REPORT_EXCLUDE)
			self.assertTrue(_report(name).is_auto_prepared_report_disabled(), name)

	def test_another_report_still_switches(self):
		self.assertFalse(_report("Some Very Slow Report").is_auto_prepared_report_disabled())

	def test_a_custom_report_inherits_the_exclusion_of_its_standard_report(self):
		self.assertTrue(_report("My Balance Sheet", "Balance Sheet").is_auto_prepared_report_disabled())

	def test_a_site_can_override_the_list(self):
		with patch.dict(frappe.conf, {"auto_prepared_report_exclude": ["Some Very Slow Report"]}):
			self.assertTrue(_report("Some Very Slow Report").is_auto_prepared_report_disabled())
			self.assertFalse(_report("Balance Sheet").is_auto_prepared_report_disabled())
		with patch.dict(frappe.conf, {"auto_prepared_report_exclude": "*"}):
			self.assertTrue(_report("Some Very Slow Report").is_auto_prepared_report_disabled())
		with patch.dict(frappe.conf, {"auto_prepared_report_exclude": []}):
			self.assertFalse(_report("Balance Sheet").is_auto_prepared_report_disabled())
