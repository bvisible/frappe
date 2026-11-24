# //// Neoffice — added file (no upstream equivalent). Guards the place of the theme's cockpit and sheets in the desk's page
# //// (www/app.py with_first, 24.11.2026, neoffice-maintenance#3262).
"""What an app declares in app_include_*_first stands right after Frappe's own files, before any other app's."""

from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.www.app import with_first


class TestFirstIncludes(FrappeTestCase):
	def test_between_frappe_and_the_other_apps(self):
		hooks = {"app_include_css_first": ["/assets/neoffice_theme/css/cockpit.css"]}
		with patch.object(frappe, "get_hooks", return_value=["desk.bundle.css", "report.bundle.css"]) as own:
			got = with_first(["desk.bundle.css", "report.bundle.css", "erpnext.bundle.css"], "app_include_css", hooks)
		own.assert_called_once_with("app_include_css", app_name="frappe")
		self.assertEqual(
			got,
			[
				"desk.bundle.css",
				"report.bundle.css",
				"/assets/neoffice_theme/css/cockpit.css",
				"erpnext.bundle.css",
			],
		)

	def test_without_the_hook_the_list_is_as_it_was(self):
		self.assertEqual(with_first(["a", "b"], "app_include_js", {}), ["a", "b"])

	def test_the_desk_still_has_its_own_files_first(self):
		own = frappe.get_hooks("app_include_js", app_name="frappe")
		self.assertIn("desk.bundle.js", own)
		every = frappe.get_hooks("app_include_js")
		self.assertEqual(every[: len(own)], own)
