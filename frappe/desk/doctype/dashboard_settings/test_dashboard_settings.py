# //// Neoffice — added file (no upstream equivalent): the race of maintenance#828.
# //// Drop it together with the guard in dashboard_settings.py once upstream handles the race.
from unittest.mock import patch

import frappe
from frappe.desk.doctype.dashboard_settings.dashboard_settings import create_dashboard_settings
from frappe.tests.utils import FrappeTestCase


class TestDashboardSettings(FrappeTestCase):
	def test_a_request_that_loses_the_race_gets_the_existing_settings(self):
		user = "Administrator"
		create_dashboard_settings(user)
		self.assertTrue(frappe.db.exists("Dashboard Settings", user))
		frappe.local.message_log = []
		# The other request inserted between our check and our insert.
		with patch.object(frappe.db, "exists", return_value=False):
			settings = create_dashboard_settings(user)
		self.assertEqual(settings.name, user)
		# No "Duplicate Name" left for the page to show.
		self.assertEqual(frappe.local.message_log, [])
