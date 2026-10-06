# //// Neoffice — added file (no upstream equivalent). The `sid` cookie lives as long as the session it
# //// carries, on every request (frappe/auth.py `_session_max_age`), not only on the one that stamped it.
"""A session stamped with its own expiry keeps it in the browser too."""

import unittest

import frappe
from frappe.auth import CookieManager
from frappe.sessions import get_expiry_in_seconds


class TestTheSessionCookieLivesAsLongAsItsSession(unittest.TestCase):
	"""`init_cookies` runs on every request: the max-age it writes is what the browser keeps."""

	def setUp(self):
		self._session = getattr(frappe.local, "session", None)

	def tearDown(self):
		frappe.local.session = self._session

	def _max_age(self, data):
		frappe.local.session = frappe._dict({"sid": "a-test-sid", "user": "someone@example.com", "data": frappe._dict(data)})
		manager = CookieManager()
		manager.init_cookies()
		return manager.cookies["sid"]["max_age"]

	def test_a_remembered_session_keeps_its_month_on_every_request(self):
		self.assertEqual(self._max_age({"session_expiry": "720:00:00"}), 720 * 3600)

	def test_a_session_stamped_with_the_sites_expiry_keeps_it(self):
		self.assertEqual(self._max_age({"session_expiry": "06:00:00"}), 6 * 3600)

	def test_a_session_without_a_stamp_keeps_the_sites_expiry(self):
		self.assertEqual(self._max_age({}), get_expiry_in_seconds())

	def test_an_unreadable_stamp_falls_back_to_the_sites_expiry(self):
		self.assertEqual(self._max_age({"session_expiry": "soon"}), get_expiry_in_seconds())

	def test_a_guest_gets_no_cookie_from_here_without_a_sid(self):
		frappe.local.session = frappe._dict({"sid": None, "user": "Guest", "data": frappe._dict()})
		manager = CookieManager()
		manager.init_cookies()
		self.assertNotIn("sid", manager.cookies)
