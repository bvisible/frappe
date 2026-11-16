# //// Neoffice — added file (no upstream equivalent). The request's address is the X-Forwarded-For entry the proxy
# //// in front of frappe wrote, the last one (frappe/auth.py `set_request_ip`, neoffice-maintenance#2296).
"""An address restriction is checked against the address the proxy saw, whatever else the header holds."""

from unittest.mock import patch

import frappe
from frappe.auth import HTTPRequest, validate_ip_address
from frappe.tests.utils import FrappeTestCase
from frappe.utils import set_request

ALLOWED = "203.0.113.7"
ELSEWHERE = "198.51.100.20"
USER = "neoffice-ip-restricted@example.com"


class TestTheRequestAddressIsTheProxysEntry(FrappeTestCase):
	def setUp(self):
		self._request = getattr(frappe.local, "request", None)
		self._request_ip = getattr(frappe.local, "request_ip", None)
		patcher = patch.object(frappe, "_dev_server", 0)  # enterContext needs Python 3.11
		patcher.start()
		self.addCleanup(patcher.stop)
		if not frappe.db.exists("User", USER):
			frappe.get_doc(
				{
					"doctype": "User",
					"email": USER,
					"first_name": "IP restricted",
					"send_welcome_email": 0,
					"restrict_ip": ALLOWED,
				}
			).insert(ignore_permissions=True)

	def tearDown(self):
		frappe.local.request = self._request
		frappe.local.request_ip = self._request_ip
		frappe.delete_doc("User", USER, force=True, ignore_permissions=True)

	def _address(self, forwarded):
		set_request(method="GET", path="/app", headers={"X-Forwarded-For": forwarded})
		HTTPRequest.set_request_ip(None)  # the method reads the request, never `self`
		return frappe.local.request_ip

	def test_behind_one_proxy_that_writes_the_header_the_address_is_unchanged(self):
		# bench's vhost: `X-Forwarded-For $remote_addr`, one entry
		self.assertEqual(self._address(ELSEWHERE), ELSEWHERE)

	def test_behind_a_proxy_that_appends_the_address_is_the_last_entry(self):
		# a website vhost: `$proxy_add_x_forwarded_for`, the header as it arrived, then the connection's address
		self.assertEqual(self._address(f"{ALLOWED}, {ELSEWHERE}"), ELSEWHERE)

	def test_an_allowed_address_in_front_of_the_header_does_not_open_a_restricted_account(self):
		self._address(f"{ALLOWED}, {ELSEWHERE}")
		with self.assertRaises(frappe.AuthenticationError):
			validate_ip_address(USER)

	def test_a_restricted_account_still_signs_in_from_its_address_behind_either_proxy(self):
		self._address(ALLOWED)
		validate_ip_address(USER)
		self._address(f"10.20.30.40, {ALLOWED}")
		validate_ip_address(USER)
