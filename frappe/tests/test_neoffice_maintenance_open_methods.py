# //// Neoffice — added file (no upstream equivalent). A site in maintenance keeps serving the methods it names in
# //// `maintenance_open_methods`: the hub's relays stay up while its desk is closed for an update (neoffice-maintenance#1785).
"""What a site in maintenance still answers, and what it refuses."""

from unittest.mock import patch

import frappe
from frappe.app import is_open_during_maintenance
from frappe.installer import update_site_config
from frappe.tests.test_api import make_request
from frappe.tests.utils import FrappeTestCase
from frappe.utils import get_test_client

OPEN = "frappe.ping"
CLOSED = "frappe.auth.get_logged_user"


def _request(path):
	return frappe._dict(path=path)


class TestOnlyTheNamedMethodsAreOpen(FrappeTestCase):
	def test_a_named_method_is_open_under_every_api_prefix(self):
		with patch.dict(frappe.local.conf, {"maintenance_open_methods": [OPEN]}):
			for path in (
				f"/api/method/{OPEN}",
				f"/api/v1/method/{OPEN}",
				f"/api/v2/method/{OPEN}",
				f"/api/method/{OPEN}/",
			):
				with self.subTest(path=path):
					self.assertTrue(is_open_during_maintenance(_request(path)))

	def test_anything_else_stays_closed(self):
		with patch.dict(frappe.local.conf, {"maintenance_open_methods": [OPEN]}):
			for path in (
				f"/api/method/{CLOSED}",
				f"/api/method/{OPEN}x",
				f"/app/{OPEN}",
				"/",
				f"/api/resource/{OPEN}",
			):
				with self.subTest(path=path):
					self.assertFalse(is_open_during_maintenance(_request(path)))

	def test_a_site_that_names_nothing_keeps_everything_closed(self):
		with patch.dict(frappe.local.conf, {"maintenance_open_methods": None}):
			self.assertFalse(is_open_during_maintenance(_request(f"/api/method/{OPEN}")))

	def test_a_single_method_written_as_a_string_is_read_as_one_method(self):
		with patch.dict(frappe.local.conf, {"maintenance_open_methods": OPEN}):
			self.assertTrue(is_open_during_maintenance(_request(f"/api/method/{OPEN}")))
			self.assertFalse(is_open_during_maintenance(_request("/api/method/frappe")))


class TestASiteInMaintenanceServesOnlyItsOpenMethods(FrappeTestCase):
	"""Through the real request path: the site config on disk, read by init_request in another thread."""

	def setUp(self):
		self.client = get_test_client()
		self.addCleanup(self._leave_maintenance)

	def _leave_maintenance(self):
		for key in ("maintenance_mode", "maintenance_open_methods"):
			update_site_config(key, "None")
			# update_site_config leaves the string "None" in this process's config: a truthy maintenance_mode
			frappe.local.conf.pop(key, None)

	def _get(self, method):
		return make_request(target=self.client.get, args=(f"/api/method/{method}",), site=frappe.local.site)

	def test_the_open_method_answers_and_the_rest_is_refused(self):
		update_site_config("maintenance_open_methods", [OPEN])
		update_site_config("maintenance_mode", 1)

		opened = self._get(OPEN)
		self.assertEqual(opened.status_code, 200)
		self.assertEqual(opened.json.get("message"), "pong")
		self.assertEqual(self._get(CLOSED).status_code, 503)

	def test_a_cmd_among_the_arguments_does_not_ride_on_an_open_path(self):
		# application() runs `cmd` before it looks at the path: the open method's path must not carry another method
		update_site_config("maintenance_open_methods", [OPEN])
		update_site_config("maintenance_mode", 1)

		for target, args, kwargs in (
			(self.client.get, (f"/api/method/{OPEN}?cmd={CLOSED}",), {}),
			(self.client.post, (f"/api/method/{OPEN}",), {"data": {"cmd": CLOSED}}),
			(self.client.post, (f"/api/method/{OPEN}",), {"json": {"cmd": CLOSED}}),
		):
			with self.subTest(target=target.__name__, kwargs=kwargs):
				answer = make_request(target=target, args=args, kwargs=kwargs, site=frappe.local.site)
				self.assertEqual(answer.status_code, 503)

	def test_without_open_methods_maintenance_refuses_everything(self):
		update_site_config("maintenance_mode", 1)

		self.assertEqual(self._get(OPEN).status_code, 503)

	def test_out_of_maintenance_the_list_changes_nothing(self):
		update_site_config("maintenance_open_methods", [OPEN])

		self.assertEqual(self._get(OPEN).status_code, 200)
		# a guest is refused this one by its permissions (403), not by a maintenance (503)
		self.assertNotEqual(self._get(CLOSED).status_code, 503)
