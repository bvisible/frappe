# //// Neoffice — added file (no upstream equivalent). A Website Profile in maintenance closes the showcase website, not
# //// the pages a customer is sent to nor the apps of the staff (neoffice-maintenance#2171).
"""What a website in maintenance still serves, and to whom."""

import frappe
from frappe.tests.test_api import make_request
from frappe.tests.utils import FrappeTestCase, patch_hooks
from frappe.utils import get_test_client
from frappe.website.path_resolver import PathResolver, customer_service_route

OFFLINE = "site-offline"
STAFF = "_test_maintenance_staff@example.com"
CUSTOMER = "_test_maintenance_customer@example.com"


def enter_maintenance():
	"""Stands for neoffice_theme's profile resolver (a before_request hook): the host's site is in maintenance."""
	frappe.local.website_profile_doc = frappe._dict(name="_Test Site In Maintenance", maintenance_mode=1)


def resolved(path):
	return PathResolver(path).resolve()[0]


class MaintenanceCase(FrappeTestCase):
	def setUp(self):
		enter_maintenance()
		self.addCleanup(self._leave_maintenance)
		frappe.set_user("Guest")
		self.addCleanup(frappe.set_user, "Administrator")

	def _leave_maintenance(self):
		frappe.local.website_profile_doc = None

	def assertClosed(self, path):
		self.assertEqual(resolved(path), OFFLINE, f"/{path} should show the maintenance page")

	def assertOpen(self, path):
		self.assertNotEqual(resolved(path), OFFLINE, f"/{path} should stay open")


class TestTheShowcaseCloses(MaintenanceCase):
	def test_the_showcase_is_closed_to_a_visitor(self):
		for path in ("", "about", "contact", "_test-opener"):
			with self.subTest(path=path):
				self.assertClosed(path)

	def test_the_infrastructure_stays_open(self):
		for path in ("login", "app", "app/todo", "api/method/ping", "assets/frappe/x.js", "update-password"):
			with self.subTest(path=path):
				self.assertOpen(path)

	def test_a_site_out_of_maintenance_serves_everything(self):
		frappe.local.website_profile_doc = frappe._dict(name="_Test Site Online", maintenance_mode=0)
		self.assertOpen("about")


class TestWhatAnAppDeclaresStaysOpen(MaintenanceCase):
	def test_a_route_an_app_declares_stays_open_with_what_follows_it(self):
		with patch_hooks({"website_maintenance_open_routes": ["_test-open", "/_test-slashed/"]}):
			for path in ("_test-open", "_test-open/abc", "_test-slashed", "_test-slashed/x/y"):
				with self.subTest(path=path):
					self.assertOpen(path)
			# a route opens itself and what follows it, never a longer name that starts like it
			self.assertClosed("_test-opener")
			self.assertClosed("")

	def test_frappe_declares_the_pages_it_sends_a_customer_to(self):
		for path in (
			"me",
			"printview",
			"message",
			"unsubscribe",
			"update-profile",
			"confirm_workflow_action",
			"qrcode",
		):
			with self.subTest(path=path):
				self.assertOpen(path)

	def test_an_app_mounted_by_a_route_rule_stays_open(self):
		rules = [
			{"from_route": "/_test-app/<path:app_path>", "to_route": "_test_app"},
			{"from_route": "/_test-page/<name>", "to_route": "_test_page"},
		]
		with patch_hooks({"website_route_rules": rules}):
			self.assertOpen("_test-app")
			self.assertOpen("_test-app/tickets/12")
			# a page reached through a rule is not an app: it stays behind the maintenance page
			self.assertClosed("_test-page/x")
			# an app is not one of the customer's service pages, which webshop's own veil opens to visitors
			self.assertFalse(customer_service_route("_test-app/tickets/12"))

	def test_the_customer_portal_stays_open(self):
		with patch_hooks({"standard_portal_menu_items": [{"title": "Test", "route": "/_test-portal"}]}):
			self.assertOpen("_test-portal")
			self.assertOpen("_test-portal/SO-0001")
			self.assertTrue(customer_service_route("/_test-portal/SO-0001"))


class TestWhoSeesThrough(MaintenanceCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		staff = frappe.get_doc(
			{"doctype": "User", "email": STAFF, "first_name": "Staff", "send_welcome_email": 0}
		)
		staff.append("roles", {"role": "Blogger"})
		staff.insert(ignore_permissions=True)
		frappe.get_doc(
			{
				"doctype": "User",
				"email": CUSTOMER,
				"first_name": "Customer",
				"user_type": "Website User",
				"send_welcome_email": 0,
			}
		).insert(ignore_permissions=True)

	def test_a_member_of_staff_sees_the_site_behind_its_maintenance_page(self):
		# a desk user without System Manager: in Neoffice only Administrator holds it, so the old check locked
		# every employee out of the apps and pages of an instance whose site is in maintenance
		self.assertEqual(frappe.get_cached_value("User", STAFF, "user_type"), "System User")
		frappe.set_user(STAFF)
		self.assertNotIn("System Manager", frappe.get_roles())
		self.assertOpen("about")
		self.assertOpen("")

	def test_a_signed_in_customer_still_meets_the_maintenance_page_on_the_showcase(self):
		frappe.set_user(CUSTOMER)
		self.assertClosed("about")
		self.assertOpen("me")


class TestThroughTheRealRequest(FrappeTestCase):
	"""The same answers through the request path, in another thread, as a visitor's browser gets them."""

	def _get(self, path):
		with patch_hooks({"before_request": [f"{__name__}.enter_maintenance"]}):
			return make_request(target=get_test_client().get, args=(path,), site=frappe.local.site)

	def test_a_page_frappe_declares_answers_a_visitor(self):
		self.assertEqual(self._get("/message").status_code, 200)

	def test_the_same_page_undeclared_shows_the_maintenance_page(self):
		with patch_hooks({"website_maintenance_open_routes": []}):
			self.assertNotEqual(self._get("/message").status_code, 200)

	def test_the_home_still_shows_the_maintenance_page(self):
		self.assertNotEqual(self._get("/").status_code, 200)
