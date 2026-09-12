# //// Neoffice — added file (no upstream equivalent). Guards #364: the app list must never
# //// offer a desk (/app) route to a user who has no desk, and must not change for anyone else.
import frappe
from frappe.apps import get_default_path, without_desk_routes_for_portal_users
from frappe.tests.utils import FrappeTestCase

PORTAL_USER = "apps-portal-filter-test@yopmail.com"
APPS = [
	{"name": "erpnext", "route": "/app/selling"},
	{"name": "erpnext", "route": "/app"},
	{"name": "lms", "route": "/lms"},
	{"name": "helpdesk", "route": "/apps"},
]


class TestAppsPortalFilter(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if not frappe.db.exists("User", PORTAL_USER):
			user = frappe.get_doc(
				{
					"doctype": "User",
					"email": PORTAL_USER,
					"first_name": "Portal",
					"user_type": "Website User",
					"send_welcome_email": 0,
				}
			)
			user.flags.no_welcome_mail = True
			user.insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")

	def test_system_user_keeps_every_route(self):
		frappe.set_user("Administrator")
		self.assertEqual(without_desk_routes_for_portal_users(APPS), APPS)

	def test_portal_user_loses_desk_routes_only(self):
		frappe.set_user(PORTAL_USER)
		kept = [app["route"] for app in without_desk_routes_for_portal_users(APPS)]
		self.assertEqual(kept, ["/lms", "/apps"])

	def test_route_key_is_configurable(self):
		frappe.set_user(PORTAL_USER)
		apps = [{"app_route": "/app/stock"}, {"app_route": "/helpdesk"}]
		self.assertEqual(
			without_desk_routes_for_portal_users(apps, route_key="app_route"), [{"app_route": "/helpdesk"}]
		)

	def test_default_path_never_sends_a_portal_user_to_the_desk(self):
		frappe.set_user(PORTAL_USER)
		desk_only = without_desk_routes_for_portal_users([{"name": "erpnext", "route": "/app/selling"}])
		self.assertNotEqual(get_default_path(desk_only), "/app")
