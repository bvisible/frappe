# //// Neoffice — added file (no upstream equivalent). A sign-in in the request of a browser whose session has expired keeps
# //// its cookie (frappe/auth.py, LoginManager.post_login).
"""A link that signs a person in works in a browser that still holds the cookie of an expired session."""

from werkzeug.wrappers import Response

import frappe
from frappe.auth import CookieManager, LoginManager
from frappe.tests.utils import FrappeTestCase
from frappe.utils import set_request

PERSON = "sign-in-over-an-expired-session@yopmail.com"


class TestASignInOverAnExpiredSession(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if not frappe.db.exists("User", PERSON):
			person = frappe.get_doc(
				doctype="User", email=PERSON, first_name="Expired", user_type="Website User", send_welcome_email=0
			)
			person.insert(ignore_permissions=True)
		frappe.db.commit()  # nosemgrep — a session start commits

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		frappe.db.delete("Sessions", {"user": PERSON})
		frappe.delete_doc("User", PERSON, force=True)
		frappe.db.commit()  # nosemgrep
		super().tearDownClass()

	def setUp(self):
		self.kept = {
			key: getattr(frappe.local, key)
			for key in ("request", "cookie_manager", "login_manager", "session", "session_obj", "request_ip")
			if hasattr(frappe.local, key)
		}

	def tearDown(self):
		for key in ("request", "cookie_manager", "login_manager", "session_obj"):
			if hasattr(frappe.local, key):
				delattr(frappe.local, key)
		for key, value in self.kept.items():
			setattr(frappe.local, key, value)
		frappe.set_user("Administrator")

	def request_of_an_expired_session(self):
		set_request(method="GET", path="/api/method/ping", headers={"Cookie": "sid=a-session-long-gone"})
		frappe.local.request_ip = "127.0.0.1"
		frappe.local.cookie_manager = CookieManager()
		frappe.local.login_manager = LoginManager()

	def sid_cookies(self):
		response = Response()
		frappe.local.cookie_manager.flush_cookies(response)
		return [c.split(";")[0] for c in response.headers.getlist("Set-Cookie") if c.startswith("sid=")]

	def test_the_expired_session_alone_still_loses_its_cookie(self):
		self.request_of_an_expired_session()
		self.assertEqual(frappe.session.user, "Guest")
		self.assertEqual(self.sid_cookies()[-1], "sid=")

	def test_a_sign_in_in_the_same_request_keeps_its_cookie(self):
		self.request_of_an_expired_session()
		frappe.local.login_manager.login_as(PERSON)
		self.assertEqual(frappe.session.user, PERSON)
		self.assertEqual(self.sid_cookies(), [f"sid={frappe.session.sid}"])

	def test_a_sign_out_after_it_still_loses_its_cookie(self):
		self.request_of_an_expired_session()
		frappe.local.login_manager.login_as(PERSON)
		frappe.local.login_manager.logout()
		self.assertEqual(self.sid_cookies()[-1], "sid=")
