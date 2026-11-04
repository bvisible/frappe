# //// Neoffice — added file (no upstream equivalent). Tests of frappe/passwordless.py: signing in with the code
# //// (and the link) mailed to the person. Every test uses an address and a "connection" of its own: the counters
# //// live in Redis and outlive a run.
import importlib.util
import os
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import frappe
from frappe import passwordless
from frappe.tests.utils import FrappeTestCase

LANDING_PATH = "login-link"


def _address(tag):
	return f"pwl-{tag}-{uuid.uuid4().hex[:8]}@example.com"


def _make_user(address, user_type="Website User", enabled=1):
	user = frappe.get_doc(
		{
			"doctype": "User",
			"email": address,
			"first_name": "Pwl",
			"user_type": user_type,
			"enabled": enabled,
			"send_welcome_email": 0,
		}
	)
	user.flags.ignore_permissions = True
	user.flags.ignore_password_policy = True
	user.insert()
	if user_type == "System User":
		# frappe turns a user with no desk role back into a Website User at validation
		user.add_roles("System Manager")
		assert frappe.db.get_value("User", address, "user_type") == "System User"
	return user


class TestPasswordless(FrappeTestCase):
	def setUp(self):
		self.peer = f"203.0.113.{uuid.uuid4().int % 250 + 1}-{uuid.uuid4().hex[:6]}"
		self.mails = []

		def fake_sendmail(**kwargs):
			self.mails.append(kwargs)

		self.real_peer_ip = passwordless._peer_ip
		self.real_signup_is_open = passwordless.signup_is_open
		self.cookie_spy = patch.object(
			passwordless, "_set_nonce_cookie", wraps=passwordless._set_nonce_cookie
		)
		patches = [
			self.cookie_spy,
			patch.object(passwordless, "is_enabled", return_value=True),
			patch.object(passwordless, "system_users_allowed", return_value=False),
			patch.object(passwordless, "signup_is_open", return_value=False),
			patch.object(passwordless, "_peer_ip", side_effect=lambda: self.peer),
			patch("frappe.sendmail", side_effect=fake_sendmail),
		]
		self.cookie_spy_mock = None
		for p in patches:
			started = p.start()
			if p is self.cookie_spy:
				self.cookie_spy_mock = started
			self.addCleanup(p.stop)
		frappe.local.response = frappe._dict()

		self.manager = MagicMock()
		frappe.local.login_manager = self.manager
		self.addCleanup(lambda: delattr(frappe.local, "login_manager"))

	# ----------------------------------------------------------------------------------- helpers

	def ask(self, address, **kwargs):
		return passwordless.request_code(address, **kwargs)

	def code_for(self, address):
		"""The code of the last mail to `address`, the way the person reads it."""
		sent = [m for m in self.mails if m["recipients"] == [address]]
		self.assertTrue(sent, f"no mail was sent to {address}")
		return sent[-1]["args"]["code"]

	def link_token_for(self, address):
		sent = [m for m in self.mails if m["recipients"] == [address]]
		return sent[-1]["args"]["link"].split("token=")[1]

	def challenge(self, address):
		key = passwordless._key("challenge", passwordless._mac("address", address))
		return frappe.cache.get_value(key, expires=True)

	def skip_cooldown(self, address):
		passwordless._forget_counter("cooldown", passwordless._mac("address", address))

	def with_cookie(self, nonce):
		frappe.local.request = SimpleNamespace(
			cookies={passwordless.NONCE_COOKIE: nonce}, host="shop.example", headers={}, scheme="https"
		)
		self.addCleanup(lambda: hasattr(frappe.local, "request") and delattr(frappe.local, "request"))

	# ----------------------------------------------------------------------------------- the code

	def test_the_code_is_six_digits_and_never_kept_in_clear(self):
		address = _address("clear")
		_make_user(address)
		self.assertTrue(self.ask(address)["ok"])

		code = self.code_for(address)
		self.assertRegex(code, r"^\d{6}$")
		stored = repr(self.challenge(address))
		self.assertNotIn(code, stored)
		self.assertNotIn(self.link_token_for(address), stored)

	def test_every_address_gets_the_same_answer(self):
		known, unknown, closed = _address("known"), _address("unknown"), _address("closed")
		_make_user(known)
		with patch.object(passwordless, "signup_is_open", return_value=True):
			answers = [self.ask(known), self.ask(unknown)]
		answers.append(self.ask(closed))  # sign-up closed: no code, same answer

		self.assertEqual(answers[0], answers[1])
		self.assertEqual(answers[1], answers[2])
		self.assertEqual({tuple(m["recipients"]) for m in self.mails}, {(known,), (unknown,)})

	def test_the_nonce_cookie_is_set_for_every_address_but_not_during_the_cooldown(self):
		known, closed = _address("cookie"), _address("cookieclosed")
		_make_user(known)
		manager = MagicMock()
		frappe.local.cookie_manager = manager
		self.addCleanup(lambda: delattr(frappe.local, "cookie_manager"))

		self.ask(known)
		self.ask(closed)
		self.assertEqual(manager.set_cookie.call_count, 2)

		self.ask(known)  # inside the cooldown: the first code's cookie must stay valid
		self.assertEqual(manager.set_cookie.call_count, 2)
		self.assertEqual(len(self.mails), 1)

	def test_a_right_code_signs_in_once(self):
		address = _address("once")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)

		result = passwordless.verify_code(address, code)
		self.assertEqual((result["ok"], result["status"], result["user"]), (True, "signed_in", address))
		self.manager.login_as.assert_called_once_with(address)

		again = passwordless.verify_code(address, code)
		self.assertFalse(again["ok"])
		self.manager.login_as.assert_called_once()

	def test_spaces_in_the_code_are_fine(self):
		address = _address("spaces")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)
		self.assertTrue(passwordless.verify_code(address, f"{code[:3]} {code[3:]}")["ok"])

	def test_five_wrong_codes_lock_even_the_right_one(self):
		address = _address("lock")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)
		wrong = "000000" if code != "000000" else "111111"

		self.assertEqual(
			passwordless.MAX_ATTEMPTS, 5
		)  # the contract; the loop must not follow a changed constant
		reasons = [passwordless.verify_code(address, wrong)["reason"] for _ in range(5)]
		self.assertEqual(set(reasons), {"invalid"})
		self.assertEqual(passwordless.verify_code(address, wrong)["reason"], "locked")
		self.assertEqual(passwordless.verify_code(address, code)["reason"], "locked")
		self.manager.login_as.assert_not_called()

	def test_an_address_nobody_holds_locks_at_the_same_attempt(self):
		address = _address("nobody")
		reasons = [passwordless.verify_code(address, "123456")["reason"] for _ in range(6)]
		self.assertEqual(reasons, ["invalid"] * 5 + ["locked"])

	def test_a_new_request_kills_the_old_code(self):
		address = _address("replace")
		_make_user(address)
		self.ask(address)
		first = self.code_for(address)
		self.skip_cooldown(address)
		self.ask(address)
		second = self.code_for(address)

		self.assertNotEqual(first, second)
		self.assertFalse(passwordless.verify_code(address, first)["ok"])
		self.assertTrue(passwordless.verify_code(address, second)["ok"])

	def test_a_fresh_code_is_not_locked_by_the_attempts_made_on_the_old_one(self):
		address = _address("fresh")
		_make_user(address)
		self.ask(address)
		wrong = "000000" if self.code_for(address) != "000000" else "111111"
		for _ in range(6):
			passwordless.verify_code(address, wrong)  # the address is now locked
		self.assertEqual(passwordless.verify_code(address, wrong)["reason"], "locked")

		self.skip_cooldown(address)
		self.ask(address)
		self.assertTrue(passwordless.verify_code(address, self.code_for(address))["ok"])

	def test_someone_who_signs_in_and_out_again_at_once_is_sent_a_new_code(self):
		address = _address("again")
		_make_user(address)
		self.ask(address)
		self.assertTrue(passwordless.verify_code(address, self.code_for(address))["ok"])

		self.ask(address)  # no skip_cooldown: the sign-in cleared it
		self.assertEqual(len(self.mails), 2)
		# the hourly cap is untouched by the reset
		for _ in range(passwordless.SENDS_PER_HOUR):
			self.skip_cooldown(address)
			try:
				self.ask(address)
			except frappe.TooManyRequestsError:
				break
		else:
			self.fail("the hourly cap on mails to one address was lost")

	def test_two_parallel_right_answers_sign_in_once(self):
		address = _address("race")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)

		with patch.object(passwordless, "_take", return_value=False):  # the other call deleted the code first
			self.assertFalse(passwordless.verify_code(address, code)["ok"])
		self.manager.login_as.assert_not_called()

	def test_malformed_input_is_refused_before_it_costs_an_attempt(self):
		address = _address("malformed")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)
		for bad in ("12345", "1234567", "abcdef", "", None, 123456):
			self.assertEqual(passwordless.verify_code(address, bad), {"ok": False, "reason": "invalid"})
		self.assertEqual(
			passwordless.request_code("not an address"), {"ok": False, "reason": "invalid_address"}
		)
		self.assertTrue(passwordless.verify_code(address, code)["ok"])  # the attempts are intact

	def test_the_expiry_ends_the_code(self):
		address = _address("expiry")
		_make_user(address)
		self.ask(address)
		code = self.code_for(address)
		frappe.cache.delete_value(passwordless._key("challenge", passwordless._mac("address", address)))
		self.assertFalse(passwordless.verify_code(address, code)["ok"])

	# ----------------------------------------------------------------------------------- who may sign in

	def test_the_switch_off_sends_nothing(self):
		address = _address("off")
		_make_user(address)
		with patch.object(passwordless, "is_enabled", return_value=False):
			self.assertEqual(self.ask(address), {"ok": False, "reason": "disabled"})
		self.assertEqual(self.mails, [])

	def test_staff_only_where_the_setting_allows_it(self):
		staff = _address("staff")
		_make_user(staff, user_type="System User")
		self.ask(staff)
		self.assertEqual(self.mails, [])

		with patch.object(passwordless, "system_users_allowed", return_value=True):
			self.skip_cooldown(staff)
			self.ask(staff)
			self.assertTrue(passwordless.verify_code(staff, self.code_for(staff))["ok"])

	def test_administrator_guest_and_the_disabled_never(self):
		disabled = _address("disabled")
		_make_user(disabled, enabled=0)
		with patch.object(passwordless, "system_users_allowed", return_value=True):
			for address in ("administrator", "Administrator", "guest", disabled):
				self.ask(address)
		self.assertEqual(self.mails, [])
		self.assertIsNone(passwordless.signable_user("Administrator"))
		self.assertIsNone(passwordless.signable_user("Guest"))

	def test_a_user_who_must_pass_two_factor_authentication_uses_the_password(self):
		address = _address("twofa")
		_make_user(address)
		with patch("frappe.twofactor.should_run_2fa", return_value=True):
			self.ask(address)
			self.assertIsNone(passwordless.signable_user(address))
		self.assertEqual(self.mails, [])

	def test_a_sign_in_the_site_refuses_is_refused_without_a_message(self):
		address = _address("refused")
		_make_user(address)
		self.ask(address)
		self.manager.login_as.side_effect = frappe.PermissionError("not on this site")
		self.assertEqual(
			passwordless.verify_code(address, self.code_for(address)), {"ok": False, "reason": "not_allowed"}
		)

	# ----------------------------------------------------------------------------------- an address with no account

	def test_a_new_address_proves_its_mailbox_and_the_app_creates_the_account(self):
		address = _address("new")
		with patch.object(passwordless, "signup_is_open", return_value=True):
			self.ask(address)
			result = passwordless.verify_code(address, self.code_for(address))
		self.assertEqual((result["ok"], result["status"]), (True, "unknown_address"))
		self.manager.login_as.assert_not_called()

		self.assertIsNone(passwordless.consume_proof("not-a-proof"))
		self.assertIsNone(passwordless.consume_proof(None))
		self.assertEqual(passwordless.consume_proof(result["proof"]), address)
		self.assertIsNone(passwordless.consume_proof(result["proof"]))  # once

	def test_a_new_address_where_sign_up_is_closed_gets_no_code(self):
		address = _address("closedsignup")
		self.ask(address)
		self.assertEqual(self.mails, [])
		self.assertFalse(passwordless.verify_code(address, "123456")["ok"])

	def test_an_app_can_close_sign_up_for_its_site(self):
		"""The shop closes it on a site for business accounts only, through the `passwordless_signup_closed` hook."""
		with patch("frappe.website.utils.is_signup_disabled", return_value=0):
			with patch("frappe.get_hooks", return_value=[]):
				self.assertTrue(self.real_signup_is_open())
			with (
				patch("frappe.get_hooks", return_value=["some.app.closed"]),
				patch("frappe.get_attr", return_value=lambda: True),
			):
				self.assertFalse(self.real_signup_is_open())
			with (
				patch("frappe.get_hooks", return_value=["some.app.open"]),
				patch("frappe.get_attr", return_value=lambda: False),
			):
				self.assertTrue(self.real_signup_is_open())
		with (
			patch("frappe.website.utils.is_signup_disabled", return_value=1),
			patch("frappe.get_hooks", return_value=[]),
		):
			self.assertFalse(self.real_signup_is_open())

	def test_an_app_that_finishes_the_sign_in_gets_the_same_safe_redirect(self):
		address = _address("finish")
		_make_user(address)
		for hostile in ("//evil.example", "https://evil.example/x", "javascript:alert(1)"):
			result = passwordless.sign_in(address, hostile)
			self.assertTrue(result["redirect_to"].endswith("/me"), hostile)
		self.assertEqual(passwordless.sign_in(address, "/cart")["redirect_to"], "/cart")

	def test_a_known_but_barred_address_is_never_offered_a_new_account(self):
		address = _address("barred")
		_make_user(address, user_type="System User")  # staff, not allowed
		with patch.object(passwordless, "signup_is_open", return_value=True):
			self.ask(address)
		self.assertEqual(self.mails, [])

	# ----------------------------------------------------------------------------------- where it goes next

	def test_the_redirect_stays_on_the_site(self):
		cases = {
			"/checkout": "/checkout",
			"/cart?x=1": "/cart?x=1",
			"//evil.example/x": None,
			"///evil.example": None,  # a browser reads it as //evil.example
			"////evil.example": None,
			"evil.example/x": None,  # relative: it would resolve under whatever page the browser is on
			"checkout": None,
			"?next=/cart": None,
			"/\\evil.example": None,
			"https://evil.example/x": None,
			"javascript:alert(1)": None,
			"/a\nb": None,
			"x" * 600: None,
			"": None,
			None: None,
			42: None,
		}
		for given, expected in cases.items():
			self.assertEqual(passwordless._safe_redirect(given), expected, repr(given)[:40])

	def test_an_absolute_address_of_this_site_becomes_its_path(self):
		frappe.local.request = SimpleNamespace(host="shop.example", cookies={})
		self.addCleanup(lambda: hasattr(frappe.local, "request") and delattr(frappe.local, "request"))
		with patch.object(passwordless, "get_request_host", return_value="shop.example"):
			self.assertEqual(passwordless._safe_redirect("https://shop.example/cart?a=1#top"), "/cart?a=1")
			self.assertIsNone(passwordless._safe_redirect("https://evil.example/cart"))
			self.assertIsNone(passwordless._safe_redirect("ftp://shop.example/cart"))

	def test_the_redirect_rides_with_the_challenge(self):
		address = _address("redirect")
		_make_user(address)
		self.ask(address, redirect_to="/checkout")
		self.assertEqual(self.challenge(address)["redirect_to"], "/checkout")
		self.assertEqual(
			passwordless.verify_code(address, self.code_for(address))["redirect_to"], "/checkout"
		)

	def test_a_customer_lands_on_the_account_and_staff_on_the_desk(self):
		customer, staff = _address("landcustomer"), _address("landstaff")
		_make_user(customer)
		_make_user(staff, user_type="System User")
		self.assertTrue(passwordless._landing(customer, None).endswith("/me"))
		self.assertIn("/app", passwordless._landing(staff, None))
		self.assertEqual(passwordless._landing(customer, "/cart"), "/cart")
		# a customer is never sent to the desk, which has nothing for them; staff keep the page they asked for
		self.assertTrue(passwordless._landing(customer, "/app/home").endswith("/me"))
		self.assertTrue(passwordless._landing(customer, "/app").endswith("/me"))
		self.assertEqual(passwordless._landing(staff, "/app/todo"), "/app/todo")

	# ----------------------------------------------------------------------------------- the link

	def test_opening_the_link_changes_nothing_and_only_the_button_signs_in(self):
		address = _address("link")
		_make_user(address)
		self.ask(address)
		token = self.link_token_for(address)

		# a scanner: no cookie. It may open the page as often as it likes.
		for _ in range(3):
			self.assertEqual(passwordless.link_state(token)["status"], "other_device")
		self.manager.login_as.assert_not_called()

		# the browser that asked: the cookie carries the nonce the request set
		self.skip_cooldown(address)
		self.ask(address)
		nonce = self.cookie_spy_mock.call_args.args[0]
		token = self.link_token_for(address)
		self.with_cookie(nonce)
		self.assertEqual(passwordless.link_state(token)["status"], "ready")

		passwordless.sign_in_with_link(token)
		self.manager.login_as.assert_called_once_with(address)
		self.assertEqual(frappe.local.response["type"], "redirect")
		self.assertEqual(passwordless.link_state(token)["status"], "invalid")  # once

	def test_a_link_posted_without_the_askers_cookie_signs_nobody_in(self):
		address = _address("login-csrf")
		_make_user(address)
		self.ask(address)
		token = self.link_token_for(address)
		self.with_cookie("somebody-elses-nonce")

		passwordless.sign_in_with_link(token)
		self.assertEqual(frappe.local.response.get("http_status_code"), 403)
		self.manager.login_as.assert_not_called()

	def test_a_replaced_link_and_a_link_for_a_new_address_do_not_sign_in(self):
		address = _address("replaced")
		_make_user(address)
		self.ask(address)
		old = self.link_token_for(address)
		self.skip_cooldown(address)
		self.ask(address)
		self.assertEqual(passwordless.link_state(old)["status"], "invalid")
		self.assertEqual(passwordless.link_state("garbage")["status"], "invalid")
		self.assertEqual(passwordless.link_state(None)["status"], "invalid")

		new = _address("newlink")
		with patch.object(passwordless, "signup_is_open", return_value=True):
			self.ask(new)
		# a new address gets no link in its mail at all; the code is the way to open an account
		self.assertFalse([m for m in self.mails if m["recipients"] == [new] and m["args"]["known"]])

	def test_the_landing_page_has_the_controller_frappe_looks_for(self):
		"""Frappe turns the hyphens of a page's name into underscores to find its controller: login-link.html is served
		by login_link.py. A controller named like the page is never loaded and the page renders with no context."""
		www = os.path.join(os.path.dirname(frappe.__file__), "www")
		self.assertTrue(os.path.exists(os.path.join(www, "login-link.html")))
		self.assertTrue(os.path.exists(os.path.join(www, "login_link.py")))
		self.assertFalse(os.path.exists(os.path.join(www, "login-link.py")))
		from frappe.website.page_renderers.template_page import TemplatePage

		page = TemplatePage(LANDING_PATH, 200)
		page.set_pymodule()
		self.assertEqual(page.pymodule_name, "frappe.www.login_link")

	def test_the_landing_page_shows_what_the_link_state_says(self):
		path = os.path.join(os.path.dirname(frappe.__file__), "www", "login_link.py")
		spec = importlib.util.spec_from_file_location("login_link_page", path)
		page = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(page)

		context = frappe._dict()
		frappe.local.form_dict.token = "garbage"
		page.get_context(context)
		self.assertEqual((context.status, context.token), ("invalid", ""))
		self.assertIn("noindex", context.metatags["robots"])

	# ----------------------------------------------------------------------------------- the limits

	def test_one_address_gets_five_mails_an_hour(self):
		address = _address("flood")
		_make_user(address)
		for _ in range(passwordless.SENDS_PER_HOUR):
			self.skip_cooldown(address)
			self.ask(address)
		self.skip_cooldown(address)
		with self.assertRaises(frappe.TooManyRequestsError):
			self.ask(address)
		self.assertEqual(len(self.mails), passwordless.SENDS_PER_HOUR)

	def test_a_second_request_inside_the_cooldown_sends_nothing(self):
		address = _address("cooldown")
		_make_user(address)
		self.ask(address)
		self.ask(address)
		self.assertEqual(len(self.mails), 1)

	def test_one_connection_cannot_ask_for_more_than_its_share(self):
		for i in range(passwordless.REQUESTS_PER_PEER_HOUR):
			self.ask(_address(f"peer{i}"))
		with self.assertRaises(frappe.TooManyRequestsError):
			self.ask(_address("peer-over"))

	def test_the_counted_address_is_the_last_forwarded_entry(self):
		"""nginx's own entry: a caller writes the first ones, so a fresh first entry per request must not reset the count."""
		with patch("frappe.get_request_header", return_value="1.1.1.1, 2.2.2.2, 9.9.9.9"):
			self.assertEqual(self.real_peer_ip(), "9.9.9.9")
		with patch("frappe.get_request_header", return_value="6.6.6.6"):
			self.assertEqual(self.real_peer_ip(), "6.6.6.6")
		with patch("frappe.get_request_header", return_value=None):
			frappe.local.request_ip = "5.5.5.5"
			self.assertEqual(self.real_peer_ip(), "5.5.5.5")

	# ----------------------------------------------------------------------------------- the mail

	def test_the_mail_carries_the_code_the_link_and_the_expiry(self):
		address = _address("mail")
		_make_user(address)
		self.ask(address)

		mail = self.mails[-1]
		args = mail["args"]
		self.assertIn(args["code"], mail["subject"])
		self.assertIn("/login-link?token=", args["link"])
		self.assertEqual(mail["template"], "passwordless_code")
		self.assertTrue(mail["now"])

	def test_the_brand_is_the_servers_never_the_callers(self):
		import inspect

		self.assertNotIn("brand", inspect.signature(passwordless.request_code).parameters)

		real = frappe.get_hooks

		def hooks(name=None, *args, **kwargs):
			return ["some.app.shop_name"] if name == "passwordless_brand" else real(name, *args, **kwargs)

		# the hook, called by itself: patching get_attr for a whole request would also swap the job that sends the mail
		with (
			patch("frappe.get_hooks", side_effect=hooks),
			patch("frappe.get_attr", return_value=lambda: "Chez <b>Martin</b>"),
		):
			self.assertEqual(passwordless._brand(), "Chez bMartin/b")  # markup characters dropped
		self.assertTrue(passwordless._brand())  # no hook answers: the application's own name

		# and the mail carries what the server decided
		address = _address("brand")
		_make_user(address)
		with patch.object(passwordless, "_brand", return_value="Chez Martin"):
			self.ask(address)
		self.assertEqual(self.mails[-1]["args"]["brand"], "Chez Martin")
		self.assertIn("Chez Martin", self.mails[-1]["subject"])

	def test_a_name_set_by_the_asking_app_comes_before_every_hook(self):
		"""The shop's hook comes first in the install order, so a restaurant's page asking through the same engine was mailed
		under the shop's name: the app that asks names the site for its call, and that name wins."""
		real = frappe.get_hooks

		def hooks(name=None, *args, **kwargs):
			return ["some.app.shop_name"] if name == "passwordless_brand" else real(name, *args, **kwargs)

		with (
			patch("frappe.get_hooks", side_effect=hooks),
			patch("frappe.get_attr", return_value=lambda: "The Shop"),
		):
			self.assertEqual(passwordless._brand(), "The Shop")
			try:
				frappe.flags.passwordless_brand = "  Chez <b>Martin</b>  "
				self.assertEqual(passwordless._brand(), "Chez bMartin/b")  # cleaned like a hook's name
			finally:
				frappe.flags.passwordless_brand = None
			# once the call is over, the hooks speak again
			self.assertEqual(passwordless._brand(), "The Shop")
			try:
				frappe.flags.passwordless_brand = "   "
				self.assertEqual(passwordless._brand(), "The Shop")  # a name with nothing in it names nobody
			finally:
				frappe.flags.passwordless_brand = None

		# and it is the one the mail carries
		address = _address("flagbrand")
		_make_user(address)
		try:
			frappe.flags.passwordless_brand = "Chez Martin"
			self.ask(address)
		finally:
			frappe.flags.passwordless_brand = None
		self.assertEqual(self.mails[-1]["args"]["brand"], "Chez Martin")
		self.assertIn("Chez Martin", self.mails[-1]["subject"])

	def test_no_request_parameter_names_the_site(self):
		keys = ("brand", "passwordless_brand")
		address = _address("formbrand")
		_make_user(address)
		try:
			for key in keys:
				frappe.form_dict[key] = "Evil Corp"
			self.ask(address)
		finally:
			for key in keys:
				frappe.form_dict.pop(key, None)
		self.assertNotIn("Evil Corp", self.mails[-1]["subject"])
		self.assertNotEqual(self.mails[-1]["args"]["brand"], "Evil Corp")

	def test_the_template_renders_and_escapes_the_brand(self):
		html = frappe.get_template("templates/emails/passwordless_code.html").render(
			code="123456", link="https://shop.example/login-link?token=t", minutes=10, brand="A&B", known=True
		)
		self.assertIn("123456", html)
		self.assertIn("A&amp;B", html)
		self.assertIn("login-link?token=t", html)

		new_account = frappe.get_template("templates/emails/passwordless_code.html").render(
			code="123456",
			link="https://shop.example/login-link?token=t",
			minutes=10,
			brand="Shop",
			known=False,
		)
		self.assertNotIn("login-link?token", new_account)

	def test_the_mail_writes_the_link_out_to_be_pasted_into_the_browser_that_asked(self):
		"""A mail cannot run a script, so it has no « copy » button: the link is also written out, where a long press selects it. The link signs
		in only in the browser where the code was asked for, and a mail app's own browser is not that one."""
		link = "https://shop.example/login-link?token=t"
		render = frappe.get_template("templates/emails/passwordless_code.html").render
		known = render(code="123456", link=link, minutes=10, brand="Shop", known=True)

		# once as the button's address, once as text
		self.assertEqual(known.count(link), 2)
		self.assertIn(f">{link}</p>", known)
		self.assertIn("paste it into the browser where you asked for the code", known)

		# an address with no account is given no link at all, written out or not
		self.assertNotIn(link, render(code="123456", link=link, minutes=10, brand="Shop", known=False))

	def test_the_page_of_a_link_opened_in_another_browser_can_copy_it(self):
		"""Where the link was opened in the wrong browser (the button is not there), the page offers to copy it, to paste it where the code was asked for."""
		template = frappe.get_template("www/login-link.html")

		def page(status):
			context = template.new_context({"status": status, "token": "t" if status == "ready" else ""})
			return "".join(template.blocks["page_content"](context))

		other_device = page("other_device")
		self.assertIn('id="copy-login-link"', other_device)
		self.assertIn("navigator.clipboard", other_device)
		# a page that is not a secure context has no clipboard API
		self.assertIn("document.execCommand", other_device)
		self.assertIn("Paste it into the browser where you asked for the code.", other_device)

		for status in ("ready", "needs_code", "invalid"):
			self.assertNotIn("copy-login-link", page(status), status)

	def test_a_mail_that_cannot_leave_is_logged_and_never_shown(self):
		address = _address("nomail")
		_make_user(address)
		with (
			patch("frappe.sendmail", side_effect=frappe.OutgoingEmailError("no account")),
			patch("frappe.log_error") as log,
		):
			passwordless.send_code_email(address, "123456", "token", 10, "Shop")
		log.assert_called_once()

	def test_the_native_sender_is_the_code_and_link(self):
		from frappe.www.login import send_login_link

		with (
			patch("frappe.get_system_settings", side_effect=lambda f: 1),
			patch("frappe.passwordless.request_code", return_value={"ok": True}) as request,
		):
			send_login_link("someone@example.com")
		request.assert_called_once_with("someone@example.com")
