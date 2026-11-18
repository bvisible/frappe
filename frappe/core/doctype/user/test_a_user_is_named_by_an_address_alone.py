# //// Neoffice — added file (no upstream equivalent)
# Copyright (c) 2026, Neoffice and contributors
# License: MIT. See LICENSE

"""A new user is named by an address and nothing else (maintenance#2860).

On 18.11 a vulnerability scanner sent a shop's signup addresses followed by SQL and script payloads. Frappe's
validate_email_address finds an address inside a string and returns it, so every one passed and 17 enabled Website
Users were created, named `sample@email.tst' and 2*3*8=6*8 and 'a9is'='a9is`, `sample@email.tst%c0%a7...` or a
<script> tag. A new or renamed user must now be exactly an address; an existing user keeps saving as before.
"""

import frappe
from frappe.tests.utils import FrappeTestCase

PROBES = (
	"zz-probe@email.tst%c0%a7%c0%a2%2527%2522\\'\"",
	"zz-probe@email.tst' and 2*3*8=6*8 and 'a9is'='a9is",
	"zz-probe@email.tstfa6oi2lf') or 701=(select 701 from pg_sleep(15))--",
	"zz-probe@email.tst'\"()&%<zzz><script >qscz(9600)</script>",
	"zz-probe@email.tst&n948154=v924635",
)


def new_user(email):
	return frappe.get_doc(
		{
			"doctype": "User",
			"email": email,
			"first_name": "Zz",
			"last_name": "Probe",
			"user_type": "Website User",
			"send_welcome_email": 0,
		}
	)


class TestAUserIsNamedByAnAddressAlone(FrappeTestCase):
	def test_a_scanner_payload_after_an_address_makes_no_user(self):
		for probe in PROBES:
			with self.subTest(probe=probe):
				# A name with < or > is refused earlier, by the naming rule (NameError); the others reach the address.
				with self.assertRaises((frappe.InvalidEmailAddressError, frappe.NameError)):
					new_user(probe).insert(ignore_permissions=True)
				self.assertFalse(frappe.db.exists("User", probe.lower()))

	def test_a_plain_address_still_makes_a_user(self):
		user = new_user("zz-plain.probe+shop@example.com").insert(ignore_permissions=True)
		self.addCleanup(frappe.delete_doc, "User", user.name, force=True, ignore_permissions=True)
		self.assertEqual(user.name, "zz-plain.probe+shop@example.com")

	def test_the_payload_alone_is_refused_not_only_by_the_naming_rule(self):
		with self.assertRaises(frappe.InvalidEmailAddressError):
			new_user("zz-probe@email.tst' and 2*3*8=6*8 and 'a9is'='a9is").insert(ignore_permissions=True)

	def test_an_existing_user_saves_as_before(self):
		user = new_user("zz-legacy.probe@example.com").insert(ignore_permissions=True)
		self.addCleanup(frappe.delete_doc, "User", user.name, force=True, ignore_permissions=True)
		user.reload()
		user.first_name = "Zy"
		user.save(ignore_permissions=True)
		self.assertEqual(frappe.db.get_value("User", user.name, "first_name"), "Zy")
