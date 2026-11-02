# //// Neoffice — added file (no upstream equivalent): the tool every app's tests act through (roles.py).
import frappe
from frappe.tests.roles import (
	ADMINISTRATOR,
	PERSONAS,
	Persona,
	as_role,
	make_persona,
	prove_red,
	rights_used,
	without_right,
)
from frappe.tests.utils import FrappeTestCase

ROLE = "_Test Role Tool"


class TestRoles(FrappeTestCase):
	def setUp(self):
		frappe.set_user(ADMINISTRATOR)
		if not frappe.db.exists("Role", ROLE):
			frappe.get_doc({"doctype": "Role", "role_name": ROLE, "desk_access": 1}).insert(
				ignore_permissions=True
			)
		PERSONAS["_tool"] = Persona((ROLE,))
		self.addCleanup(PERSONAS.pop, "_tool")

	def test_every_persona_stays_away_from_system_manager(self):
		for name, persona in PERSONAS.items():
			self.assertNotIn("System Manager", persona.roles, name)

	def test_as_role_gives_back_whoever_was_acting(self):
		with as_role("_tool") as user:
			self.assertEqual(frappe.session.user, user)
			self.assertIn(ROLE, frappe.get_roles(user))
			self.assertNotIn("System Manager", frappe.get_roles(user))
		self.assertEqual(frappe.session.user, ADMINISTRATOR)

	def test_as_role_gives_back_after_an_exception(self):
		with self.assertRaises(RuntimeError), as_role("_tool"):
			raise RuntimeError
		self.assertEqual(frappe.session.user, ADMINISTRATOR)

	def test_administrator_is_not_a_role(self):
		with self.assertRaises(ValueError), as_role(ADMINISTRATOR):
			pass

	def test_an_account_with_system_manager_is_refused(self):
		email = "role-test-holds-system-manager@yopmail.com"
		if not frappe.db.exists("User", email):
			user = frappe.new_doc("User")
			user.update({"email": email, "first_name": "holds", "send_welcome_email": 0})
			user.append("roles", {"role": "System Manager"})
			user.insert(ignore_permissions=True)
		with self.assertRaises(ValueError), as_role(email):
			pass

	def test_a_site_without_the_job_cannot_play_the_persona(self):
		PERSONAS["_absent"] = Persona(("_Test No Such Role",))
		self.addCleanup(PERSONAS.pop, "_absent")
		with self.assertRaises(ValueError):
			make_persona("_absent")

	def test_a_persona_holds_exactly_its_roles_again(self):
		email = make_persona("_tool")
		user = frappe.get_doc("User", email)
		user.append("roles", {"role": "System Manager"})
		user.save(ignore_permissions=True)
		make_persona("_tool")
		self.assertEqual([r.role for r in frappe.get_doc("User", email).roles], [ROLE])

	def test_without_right_refuses_a_real_user_and_only_that_right(self):
		with as_role("_tool") as user:
			frappe.permissions.add_permission("ToDo", ROLE)
			frappe.clear_cache(doctype="ToDo")
			frappe.set_user(user)
			self.assertTrue(frappe.has_permission("ToDo", "read", user=user))
			with without_right("ToDo", "read"):
				self.assertFalse(frappe.has_permission("ToDo", "read", user=user))
				self.assertTrue(frappe.has_permission("ToDo", "read", user=ADMINISTRATOR))
			self.assertTrue(frappe.has_permission("ToDo", "read", user=user))

	def test_rights_used_collects_what_a_real_user_held(self):
		frappe.permissions.add_permission("ToDo", ROLE)
		frappe.clear_cache(doctype="ToDo")
		with rights_used() as used, as_role("_tool") as user:
			frappe.has_permission("ToDo", "read", user=user)
		self.assertIn(("ToDo", "read"), used)

	def test_prove_red_is_refused_on_a_site_not_made_for_tests(self):
		allow_tests = frappe.conf.get("allow_tests")
		frappe.conf.allow_tests = 0
		self.addCleanup(frappe.conf.__setitem__, "allow_tests", allow_tests)
		with self.assertRaises(frappe.ValidationError):
			prove_red("frappe.tests.test_roles")
