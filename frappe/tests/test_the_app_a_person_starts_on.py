# //// Neoffice — added file (no upstream equivalent)
"""The application a person starts on is found though our menu has no row for it (neoffice-maintenance#3357).

Our list of applications is « App Customization », a menu of desk workspaces. A sales representative of the hub is given
the CRM to start on (`User.default_app`): the CRM has no row in the menu, so they signed in to /app."""

import importlib.util
from types import ModuleType
from unittest.mock import patch

import frappe
from frappe import apps
from frappe.tests.roles import as_role
from frappe.tests.utils import FrappeTestCase

MENU = [{"name": "selling", "route": "/app/selling"}, {"name": "accounting", "route": "/app/accounting"}]


class TestTheAppAPersonStartsOn(FrappeTestCase):
	def starts_on(self, default_app, screen, installed=("frappe", "crm"), in_the_menu=False, hidden=()):
		real_hooks, real_value, real_exists = frappe.get_hooks, frappe.db.get_value, frappe.db.exists
		real_table = frappe.db.table_exists

		def hooks(hook=None, *args, **kwargs):
			if hook == "add_to_apps_screen":
				return screen
			return real_hooks(hook, *args, **kwargs)

		def value(*args, **kwargs):
			if args[:1] == ("User",) and args[2:3] == ("default_app",):
				return default_app
			return real_value(*args, **kwargs)

		def exists(*args, **kwargs):
			if args[:1] == ("App Customization",):
				return "row" if in_the_menu else None
			return real_exists(*args, **kwargs)

		def table(doctype, *args, **kwargs):
			return doctype == "App Customization" or real_table(doctype, *args, **kwargs)

		modules = {"neoffice_theme.app_visibility": _Gate(hidden)}
		if importlib.util.find_spec("neoffice_theme") is None:
			modules["neoffice_theme"] = ModuleType("neoffice_theme")
		with (
			patch.object(frappe, "get_hooks", side_effect=hooks),
			patch.object(frappe.db, "get_value", side_effect=value),
			patch.object(frappe.db, "exists", side_effect=exists),
			patch.object(frappe.db, "table_exists", side_effect=table),
			patch.object(frappe, "get_installed_apps", return_value=list(installed)),
			patch.dict("sys.modules", modules),
		):
			return apps.get_default_path(list(MENU))

	def test_an_app_with_a_page_of_its_own_and_no_row_in_the_menu(self):
		with as_role("sales_user"):
			self.assertEqual(self.starts_on("crm", [{"name": "crm", "route": "/crm"}]), "/crm")

	def test_an_app_of_the_menu_is_taken_from_the_menu(self):
		with as_role("sales_user"):
			self.assertEqual(
				self.starts_on("selling", [{"name": "selling", "route": "/elsewhere"}]), "/app/selling"
			)

	def test_an_app_the_person_may_not_open_is_not_where_they_start(self):
		screen = [
			{
				"name": "crm",
				"route": "/crm",
				"has_permission": "frappe.tests.test_the_app_a_person_starts_on.refused",
			}
		]
		with as_role("sales_user"):
			self.assertIsNone(self.starts_on("crm", screen))
			# It has a row in the menu, and the menu does not show it to this person
			self.assertIsNone(self.starts_on("crm", [{"name": "crm", "route": "/crm"}], in_the_menu=True))
			self.assertIsNone(self.starts_on("crm", [{"name": "crm", "route": "/crm"}], hidden=["crm"]))
			self.assertIsNone(self.starts_on("crm", [{"name": "crm", "route": "/crm"}], installed=["frappe"]))

	def test_a_desk_route_stays_the_menus(self):
		with as_role("sales_user"):
			self.assertIsNone(self.starts_on("crm", [{"name": "crm", "route": "/app/frappe-crm"}]))

	def test_a_portal_account_is_never_sent_to_an_app_of_the_staff(self):
		with as_role("portal_customer"):
			self.assertIsNone(self.starts_on("crm", [{"name": "crm", "route": "/crm"}]))


def refused():
	return False


class _Gate:
	"""neoffice_theme.app_visibility, as far as this reads it."""

	def __init__(self, hidden):
		self.hidden = set(hidden)

	def gated_customizations_to_hide(self):
		return self.hidden
