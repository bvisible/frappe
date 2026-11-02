# //// Neoffice — added file (no upstream equivalent): the people a test acts as, for every app of the fleet.
"""A test acts as the person who acts in real life, never as Administrator (neoffice-maintenance#2125).

Administrator is our account: no client creates an order or an invoice with it, and no permission check ever refuses
it. A Frappe test runs as Administrator unless it says otherwise, so a green test can say nothing about a real user.
A customer of the portal could no longer pay (#2114) through thousands of green tests for that reason alone.

Administrator may prepare the scenery. The act under test is played by whoever plays it in real life:

	with as_role("sales_user"):
		order.insert()

	with as_role("portal_customer", customer=customer.name):
		pay_the_order(order.name)

The personas are the roles a real account holds: the bundles of neoffice_theme/access_model.py, by the workspace a job
gives. Never System Manager: on our instances it is Administrator's alone.

It lives here because every app of the fleet has frappe, in its CI, on the clone and on an instance, and only here.

A test that cannot fail proves nothing. `prove_red` runs a test, then runs it again with each right it used taken
away, and says which right turns it red. Nothing is written: the right is masked where every check reads it.

	bench --site prodclone.local execute frappe.tests.roles.prove_red --kwargs "{'module': 'webshop.tests.test_x'}"
"""

import io
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from unittest.mock import patch

import frappe
import frappe.permissions

ADMINISTRATOR = "Administrator"
FORBIDDEN_ROLE = "System Manager"
USER_IDENTITY_FIELD = (
	"user_identity"  # set by neoffice_theme: the client's own administrator is "admin", one per site
)
# Tried first by prove_red: what a broken right costs most when it is one of these.
BUSINESS_DOCTYPES = (
	"Payment Request",
	"Payment Entry",
	"Sales Invoice",
	"POS Invoice",
	"Sales Order",
	"Quotation",
	"Delivery Note",
	"Stock Entry",
	"Purchase Invoice",
	"Purchase Order",
	"Purchase Receipt",
	"Journal Entry",
)
RIGHTS_TRIED = ("submit", "create", "write", "read", "cancel")


@dataclass(frozen=True)
class Persona:
	roles: tuple[str, ...]  # the first one is the job: a site without it cannot play this persona
	identity: str | None = None


# Administrateur·rice holds every right but System Manager (access_model.ADMIN_TEMPLATE); these are the ones a test of a
# business document meets. Admin alone opens no document, it is the Setup workspace's role.
_CLIENT_ADMIN_ROLES = (
	"Admin",
	"Accounts Manager",
	"Accounts User",
	"Sales Manager",
	"Sales Master Manager",
	"Sales User",
	"Billing",
	"Stock Manager",
	"Stock User",
	"Item Manager",
	"Purchase Manager",
	"Purchase Master Manager",
	"Purchase User",
	"Nexus POS Manager",
	"POSNext Cashier",
	"Website Manager",
)

PERSONAS = {
	"portal_customer": Persona(("Customer",)),
	"cashier": Persona(("POSNext Cashier",)),  # Retail, level User
	"till_manager": Persona(("Nexus POS Manager", "POSNext Cashier", "Retail")),  # Retail, level Manager
	"accountant": Persona(("Accounts User",)),  # Accounting, level User
	"head_accountant": Persona(("Accounts Manager", "Accounts User")),  # Accounting, level Manager
	"sales_user": Persona(("Sales User", "Billing")),  # Selling, level User
	"sales_manager": Persona(
		("Sales Manager", "Sales Master Manager", "Sales User", "Billing")
	),  # level Manager
	"warehouse": Persona(("Stock User", "Item Manager")),  # Stock, level User
	"stock_manager": Persona(
		("Stock Manager", "Stock User", "Item Manager", "Purchase User")
	),  # level Manager
	"purchase_user": Persona(("Purchase User",)),  # Buying, level User
	"client_admin": Persona(_CLIENT_ADMIN_ROLES, identity="admin"),
}


def make_persona(persona: str, email: str | None = None, customer: str | None = None) -> str:
	"""The account of a persona, created when it is missing and always holding exactly its roles.

	`customer` ties it to a Customer through a Contact, which is how a portal account finds its party."""
	wanted = PERSONAS[persona]
	if FORBIDDEN_ROLE in wanted.roles:
		raise ValueError(f"A persona never holds {FORBIDDEN_ROLE}: it is Administrator's alone")
	if not frappe.db.exists("Role", wanted.roles[0]):
		raise ValueError(f"This site has no role {wanted.roles[0]}: it cannot play the persona {persona}")

	email = email or f"role-test-{persona.replace('_', '-')}@yopmail.com"
	previous = frappe.session.user
	frappe.set_user(ADMINISTRATOR)
	try:
		roles = [role for role in wanted.roles if frappe.db.exists("Role", role)]
		_set_account(email, persona, roles)
		if customer:
			_tie_to_customer(email, customer)
		if wanted.identity:
			_take_identity(email, wanted.identity)
	finally:
		frappe.set_user(previous)
	return email


def _set_account(email: str, persona: str, roles: list[str]) -> None:
	if frappe.db.exists("User", email):
		user = frappe.get_doc("User", email)
		if user.enabled and sorted(row.role for row in user.roles) == sorted(roles):
			return
		user.enabled = 1
	else:
		user = frappe.new_doc("User")
		user.update({"email": email, "first_name": persona.replace("_", " "), "send_welcome_email": 0})
	user.set("roles", [{"role": role} for role in roles])
	user.save(ignore_permissions=True)
	frappe.clear_cache(user=email)


def _tie_to_customer(email: str, customer: str) -> None:
	contact = frappe.db.get_value("Contact Email", {"email_id": email, "parenttype": "Contact"}, "parent")
	if contact:
		doc = frappe.get_doc("Contact", contact)
		if not any(l.link_doctype == "Customer" and l.link_name == customer for l in doc.links):
			doc.append("links", {"link_doctype": "Customer", "link_name": customer})
			doc.save(ignore_permissions=True)
		return
	doc = frappe.new_doc("Contact")
	doc.first_name = email.split("@")[0]
	doc.append("email_ids", {"email_id": email, "is_primary": 1})
	doc.append("links", {"link_doctype": "Customer", "link_name": customer})
	doc.insert(ignore_permissions=True)


def _take_identity(email: str, identity: str) -> None:
	"""One account per site holds it: it moves to this one, in the test's transaction."""
	if frappe.get_meta("User").get_field(USER_IDENTITY_FIELD):
		frappe.db.set_value("User", {USER_IDENTITY_FIELD: identity}, USER_IDENTITY_FIELD, None)
		frappe.db.set_value("User", email, USER_IDENTITY_FIELD, identity)


@contextmanager
def as_role(persona: str, email: str | None = None, customer: str | None = None) -> Iterator[str]:
	"""Act as a persona of PERSONAS, or as an account the test made. Gives back whoever was acting before.

	Usable as a decorator of a test, too."""
	if persona == ADMINISTRATOR:
		raise ValueError("Administrator is not a role anyone plays: see frappe/tests/roles.py")
	user = make_persona(persona, email, customer) if persona in PERSONAS else persona
	if persona not in PERSONAS and FORBIDDEN_ROLE in frappe.get_roles(user):
		raise ValueError(f"{user} holds {FORBIDDEN_ROLE}: on our instances only Administrator does")
	previous = frappe.session.user
	frappe.set_user(user)
	try:
		yield user
	finally:
		frappe.set_user(previous)


# ---------------------------------------------------------------------------- a right taken away, a test seen red


def _doctype_name(meta) -> str:
	return meta if isinstance(meta, str) else meta.name


def _is_real_user(user: str | None) -> bool:
	return (user or frappe.session.user) != ADMINISTRATOR


@contextmanager
def without_right(doctype: str, ptype: str) -> Iterator[None]:
	"""Nobody but Administrator holds this right while it lasts. Nothing is written: a run that dies leaves no trace."""
	original = frappe.permissions.get_role_permissions

	def masked(doctype_meta, user=None, is_owner=None, debug=False):
		perms = original(doctype_meta, user=user, is_owner=is_owner, debug=debug)
		if _doctype_name(doctype_meta) != doctype or not _is_real_user(user):
			return perms
		perms = frappe._dict(perms)
		perms[ptype] = 0
		perms["if_owner"] = {
			key: value for key, value in (perms.get("if_owner") or {}).items() if key != ptype
		}
		return perms

	with patch.object(frappe.permissions, "get_role_permissions", masked):
		frappe.local.role_permissions = {}
		yield
	frappe.local.role_permissions = {}


@contextmanager
def rights_used() -> Iterator[set[tuple[str, str]]]:
	"""Collects the (doctype, right) a real user held when a check read them. The check reads the whole row of the
	doctype, so it is wider than what the test needs; prove_red finds out which ones count."""
	original = frappe.permissions.get_role_permissions
	used: set[tuple[str, str]] = set()

	def traced(doctype_meta, user=None, is_owner=None, debug=False):
		perms = original(doctype_meta, user=user, is_owner=is_owner, debug=debug)
		if _is_real_user(user):
			name = _doctype_name(doctype_meta)
			used.update((name, ptype) for ptype in RIGHTS_TRIED if perms.get(ptype))
		return perms

	with patch.object(frappe.permissions, "get_role_permissions", traced):
		yield used


def _rank(right: tuple[str, str]) -> tuple[int, int, str]:
	doctype, ptype = right
	rank = BUSINESS_DOCTYPES.index(doctype) if doctype in BUSINESS_DOCTYPES else len(BUSINESS_DOCTYPES)
	return rank, RIGHTS_TRIED.index(ptype), doctype


def _cases(suite) -> Iterator[unittest.TestCase]:
	for item in suite:
		if isinstance(item, unittest.TestSuite):
			yield from _cases(item)
		else:
			yield item


def _run(case: unittest.TestCase) -> bool:
	fresh = type(case)(case._testMethodName)
	result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(unittest.TestSuite([fresh]))
	frappe.set_user(ADMINISTRATOR)
	frappe.db.rollback()
	return result.wasSuccessful() and not result.skipped


def _refuse_a_live_site() -> None:
	if frappe.local.site == "prod.local" or not frappe.conf.get("allow_tests"):
		frappe.throw(
			"prove_red runs tests: only on a site made for them (prodclone.local), never on a live one"
		)


def prove_red(module: str, test: str | None = None, stop_at_first: bool = True) -> dict:
	"""Run the tests of a module, then each again without each right it used. A test counts when one right turns it red.

	`test` keeps the tests whose name contains it. `stop_at_first` stops at the first right that turns a test red.
	Rights are tried in order of what a broken one costs: the business documents first, submit before read."""
	_refuse_a_live_site()
	frappe.flags.in_test = True
	frappe.set_user(ADMINISTRATOR)
	report = {"counted": [], "not_green": [], "never_red": []}
	for case in _cases(unittest.TestLoader().loadTestsFromName(module)):
		name = case.id().rsplit(".", 2)[-2:]
		name = ".".join(name)
		if test and test not in name:
			continue
		with rights_used() as used:
			green = _run(case)
		if not green:
			report["not_green"].append(name)
			continue
		red_on = []
		for doctype, ptype in sorted(used, key=_rank):
			with without_right(doctype, ptype):
				if not _run(case):
					red_on.append(f"{doctype}:{ptype}")
					if stop_at_first:
						break
		if red_on:
			report["counted"].append({"test": name, "red_without": red_on})
		else:
			report["never_red"].append({"test": name, "rights_tried": len(used)})
	return report
