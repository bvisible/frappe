import frappe


def update_system_settings(args, commit=False):
	doc = frappe.get_doc("System Settings")
	doc.update(args)
	doc.flags.ignore_mandatory = 1
	doc.save()
	if commit:
		frappe.db.commit()


def get_system_setting(key):
	return frappe.db.get_single_value("System Settings", key)


global_test_dependencies = ["User"]


#//// Neoffice — v16 test base-class names, resolved to our v15 FrappeTestCase.
#//// Fleet forks and apps written against frappe v16 docs do
#//// `from frappe.tests import IntegrationTestCase` (telephony, Letters) or
#//// `from frappe.tests import UnitTestCase` (crm, builder, helpdesk, lms) and died at
#//// test collection ("cannot import name ..."). Upstream v15 — measured on 15.120 by
#//// the weekly upstream-preview CI, 2026-09-03 — does NOT export them either: they are
#//// v16 names. So this shim is not a forward-compat stopgap to drop at the v15 upgrade:
#//// it stays for the whole v15 line and goes only when the fleet runs v16, where both
#//// classes exist natively (UnitTestCase there is a lighter, DB-less base; mapping it to
#//// the integration base is a superset, fine for collection and execution). Lazy, so no
#//// import-time cycle with frappe.tests.utils.
def __getattr__(name):
    if name in ("IntegrationTestCase", "UnitTestCase"):
        from frappe.tests.utils import FrappeTestCase
        return FrappeTestCase
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
