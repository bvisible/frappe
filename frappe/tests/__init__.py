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


#//// Neoffice — forward-compat shim for the pending frappe upstream upgrade.
#//// Current frappe v15 exposes IntegrationTestCase from frappe.tests; this
#//// (older) fork only ships FrappeTestCase, and in frappe.tests.utils. Fleet
#//// apps written against current frappe do `from frappe.tests import
#//// IntegrationTestCase` and died at test collection ("cannot import name
#//// 'IntegrationTestCase'"). Resolve the modern name to our FrappeTestCase —
#//// upstream they are the same integration base. Lazy, so no import-time cycle
#//// with frappe.tests.utils. DROP THIS once the fork is merged up to the frappe
#//// that defines it natively (only IntegrationTestCase is aliased: nothing in
#//// the fleet imports UnitTestCase yet).
def __getattr__(name):
    if name == "IntegrationTestCase":
        from frappe.tests.utils import FrappeTestCase
        return FrappeTestCase
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
