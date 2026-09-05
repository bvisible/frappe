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


# //// Neoffice — v16 test base-class names, resolved to our v15 FrappeTestCase.
# //// Fleet forks and apps written against frappe v16 docs do
# //// `from frappe.tests import IntegrationTestCase` (telephony, Letters) or
# //// `from frappe.tests import UnitTestCase` (crm, builder, helpdesk, lms) and died at
# //// test collection ("cannot import name ..."). Upstream v15 — measured on 15.120 by
# //// the weekly upstream-preview CI, 2026-09-03 — does NOT export them either: they are
# //// v16 names. So this shim is not a forward-compat stopgap to drop at the v15 upgrade:
# //// it stays for the whole v15 line and goes only when the fleet runs v16, where both
# //// classes exist natively (UnitTestCase there is a lighter, DB-less base; mapping it to
# //// the integration base is a superset, fine for collection and execution). Lazy, so no
# //// import-time cycle with frappe.tests.utils.
_V16_TEST_CASE = None


def _v16_test_case():
    """FrappeTestCase plus the v16 conveniences fleet forks call on `self`."""
    global _V16_TEST_CASE
    if _V16_TEST_CASE is None:
        from frappe.tests.utils import FrappeTestCase
        from frappe.tests.utils import change_settings as _change_settings

        class IntegrationTestCase(FrappeTestCase):
            # //// Neoffice — v16 parity (frappe/tests/classes/unit_test_case.py setUpClass):
            # //// the session user is Administrator when a class starts and again when it
            # //// ends. Without it a test that fails after a bare frappe.set_user(...) leaks
            # //// that user — deleted by the rollback — into every class that follows: suite's
            # //// Meet tests took 16 unrelated tests down with "Could not find User".
            @classmethod
            def setUpClass(cls) -> None:
                import frappe

                super().setUpClass()
                frappe.set_user("Administrator")
                cls.addClassCleanup(frappe.set_user, "Administrator")

            # //// Neoffice — v16's IntegrationTestCase exposes change_settings as a METHOD
            # //// (`with self.change_settings("Wiki Settings", {...}):` — wiki, 2026-09-03); v15 ships
            # //// the same context manager as a module-level function. Same signature, delegated.
            @staticmethod
            def change_settings(doctype, settings_dict=None, /, commit=False, **settings):
                return _change_settings(doctype, settings_dict, commit=commit, **settings)

            # //// Neoffice — v16 also exposes set_user as a context manager callable on the class
            # //// (`with cls.set_user(OWNER):` in setUpClass — suite/drive, 2026-09-03); v15's is a
            # //// plain instance method. Restores the previous user on exit.
            class _SetUser:
                def __init__(self, user):
                    import frappe

                    self._frappe = frappe
                    self._user = user
                    self._previous = None

                def __enter__(self):
                    self._previous = self._frappe.session.user
                    self._frappe.set_user(self._user)
                    return self

                def __exit__(self, *exc):
                    self._frappe.set_user(self._previous)
                    return False

            @classmethod
            def set_user(cls, user):
                return cls._SetUser(user)

        _V16_TEST_CASE = IntegrationTestCase
    return _V16_TEST_CASE


def __getattr__(name):
    if name in ("IntegrationTestCase", "UnitTestCase"):
        return _v16_test_case()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
