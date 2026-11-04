# //// Neoffice — added file (no upstream equivalent).
# ////
# //// neoffice-maintenance#2192: signing in with the code mailed to the person is the default way in, on every site,
# //// the shop's dialog, the desk's /login and the mobile app alike (decision of 2026-11-04). The two switches are
# //// ticked by default on a NEW site (System Settings' field defaults), but a site that already exists never gets a
# //// default written for a field added after its settings were first saved: it reads the field as 0. This ticks them
# //// once. The password is never taken away: every screen keeps it, and a site with no working outgoing mail account
# //// is not offered the code at all (`passwordless.can_send_mail`), so the switch alone cannot lock anybody out. A
# //// merchant who wants it off unticks it afterwards: this runs once.
import frappe


def execute():
	for field in ("login_with_email_link", "login_code_system_users"):
		frappe.db.set_single_value("System Settings", field, 1)
