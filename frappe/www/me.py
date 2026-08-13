# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import frappe
import frappe.www.list
from frappe import _

no_cache = 1


def get_context(context):
	if frappe.session.user == "Guest":
		#//// Neoffice — send guests to the login form instead of throwing.
		#//// Upstream answers /me with a 403 "You need to be logged in", which is a
		#//// dead end: the page every portal sidebar links to as "Mon compte" told
		#//// signed-out visitors they had no access rather than offering to sign
		#//// them in. Every other portal page here already redirects this way.
		frappe.local.flags.redirect_location = "/login?redirect-to=/me"
		raise frappe.Redirect

	context.current_user = frappe.get_doc("User", frappe.session.user)
	context.show_sidebar = True
	#//// Neoffice — render as a clean portal page: keep the left portal menu but
	#//// drop the website breadcrumb (the "Home / Shop / …" trail Webshop injects).
	context.no_breadcrumbs = 1
