# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
from frappe import _

no_cache = 1


def get_context(context):
	context.no_breadcrumbs = True
	context.parents = [{"name": "me", "title": _("My Account")}]
	# //// Neoffice — set the page title explicitly. Without it the website theme
	# //// derives the <h1> from the route name, so a brand-new user landing from
	# //// the welcome e-mail was greeted with "Update Password" in English on an
	# //// otherwise French page — the very first screen they ever see. The
	# //// <title> tag was already translated through the template's title block;
	# //// only the on-page heading was not.
	context.title = _("Reset Password")
