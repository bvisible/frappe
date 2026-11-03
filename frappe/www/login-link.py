# //// Neoffice — added file (no upstream equivalent). The page the link of a sign-in mail opens
# //// (frappe/passwordless.py). Opening it changes nothing: a mail scanner may open every link before the person
# //// does, so only the button below signs in.
import frappe
from frappe import _
from frappe.passwordless import link_state

no_cache = True
sitemap = 0


def get_context(context):
	token = frappe.form_dict.get("token")
	state = link_state(token)
	context.status = state["status"]
	context.token = token if state["status"] == "ready" else ""
	context.title = _("Sign in")
	context.no_breadcrumbs = True
	context.metatags = {"robots": "noindex, nofollow"}
	return context
