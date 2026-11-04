# //// Neoffice — added file (no upstream equivalent). The page the link of a sign-in mail opens
# //// (frappe/passwordless.py). Opening it changes nothing: a mail scanner may open every link before the person
# //// does, so only the button below signs in.
from urllib.parse import quote, urlencode

import frappe
from frappe import _
from frappe.passwordless import APP_LINK, link_state
from frappe.utils import get_request_host

no_cache = True
sitemap = 0


def get_context(context):
	token = frappe.form_dict.get("token")
	state = link_state(token)
	context.status = state["status"]
	context.token = token if state["status"] == "ready" else ""
	# The mobile app asked: the button reopens it with the link's token and this site's host. The token alone signs
	# nobody in, the app adds the secret only it holds (`passwordless.app_sign_in`).
	context.app_link = ""
	if state["status"] == "app":
		request = getattr(frappe.local, "request", None)
		query = urlencode(
			{"token": token, "site": get_request_host(request) if request else ""}, quote_via=quote
		)
		context.app_link = f"{APP_LINK}?{query}"
	context.title = _("Sign in")
	context.no_breadcrumbs = True
	context.metatags = {"robots": "noindex, nofollow"}
	return context
