from urllib.parse import quote_plus

import frappe
from frappe import _
from frappe.utils import cstr
from frappe.website.page_renderers.template_page import TemplatePage


class NotPermittedPage(TemplatePage):
	def __init__(self, path=None, http_status_code=None, exception=""):
		frappe.local.message = cstr(exception)
		super().__init__(path=path, http_status_code=http_status_code)
		self.http_status_code = 403

	def can_render(self):
		return True

	def render(self):
		action = f"/login?redirect-to={quote_plus(frappe.request.path)}"
		if frappe.request.path.startswith("/app/") or frappe.request.path == "/app":
			action = "/login"
		# //// Neoffice — a 403 with a LIVE session must offer a real login.
		# //// Upstream's button points at /login, but /login redirects any
		# //// signed-in user straight back — so a Website User who hit /app
		# //// (no desk access) was caught in a loop: Not Permitted → Login →
		# //// redirected → Not Permitted, with no way to switch account.
		# //// ?relogin=1 (handled in www/login.py) clears the session first,
		# //// then shows the form.
		if frappe.session.user != "Guest":
			action += ("&" if "?" in action else "?") + "relogin=1"
		frappe.local.message_title = _("Not Permitted")
		frappe.local.response["context"] = dict(
			indicator_color="red", primary_action=action, primary_label=_("Login"), fullpage=True
		)
		self.set_standard_path("message")
		return super().render()
