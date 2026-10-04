# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import frappe
from frappe import _


def get(name):
	"""
	Return the :term:`doclist` of the `Page` specified by `name`
	"""
	page = frappe.get_doc("Page", name)
	if page.is_permitted():
		page.load_assets()
		docs = frappe._dict(page.as_dict())
		if getattr(page, "_dynamic_page", None):
			docs["_dynamic_page"] = 1

		return docs
	else:
		frappe.response["403"] = 1
		# //// Neoffice — upstream raises an untranslated English sentence, shown as is to a
		# //// French user without read access to a page; we translate the template, then inject the title.
		raise frappe.PermissionError(_("No read permission for Page {0}").format(page.title or name))


@frappe.whitelist(allow_guest=True)
def getpage(name: str):
	"""
	Load the page from `frappe.form` and send it via `frappe.response`
	"""

	doc = get(name)
	frappe.response.docs.append(doc)
