# //// Neoffice — added file (no upstream equivalent). The access check of the form dialog
# //// (frappe/public/js/frappe/form/form_dialog.js): a click on a link to another document opens
# //// that document's form in a dialog, and the client asks here first whether the reader may open
# //// it. Without this check a refused form would call frappe.show_not_permitted, which replaces the
# //// page behind the dialog.
import frappe


@frappe.whitelist()
def get_access(doctype: str, name: str) -> dict:
	"""Whether the reader may open this document in a form dialog.

	Answers ``{"status": ...}``, never an exception for a refusal, so that no error dialog stacks
	on the page:

	- ``ok``: open it;
	- ``excluded``: not in a dialog (hook ``link_dialog_exclude``, singles, child tables, virtual
	  doctypes) — the link navigates as before;
	- ``not_found``: no such document;
	- ``forbidden``: the reader may not read it (roles, user permissions, owner rules,
	  ``has_permission`` hooks).

	The doctype-level permission is checked before existence, so that a reader without access to a
	doctype learns nothing about its records.
	"""
	if not doctype or not name or not frappe.db.exists("DocType", doctype):
		return {"status": "not_found"}
	if doctype in (frappe.get_hooks("link_dialog_exclude") or []):
		return {"status": "excluded"}
	meta = frappe.get_meta(doctype)
	if meta.issingle or meta.istable or meta.is_virtual:
		return {"status": "excluded"}
	if not frappe.has_permission(doctype, "read"):
		return {"status": "forbidden"}
	if not frappe.db.exists(doctype, name):
		return {"status": "not_found"}
	if not frappe.has_permission(doctype, "read", doc=name):
		return {"status": "forbidden"}
	return {"status": "ok"}


def get_boot_settings() -> dict:
	"""What the desk needs at boot: the switch (site_config ``link_dialog``: 0 turns the dialog
	off on a site) and the doctypes whose links keep navigating."""
	return {
		"enabled": frappe.utils.cint(frappe.conf.get("link_dialog", 1)),
		"exclude": frappe.get_hooks("link_dialog_exclude") or [],
	}
