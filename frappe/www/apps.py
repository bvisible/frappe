# Copyright (c) 2023, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt

import frappe
from frappe import _


#//// Neoffice — upstream: `from frappe.apps import get_apps` at the top of this file, and
#//// `all_apps = get_apps()` here. 0634af137c (2025-11-13 "Enhance workspace and app
#//// customization logic") dropped the import and routes the page through the local
#//// get_app_data() below, so /apps shows the same, App-Customization-aware list as the desk
#//// app switcher. frappe.apps.get_apps() still exists and is still used elsewhere.
#//// Companion: www/apps.html, which reads the renamed keys.
def get_context():
	# Use the same logic as boot.py for app data generation
	all_apps = get_app_data()

	system_default_app = frappe.get_system_settings("default_app")
	user_default_app = frappe.db.get_value("User", frappe.session.user, "default_app")
	default_app = user_default_app if user_default_app else system_default_app

	if len(all_apps) == 0:
		frappe.local.flags.redirect_location = "/app"
		raise frappe.Redirect

	for app in all_apps:
		#//// Neoffice — upstream: `app.get("name")`. Key renamed with the payload change above
		#//// (0634af137c).
		app["is_default"] = True if app.get("app_name") == default_app else False

	return {"apps": all_apps}


#//// Neoffice — added, no upstream equivalent (0634af137c, 2025-11-13 "Enhance workspace and
#//// app customization logic", 9 files): builds the app list the way boot.py does, honouring
#//// the Neoffice `App Customization` doctype when it exists and has rows, and filtering the
#//// result down to apps that are actually installed — plus the "virtual" apps that App
#//// Customization declares and that have no python package behind them. Upstream's
#//// frappe.apps.get_apps() knows none of this.
#//// TO REVIEW: table_exists("App Customization") is checked twice per call, and a workspace
#//// permission scan (get_workspace_sidebar_items) now runs on every /apps hit.
def get_app_data():
	"""Get app data using the same logic as boot.py with App Customization support"""
	from frappe.boot import generate_app_data_from_customization, generate_app_data_default
	from frappe.desk.desktop import get_workspace_sidebar_items

	# Get allowed workspaces for current user
	allowed_pages = get_workspace_sidebar_items().get("pages", [])
	allowed_page_names = [d.name for d in allowed_pages]

	# Check if App Customization is available and has entries
	if frappe.db.table_exists("App Customization") and frappe.db.count("App Customization") > 0:
		app_data = generate_app_data_from_customization(allowed_page_names)
	else:
		app_data = generate_app_data_default(allowed_page_names)

	# Filter out invalid apps (apps with names that aren't installed and aren't virtual)
	installed_apps = frappe.get_installed_apps()
	validated_app_data = []

	for app in app_data:
		app_name = app.get("app_name")

		# Check if this is a valid app name (must be lowercase, no spaces, no special chars except underscore/dash)
		# or if it's a virtual app check if it has the is_virtual marker in App Customization
		if app_name in installed_apps:
			validated_app_data.append(app)
		else:
			# Check if it's a virtual app
			if frappe.db.table_exists("App Customization"):
				is_virtual = frappe.db.get_value("App Customization", {"app_name": app_name, "enabled": 1}, "is_virtual")
				if is_virtual:
					validated_app_data.append(app)

	return validated_app_data
