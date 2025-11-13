# Copyright (c) 2023, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt

import frappe
from frappe import _


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
		app["is_default"] = True if app.get("app_name") == default_app else False

	return {"apps": all_apps}


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
