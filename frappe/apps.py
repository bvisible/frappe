# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import re

import frappe
from frappe import _
from frappe.core.doctype.installed_applications.installed_applications import (
	get_apps_with_incomplete_dependencies,
	get_setup_wizard_completed_apps,
	get_setup_wizard_not_required_apps,
)
from frappe.desk.utils import slug


@frappe.whitelist()
def get_apps():
	from frappe.desk.desktop import get_workspace_sidebar_items

	allowed_workspaces = get_workspace_sidebar_items().get("pages")

	# Check if App Customization is available and has entries
	if frappe.db.table_exists("App Customization") and frappe.db.count("App Customization") > 0:
		return get_apps_from_customization(allowed_workspaces)

	# Fallback to default behavior (hooks-based)
	return get_apps_default(allowed_workspaces)


def get_apps_from_customization(allowed_workspaces):
	"""Get apps list using App Customization doctype"""
	app_list = []
	installed_apps = frappe.get_installed_apps()

	# Get enabled apps from App Customization, sorted by sort_order
	customizations = frappe.get_all(
		"App Customization",
		filters={"enabled": 1},
		fields=["app_name", "app_title", "app_logo", "app_route", "sort_order", "is_virtual"],
		order_by="sort_order asc, app_title asc"
	)

	# //// Neoffice — this list is what the app switcher shows, and it obeyed
	# //// neither of our two rules. Measured 29.08.2026 over real HTTP with a
	# //// throwaway account per job: a cashier whose profile grants two
	# //// applications was served all ten, and so was a portal customer. The
	# //// per-user whitelist (User Visible App) and the entitlement gate on the
	# //// applications sold separately (Construction, Fitness) only ran on the
	# //// desk bootinfo, never here. Administrator is never filtered.
	visible_apps = set()
	hidden_apps = set()
	if frappe.session.user != "Administrator":
		try:
			visible_apps = set(
				frappe.get_all(
					"User Visible App",
					filters={"parent": frappe.session.user, "parenttype": "User"},
					pluck="app",
				)
			)
		except Exception:
			visible_apps = set()
		try:
			from neoffice_theme.app_visibility import gated_customizations_to_hide

			hidden_apps = gated_customizations_to_hide()
		except Exception:
			hidden_apps = set()

	for custom in customizations:
		app_name = custom.get("app_name")

		# //// Neoffice — an application the site has not bought is never offered.
		if app_name in hidden_apps:
			continue

		# //// Neoffice — an empty whitelist means "show everything", which is the
		# //// behaviour every account had before profiles existed.
		if visible_apps and app_name not in visible_apps:
			continue

		# Skip if not installed and not virtual
		if app_name not in installed_apps and not custom.get("is_virtual"):
			continue

		# Skip frappe app
		if app_name == "frappe":
			continue

		# Check setup wizard completion for non-virtual apps
		if app_name in installed_apps and not custom.get("is_virtual"):
			if (
				app_name not in get_setup_wizard_completed_apps()
				and app_name not in get_setup_wizard_not_required_apps()
				and "System Manager" not in frappe.get_roles()
			):
				continue

		# Check permission if app has has_permission hook
		skip_app = False
		if app_name in installed_apps:
			app_details = frappe.get_hooks("add_to_apps_screen", app_name=app_name)
			if app_details:
				for detail in app_details:
					has_permission_path = detail.get("has_permission")
					if has_permission_path:
						try:
							if not frappe.get_attr(has_permission_path)():
								skip_app = True
								break
						except Exception:
							frappe.log_error(f"Failed to call has_permission hook ({has_permission_path}) for {app_name}")

		if skip_app:
			continue

		# Resolve app logo URL using neoffice_theme if available
		app_logo = custom.get("app_logo")
		if app_logo and "neoffice_theme" in installed_apps:
			try:
				from neoffice_theme.api import resolve_app_logo_url
				app_logo = resolve_app_logo_url(app_logo) or app_logo
			except Exception:
				pass

		# Fallback to hooks for logo if not set in customization
		if not app_logo and app_name in installed_apps:
			app_details = frappe.get_hooks("add_to_apps_screen", app_name=app_name)
			if app_details:
				app_logo = app_details[0].get("logo")

		# Get route from customization or hooks
		app_route = custom.get("app_route")
		if not app_route and app_name in installed_apps:
			app_details = frappe.get_hooks("add_to_apps_screen", app_name=app_name)
			if app_details:
				app_route = get_route(app_details[0], allowed_workspaces)

		app_list.append({
			"name": app_name,
			"logo": app_logo,
			"title": _(custom.get("app_title") or app_name),
			"route": app_route or "/app",
		})

	return app_list


def get_apps_default(allowed_workspaces):
	"""Original get_apps logic as fallback when App Customization is not configured"""
	apps = frappe.get_installed_apps()
	app_list = []

	for app in apps:
		if (
			app not in get_setup_wizard_completed_apps()
			and app not in get_setup_wizard_not_required_apps()
			and "System Manager" not in frappe.get_roles()
		):
			continue

		if app == "frappe":
			continue
		app_details = frappe.get_hooks("add_to_apps_screen", app_name=app)
		if not len(app_details):
			continue
		for app_detail in app_details:
			try:
				has_permission_path = app_detail.get("has_permission")
				if has_permission_path and not frappe.get_attr(has_permission_path)():
					continue
				app_list.append(
					{
						"name": app,
						"logo": app_detail.get("logo"),
						"title": _(app_detail.get("title")),
						"route": get_route(app_detail, allowed_workspaces),
					}
				)
			except Exception:
				frappe.log_error(f"Failed to call has_permission hook ({has_permission_path}) for {app}")
	return app_list


def get_route(app, allowed_workspaces=None):
	if not allowed_workspaces:
		return "/app"

	route = app.get("route") if app and app.get("route") else "/apps"

	# Check if user has access to default workspace, if not, pick first workspace user has access to
	if route.startswith("/app/"):
		ws = route.split("/")[2]

		for allowed_ws in allowed_workspaces:
			if allowed_ws.get("name").lower() == ws.lower():
				return route

		module_app = frappe.local.module_app
		for allowed_ws in allowed_workspaces:
			module = allowed_ws.get("module")
			if module and module_app.get(module.lower()) == app.get("name"):
				return f"/app/{slug(allowed_ws.name.lower())}"
		return f"/app/{slug(allowed_workspaces[0].get('name').lower())}"
	else:
		return route


def is_desk_apps(apps):
	for app in apps:
		# check if route is /app or /app/* and not /app1 or /app1/*
		pattern = r"^/app(/.*)?$"
		route = app.get("route")
		if route and not re.match(pattern, route):
			return False
	return True


def get_default_path(apps=None):
	if not apps:
		apps = get_apps()
	_apps = [app for app in apps if app.get("name") != "frappe"]

	if len(_apps) == 0:
		return None

	system_default_app = frappe.get_system_settings("default_app")
	user_default_app = frappe.db.get_value("User", frappe.session.user, "default_app")

	if system_default_app and not user_default_app:
		app = next((app for app in apps if app.get("name") == system_default_app), None)
		return app.get("route") if app else None
	elif user_default_app:
		app = next((app for app in apps if app.get("name") == user_default_app), None)
		return app.get("route") if app else None

	if len(_apps) == 1:
		return _apps[0].get("route") or "/apps"
	elif is_desk_apps(_apps):
		return "/app"
	return "/apps"


@frappe.whitelist()
def set_app_as_default(app_name):
	if frappe.db.get_value("User", frappe.session.user, "default_app") == app_name:
		frappe.db.set_value("User", frappe.session.user, "default_app", "")
	else:
		frappe.db.set_value("User", frappe.session.user, "default_app", app_name)


@frappe.whitelist()
def get_incomplete_setup_route(current_app, app_route):
	# Validate that current_app is a real installed app
	installed_apps = frappe.get_installed_apps()

	# If current_app is not a valid installed app, just return the app_route
	if current_app not in installed_apps:
		return app_route

	try:
		pending_apps = get_apps_with_incomplete_dependencies(current_app)
	except Exception:
		# If we can't get dependencies (e.g., invalid app name), return app_route
		return app_route

	if not pending_apps:
		return app_route

	for app in pending_apps:
		if app == "frappe":
			return "app"

		try:
			app_details = frappe.get_hooks("add_to_apps_screen", app_name=app)
			if not app_details:
				continue

			if route := app_details[0].get("route"):
				return route
		except Exception:
			# If we can't load hooks for this app, skip it
			continue

	return app_route
