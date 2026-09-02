# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
"""
bootstrap client session
"""

import os

import frappe
import frappe.defaults
import frappe.desk.desk_page
from frappe.core.doctype.installed_applications.installed_applications import (
	get_setup_wizard_completed_apps,
	get_setup_wizard_not_required_apps,
)
from frappe.core.doctype.navbar_settings.navbar_settings import get_app_logo, get_navbar_settings
from frappe.desk.doctype.changelog_feed.changelog_feed import get_changelog_feed_items
from frappe.desk.doctype.form_tour.form_tour import get_onboarding_ui_tours
from frappe.desk.doctype.route_history.route_history import frequently_visited_links
from frappe.desk.form.load import get_meta_bundle
from frappe.email.inbox import get_email_accounts
from frappe.integrations.frappe_providers.frappecloud_billing import is_fc_site
from frappe.model.base_document import get_controller
from frappe.permissions import has_permission
from frappe.query_builder import DocType
from frappe.query_builder.functions import Count
from frappe.query_builder.terms import ParameterizedValueWrapper, SubQuery
from frappe.social.doctype.energy_point_log.energy_point_log import get_energy_points
from frappe.social.doctype.energy_point_settings.energy_point_settings import (
	is_energy_point_enabled,
)
from frappe.utils import add_user_info, cstr, get_system_timezone
from frappe.utils.change_log import get_versions
from frappe.utils.frappecloud import on_frappecloud
from frappe.website.doctype.web_page_view.web_page_view import is_tracking_enabled


def get_bootinfo():
	"""build and return boot info"""
	from frappe.translate import get_lang_dict, get_translated_doctypes

	frappe.set_user_lang(frappe.session.user)
	bootinfo = frappe._dict()
	hooks = frappe.get_hooks()
	doclist = []

	# user
	get_user(bootinfo)

	# system info
	bootinfo.sitename = frappe.local.site
	bootinfo.sysdefaults = frappe.defaults.get_defaults()
	bootinfo.sysdefaults["setup_complete"] = frappe.is_setup_complete()

	# //// NEOFFICE PATCH — NeoCockpit unified chrome is the DEFAULT desk chrome.
	# Emergency kill-switch back to the legacy navbar+sidebar:
	#   bench --site <site> set-config neoffice_cockpit_disable 1
	bootinfo.neoffice_cockpit_disable = 1 if frappe.conf.get("neoffice_cockpit_disable") else 0

	bootinfo.server_date = frappe.utils.nowdate()

	if frappe.session["user"] != "Guest":
		bootinfo.user_info = get_user_info()

	bootinfo.modules = {}
	bootinfo.module_list = []
	load_desktop_data(bootinfo)
	bootinfo.letter_heads = get_letter_heads()
	bootinfo.active_domains = frappe.get_active_domains()
	bootinfo.all_domains = [d.get("name") for d in frappe.get_all("Domain")]
	add_layouts(bootinfo)

	bootinfo.module_app = frappe.local.module_app
	bootinfo.single_types = [d.name for d in frappe.get_all("DocType", {"issingle": 1})]
	bootinfo.nested_set_doctypes = [
		d.parent for d in frappe.get_all("DocField", {"fieldname": "lft"}, ["parent"])
	]
	add_home_page(bootinfo, doclist)
	bootinfo.page_info = get_allowed_pages()
	load_translations(bootinfo)
	add_timezone_info(bootinfo)
	load_conf_settings(bootinfo)
	load_print(bootinfo, doclist)
	doclist.extend(get_meta_bundle("Page"))
	bootinfo.home_folder = frappe.db.get_value("File", {"is_home_folder": 1})
	bootinfo.navbar_settings = get_navbar_settings()
	bootinfo.notification_settings = get_notification_settings()
	bootinfo.onboarding_tours = get_onboarding_ui_tours()
	set_time_zone(bootinfo)

	# ipinfo
	if frappe.session.data.get("ipinfo"):
		bootinfo.ipinfo = frappe.session["data"]["ipinfo"]

	# add docs
	bootinfo.docs = doclist
	load_country_doc(bootinfo)
	load_currency_docs(bootinfo)

	for method in hooks.boot_session or []:
		frappe.get_attr(method)(bootinfo)

	if bootinfo.lang:
		bootinfo.lang = str(bootinfo.lang)
	bootinfo.versions = {k: v["version"] for k, v in get_versions().items()}

	bootinfo.error_report_email = frappe.conf.error_report_email
	bootinfo.calendars = sorted(frappe.get_hooks("calendars"))
	bootinfo.treeviews = frappe.get_hooks("treeviews") or []
	bootinfo.lang_dict = get_lang_dict()
	bootinfo.success_action = get_success_action()
	bootinfo.update(get_email_accounts(user=frappe.session.user))
	bootinfo.energy_points_enabled = is_energy_point_enabled()
	bootinfo.website_tracking_enabled = is_tracking_enabled()
	bootinfo.sms_gateway_enabled = bool(frappe.db.get_single_value("SMS Settings", "sms_gateway_url"))
	bootinfo.points = get_energy_points(frappe.session.user)
	bootinfo.frequently_visited_links = frequently_visited_links()
	bootinfo.link_preview_doctypes = get_link_preview_doctypes()
	bootinfo.additional_filters_config = get_additional_filters_from_hooks()
	bootinfo.desk_settings = get_desk_settings()
	bootinfo.app_logo_url = get_app_logo()
	bootinfo.link_title_doctypes = get_link_title_doctypes()
	bootinfo.translated_doctypes = get_translated_doctypes()
	bootinfo.subscription_conf = add_subscription_conf()
	bootinfo.marketplace_apps = get_marketplace_apps()
	bootinfo.is_fc_site = is_fc_site()
	bootinfo.changelog_feed = get_changelog_feed_items()

	if sentry_dsn := get_sentry_dsn():
		bootinfo.sentry_dsn = sentry_dsn

	bootinfo.setup_wizard_completed_apps = get_setup_wizard_completed_apps() or []
	bootinfo.setup_wizard_not_required_apps = get_setup_wizard_not_required_apps() or []
	remove_apps_with_incomplete_dependencies(bootinfo)

	return bootinfo


def remove_apps_with_incomplete_dependencies(bootinfo):
	remove_apps = []

	for app in bootinfo.setup_wizard_not_required_apps:
		if app in bootinfo.setup_wizard_completed_apps:
			continue

		for required_apps in frappe.get_hooks("required_apps"):
			required_apps = required_apps.split("/")

			for required_app in required_apps:
				if app not in bootinfo.setup_wizard_not_required_apps:
					continue

				if required_app not in bootinfo.setup_wizard_completed_apps:
					remove_apps.append(app)

	for app in remove_apps:
		if app in bootinfo.setup_wizard_not_required_apps:
			bootinfo.setup_wizard_not_required_apps.remove(app)


def get_letter_heads():
	letter_heads = {}

	if not frappe.has_permission("Letter Head"):
		return letter_heads
	for letter_head in frappe.get_list("Letter Head", fields=["name", "content", "footer"]):
		letter_heads.setdefault(
			letter_head.name, {"header": letter_head.content, "footer": letter_head.footer}
		)

	return letter_heads


def load_conf_settings(bootinfo):
	from frappe.core.api.file import get_max_file_size

	bootinfo.max_file_size = get_max_file_size()
	for key in ("developer_mode", "socketio_port", "file_watcher_port"):
		if key in frappe.conf:
			bootinfo[key] = frappe.conf.get(key)


def load_desktop_data(bootinfo):
	from frappe.desk.desktop import get_workspace_sidebar_items

	# New sidebar system
	bootinfo.sidebar_pages = get_workspace_sidebar_items()
	allowed_pages = [d.name for d in bootinfo.sidebar_pages.get("pages")]

	# Keep for backward compatibility
	bootinfo.allowed_workspaces = bootinfo.sidebar_pages.get("pages")

	bootinfo.module_wise_workspaces = get_controller("Workspace").get_module_wise_workspaces()
	bootinfo.dashboards = frappe.get_all("Dashboard")

	# Generate app_data for apps switcher
	# Check if App Customization exists (custom app switcher management)
	if frappe.db.table_exists("App Customization") and frappe.db.count("App Customization") > 0:
		bootinfo.app_data = generate_app_data_from_customization(allowed_pages)
		# Generate reverse mapping: workspace_name → app_name for fast lookups
		bootinfo.workspace_to_app_map = generate_workspace_to_app_map(bootinfo.app_data)
		# Apply parent workspace overrides from App Customization
		apply_workspace_parent_overrides(bootinfo.sidebar_pages)
	else:
		# Fallback to default behavior
		bootinfo.app_data = generate_app_data_default(allowed_pages)
		bootinfo.workspace_to_app_map = {}


def generate_app_data_from_customization(allowed_pages):
	"""
	Generate app_data from App Customization doctype.
	This allows for:
	- Virtual apps (apps not actually installed)
	- Apps without workspaces
	- Custom ordering via sort_order
	- Enable/disable apps via enabled field

	Args:
		allowed_pages: List of workspace names the current user can access

	Returns:
		list: App data entries for bootinfo
	"""
	# Get all enabled apps sorted
	customizations = frappe.get_all(
		"App Customization",
		filters={"enabled": 1},
		fields=[
			"app_name", "app_title", "app_logo", "app_route",
			"is_virtual", "sort_order", "manage_all_workspaces"
		],
		order_by="sort_order asc, app_title asc"
	)

	app_data = []
	Workspace = frappe.qb.DocType("Workspace")
	Module = frappe.qb.DocType("Module Def")

	for custom in customizations:
		app_name = custom["app_name"]
		is_virtual = custom["is_virtual"]

		# Determine workspaces based on app type
		if is_virtual or custom["manage_all_workspaces"]:
			# Get workspaces from child table
			workspaces = get_customized_workspaces(app_name, allowed_pages)
		else:
			# Auto-discover from modules (default Frappe behavior)
			workspaces = [
				r[0]
				for r in (
					frappe.qb.from_(Workspace)
					.inner_join(Module)
					.on(Workspace.module == Module.name)
					.select(Workspace.name)
					.where(Module.app_name == app_name)
					.run()
				)
				if r[0] in allowed_pages
			]

		# Build app data entry
		# Resolve icon name to URL if neoffice_theme is installed
		app_logo_url = custom["app_logo"]
		if app_logo_url and "neoffice_theme" in frappe.get_installed_apps():
			try:
				from neoffice_theme.api import resolve_app_logo_url
				app_logo_url = resolve_app_logo_url(app_logo_url) or app_logo_url
			except Exception:
				pass

		app_entry = {
			"app_name": app_name,
			"app_title": custom["app_title"] or get_app_title_fallback(app_name, is_virtual),
			"app_logo_url": app_logo_url or get_app_logo_fallback(app_name, is_virtual),
			"app_route": custom["app_route"] or determine_app_route(app_name, workspaces),
			"modules": get_app_modules(app_name) if not is_virtual else [],
			"workspaces": workspaces,
			"sort_order": custom["sort_order"] if custom["sort_order"] is not None else 999,
		}

		# CRITICAL: Include app even if workspaces=[] (for apps like NORA AI)
		app_data.append(app_entry)

	return app_data


def get_customized_workspaces(app_name, allowed_pages):
	"""
	Get workspaces from App Customization Workspace child table.

	Args:
		app_name: App name from App Customization
		allowed_pages: List of workspaces user can access

	Returns:
		list: Workspace names that should appear for this app
	"""
	if not frappe.db.table_exists("App Customization Workspace"):
		return []

	workspace_configs = frappe.get_all(
		"App Customization Workspace",
		filters={
			"parent": app_name,
			"hidden": 0
		},
		fields=["workspace_name"],
		order_by="sort_order asc, idx asc"
	)

	# Filter to only accessible workspaces
	return [ws["workspace_name"] for ws in workspace_configs if ws["workspace_name"] in allowed_pages]


def generate_workspace_to_app_map(app_data):
	"""
	Generate reverse mapping from workspace name to app name for fast lookups.

	Args:
		app_data: List of app dictionaries with workspaces

	Returns:
		dict: Mapping of workspace_name → app_name
	"""
	workspace_to_app = {}
	for app_entry in app_data:
		app_name = app_entry["app_name"]
		for workspace in app_entry.get("workspaces", []):
			# Only map if not already mapped (first app wins in case of conflicts)
			if workspace not in workspace_to_app:
				workspace_to_app[workspace] = app_name

	return workspace_to_app


def apply_workspace_parent_overrides(sidebar_pages):
	"""
	Apply parent_workspace overrides from App Customization Workspace to sidebar pages.
	This allows App Customization to control workspace hierarchy independently of the
	Workspace doctype's parent_page field.

	Args:
		sidebar_pages: Dictionary containing 'pages' list from get_workspace_sidebar_items()
	"""
	if not frappe.db.table_exists("App Customization Workspace"):
		return

	# Get all parent_workspace overrides from App Customization Workspace child table
	parent_overrides = frappe.get_all(
		"App Customization Workspace",
		filters={"hidden": 0},
		fields=["workspace_name", "parent_workspace"]
	)

	# Create lookup dictionary: workspace_name → parent_workspace
	parent_map = {
		ws["workspace_name"]: ws["parent_workspace"]
		for ws in parent_overrides
		if ws.get("parent_workspace")  # Only include if parent_workspace is set
	}

	# Also track workspaces that should have NO parent (parent_workspace is null/empty in child table)
	reset_to_root = {
		ws["workspace_name"]
		for ws in parent_overrides
		if not ws.get("parent_workspace")  # parent_workspace is empty/null
	}

	# Apply overrides to sidebar pages
	if "pages" in sidebar_pages:
		for page in sidebar_pages["pages"]:
			workspace_name = page.get("name")
			if workspace_name in parent_map:
				# Override with value from App Customization
				page["parent_page"] = parent_map[workspace_name]
			elif workspace_name in reset_to_root:
				# Explicitly set to None to make it a root-level workspace
				page["parent_page"] = None


def get_app_title_fallback(app_name, is_virtual=False):
	"""Fallback to app_title hook if not in App Customization."""
	if is_virtual:
		return app_name.replace("_", " ").title()

	try:
		hooks = frappe.get_hooks("app_title", app_name=app_name)
		return hooks[0] if hooks else app_name.replace("_", " ").title()
	except (ImportError, ModuleNotFoundError):
		return app_name.replace("_", " ").title()


def get_app_logo_fallback(app_name, is_virtual=False):
	"""Fallback to app_logo_url hook if not in App Customization."""
	if is_virtual:
		return "/assets/frappe/images/frappe-framework-logo.svg"

	try:
		hooks = frappe.get_hooks("app_logo_url", app_name=app_name)
		if hooks:
			return hooks[0]
	except (ImportError, ModuleNotFoundError):
		pass

	# Fallback to Frappe logo
	try:
		frappe_logo = frappe.get_hooks("app_logo_url", app_name="frappe")
		return frappe_logo[0] if frappe_logo else "/assets/frappe/images/frappe-framework-logo.svg"
	except:
		return "/assets/frappe/images/frappe-framework-logo.svg"


def determine_app_route(app_name, workspaces):
	"""
	Determine default route for app.
	Priority: app_home hook > first workspace > /app/home
	"""
	app_home = frappe.get_hooks("app_home", app_name=app_name)
	if app_home:
		return app_home[0]

	if workspaces:
		return f"/app/{frappe.utils.slug(workspaces[0])}"

	return "/app/home"


def get_app_modules(app_name):
	"""Get modules for installed app."""
	return [m.name for m in frappe.get_all("Module Def", {"app_name": app_name}, ["name"])]


def generate_app_data_default(allowed_pages):
	"""
	Original Frappe behavior - kept for backward compatibility.
	Used when App Customization is not available.

	Args:
		allowed_pages: List of workspace names the current user can access

	Returns:
		list: App data entries for bootinfo
	"""
	app_data = []
	Workspace = frappe.qb.DocType("Workspace")
	Module = frappe.qb.DocType("Module Def")

	for app_name in frappe.get_installed_apps():
		# Get app details from hooks
		apps = frappe.get_hooks("add_to_apps_screen", app_name=app_name)
		app_info = {}
		if apps:
			app_info = apps[0]
			has_permission = app_info.get("has_permission")
			if has_permission and not frappe.get_attr(has_permission)():
				continue

		# Get workspaces for this app
		workspaces = [
			r[0]
			for r in (
				frappe.qb.from_(Workspace)
				.inner_join(Module)
				.on(Workspace.module == Module.name)
				.select(Workspace.name)
				.where(Module.app_name == app_name)
				.run()
			)
			if r[0] in allowed_pages
		]

		app_data.append(
			dict(
				app_name=app_info.get("name") or app_name,
				app_title=app_info.get("title")
					or (frappe.get_hooks("app_title", app_name=app_name)
						and frappe.get_hooks("app_title", app_name=app_name)[0])
					or app_name,
				app_route=(frappe.get_hooks("app_home", app_name=app_name)
					and frappe.get_hooks("app_home", app_name=app_name)[0])
					or (workspaces and "/app/" + frappe.utils.slug(workspaces[0]))
					or "",
				app_logo_url=app_info.get("logo")
					or (frappe.get_hooks("app_logo_url", app_name=app_name)
						and frappe.get_hooks("app_logo_url", app_name=app_name)[0])
					or (frappe.get_hooks("app_logo_url", app_name="frappe")
						and frappe.get_hooks("app_logo_url", app_name="frappe")[0])
					or "/assets/frappe/images/frappe-framework-logo.svg",
				modules=[m.name for m in frappe.get_all("Module Def", dict(app_name=app_name))],
				workspaces=workspaces,
				sort_order=999,  # Add sort_order field for consistency
			)
		)

	return app_data


def get_allowed_pages(cache=False):
	return get_user_pages_or_reports("Page", cache=cache)


def get_allowed_reports(cache=False):
	return get_user_pages_or_reports("Report", cache=cache)


def get_allowed_report_names(cache=False) -> set[str]:
	return {cstr(report) for report in get_allowed_reports(cache).keys() if report}


def get_user_pages_or_reports(parent, cache=False):
	if cache:
		has_role = frappe.cache.get_value("has_role:" + parent, user=frappe.session.user)
		if has_role:
			return has_role

	roles = frappe.get_roles()
	has_role = {}

	page = DocType("Page")
	report = DocType("Report")

	is_report = parent == "Report"

	#//// Neoffice — `module` travels with every page and report. Upstream sends
	#//// only title + modified, so the cockpit cannot tell which application a
	#//// desk PAGE belongs to: opening Fitness → Members left "Commercial" as the
	#//// active module, because only a workspace route could be resolved to an
	#//// app. A doctype resolves through its meta on the client; a page has no
	#//// client-side meta at all, so its module has to come from here.
	if is_report:
		columns = (report.name.as_("title"), report.ref_doctype, report.report_type, report.module)
	else:
		columns = (page.title.as_("title"), page.module)

	customRole = DocType("Custom Role")
	hasRole = DocType("Has Role")
	parentTable = DocType(parent)

	# get pages or reports set on custom role
	pages_with_custom_roles = (
		frappe.qb.from_(customRole)
		.from_(hasRole)
		.from_(parentTable)
		.select(customRole[parent.lower()].as_("name"), customRole.modified, customRole.ref_doctype, *columns)
		.where(
			(hasRole.parent == customRole.name)
			& (parentTable.name == customRole[parent.lower()])
			& (customRole[parent.lower()].isnotnull())
			& (hasRole.role.isin(roles))
		)
	).run(as_dict=True)

	for p in pages_with_custom_roles:
		has_role[p.name] = {
			"modified": p.modified,
			"title": p.title,
			"ref_doctype": p.ref_doctype,
			"module": p.module,  #//// Neoffice — see `columns` above
		}

	subq = (
		frappe.qb.from_(customRole)
		.select(customRole[parent.lower()])
		.where(customRole[parent.lower()].isnotnull())
	)

	pages_with_standard_roles = (
		frappe.qb.from_(hasRole)
		.from_(parentTable)
		.select(parentTable.name.as_("name"), parentTable.modified, *columns)
		.where(
			(hasRole.role.isin(roles)) & (hasRole.parent == parentTable.name) & (parentTable.name.notin(subq))
		)
		.distinct()
	)

	if is_report:
		pages_with_standard_roles = pages_with_standard_roles.where(report.disabled == 0)

	pages_with_standard_roles = pages_with_standard_roles.run(as_dict=True)

	for p in pages_with_standard_roles:
		if p.name not in has_role:
			has_role[p.name] = {"modified": p.modified, "title": p.title, "module": p.module}  #//// Neoffice
			if parent == "Report":
				has_role[p.name].update({"ref_doctype": p.ref_doctype})

	no_of_roles = SubQuery(
		frappe.qb.from_(hasRole).select(Count("*")).where(hasRole.parent == parentTable.name)
	)

	# pages and reports with no role are allowed
	rows_with_no_roles = (
		frappe.qb.from_(parentTable)
		.select(parentTable.name, parentTable.modified, *columns)
		.where(no_of_roles == 0)
	).run(as_dict=True)

	for r in rows_with_no_roles:
		if r.name not in has_role:
			has_role[r.name] = {"modified": r.modified, "title": r.title, "module": r.module}  #//// Neoffice
			if is_report:
				has_role[r.name] |= {"ref_doctype": r.ref_doctype}

	if is_report:
		if not has_permission("Report", raise_exception=False):
			return {}

		reports = frappe.get_list(
			"Report",
			fields=["name", "report_type"],
			filters={"name": ("in", has_role.keys())},
			ignore_ifnull=True,
		)
		for report in reports:
			has_role[report.name]["report_type"] = report.report_type

		non_permitted_reports = set(has_role.keys()) - {r.name for r in reports}
		for r in non_permitted_reports:
			has_role.pop(r, None)

	# Expire every six hours
	frappe.cache.set_value("has_role:" + parent, has_role, frappe.session.user, 21600)
	return has_role


def load_translations(bootinfo):
	from frappe.translate import get_messages_for_boot

	bootinfo["lang"] = frappe.lang
	bootinfo["__messages"] = get_messages_for_boot()


def get_user_info():
	# get info for current user
	user_info = frappe._dict()
	add_user_info(frappe.session.user, user_info)

	if frappe.session.user == "Administrator" and user_info.Administrator.email:
		user_info[user_info.Administrator.email] = user_info.Administrator

	return user_info


def get_user(bootinfo):
	"""get user info"""
	bootinfo.user = frappe.get_user().load_user()


def add_home_page(bootinfo, docs):
	"""load home page"""
	if frappe.session.user == "Guest":
		return
	home_page = frappe.db.get_default("desktop:home_page")

	if not frappe.is_setup_complete():
		bootinfo.setup_wizard_requires = frappe.get_hooks("setup_wizard_requires")

	try:
		page = frappe.desk.desk_page.get(home_page)
		docs.append(page)
		bootinfo["home_page"] = page.name
	except (frappe.DoesNotExistError, frappe.PermissionError):
		frappe.clear_last_message()
		bootinfo["home_page"] = "Workspaces"


def add_timezone_info(bootinfo):
	system = bootinfo.sysdefaults.get("time_zone")
	import frappe.utils.momentjs

	bootinfo.timezone_info = {"zones": {}, "rules": {}, "links": {}}
	frappe.utils.momentjs.update(system, bootinfo.timezone_info)


def load_print(bootinfo, doclist):
	print_settings = frappe.db.get_singles_dict("Print Settings")
	print_settings.doctype = ":Print Settings"
	doclist.append(print_settings)
	load_print_css(bootinfo, print_settings)


def load_print_css(bootinfo, print_settings):
	import frappe.www.printview

	bootinfo.print_css = frappe.www.printview.get_print_style(
		print_settings.print_style or "Redesign", for_legacy=True
	)


def get_unseen_notes():
	note = DocType("Note")
	nsb = DocType("Note Seen By").as_("nsb")

	return (
		frappe.qb.from_(note)
		.select(note.name, note.title, note.content, note.notify_on_every_login)
		.where(
			(note.notify_on_login == 1)
			& (note.expire_notification_on > frappe.utils.now())
			& (
				ParameterizedValueWrapper(frappe.session.user).notin(
					SubQuery(frappe.qb.from_(nsb).select(nsb.user).where(nsb.parent == note.name))
				)
			)
		)
	).run(as_dict=1)


def get_success_action():
	return frappe.get_all("Success Action", fields=["*"])


def get_link_preview_doctypes():
	from frappe.utils import cint

	link_preview_doctypes = [d.name for d in frappe.get_all("DocType", {"show_preview_popup": 1})]
	customizations = frappe.get_all(
		"Property Setter", fields=["doc_type", "value"], filters={"property": "show_preview_popup"}
	)

	for custom in customizations:
		if not cint(custom.value) and custom.doc_type in link_preview_doctypes:
			link_preview_doctypes.remove(custom.doc_type)
		else:
			link_preview_doctypes.append(custom.doc_type)

	return link_preview_doctypes


def get_additional_filters_from_hooks():
	filter_config = frappe._dict()
	filter_hooks = frappe.get_hooks("filters_config")
	for hook in filter_hooks:
		filter_config.update(frappe.get_attr(hook)())

	return filter_config


def add_layouts(bootinfo):
	# add routes for readable doctypes
	bootinfo.doctype_layouts = frappe.get_all("DocType Layout", ["name", "route", "document_type"])


def get_desk_settings():
	from frappe.core.doctype.user.user import desk_properties

	return frappe.get_value("User", frappe.session.user, desk_properties, as_dict=True)


def get_notification_settings():
	return frappe.get_cached_doc("Notification Settings", frappe.session.user)


def get_link_title_doctypes():
	dts = frappe.get_all("DocType", {"show_title_field_in_link": 1})
	custom_dts = frappe.get_all(
		"Property Setter",
		{"property": "show_title_field_in_link", "value": "1"},
		["doc_type as name"],
	)
	return [d.name for d in dts + custom_dts if d]


def set_time_zone(bootinfo):
	bootinfo.time_zone = {
		"system": get_system_timezone(),
		"user": bootinfo.get("user_info", {}).get(frappe.session.user, {}).get("time_zone", None)
		or get_system_timezone(),
	}


def load_country_doc(bootinfo):
	country = frappe.db.get_default("country")
	if not country:
		return
	try:
		bootinfo.docs.append(frappe.get_cached_doc("Country", country))
	except Exception:
		pass


def load_currency_docs(bootinfo):
	currency = frappe.qb.DocType("Currency")

	currency_docs = (
		frappe.qb.from_(currency)
		.select(
			currency.name,
			currency.fraction,
			currency.fraction_units,
			currency.number_format,
			currency.smallest_currency_fraction_value,
			currency.symbol,
			currency.symbol_on_right,
		)
		.where(currency.enabled == 1)
		.run(as_dict=1, update={"doctype": ":Currency"})
	)

	bootinfo.docs += currency_docs


def get_marketplace_apps():
	import requests

	apps = []
	cache_key = "frappe_marketplace_apps"

	if frappe.conf.developer_mode or not on_frappecloud():
		return apps

	def get_apps_from_fc():
		remote_site = frappe.conf.frappecloud_url or "frappecloud.com"
		request_url = f"https://{remote_site}/api/method/press.api.marketplace.get_marketplace_apps"
		request = requests.get(request_url, timeout=2.0)
		return request.json()["message"]

	try:
		apps = frappe.cache().get_value(cache_key, get_apps_from_fc, shared=True)
		installed_apps = set(frappe.get_installed_apps())
		apps = [app for app in apps if app["name"] not in installed_apps]
	except Exception:
		# Don't retry for a day
		frappe.cache().set_value(cache_key, apps, shared=True, expires_in_sec=24 * 60 * 60)

	return apps


def add_subscription_conf():
	try:
		return frappe.conf.subscription
	except Exception:
		return ""


def get_sentry_dsn():
	if not frappe.get_system_settings("enable_telemetry"):
		return

	return os.getenv("FRAPPE_SENTRY_DSN")
