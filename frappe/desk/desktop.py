# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
# Author - Shivam Mishra <shivam@frappe.io>

from functools import wraps
from json import dumps, loads

import frappe
from frappe import DoesNotExistError, ValidationError, _, _dict
from frappe.boot import get_allowed_pages, get_allowed_reports
from frappe.cache_manager import (
	build_domain_restriced_doctype_cache,
	build_domain_restriced_page_cache,
	build_table_count_cache,
)
from frappe.core.doctype.custom_role.custom_role import get_custom_allowed_roles


def handle_not_exist(fn):
	@wraps(fn)
	def wrapper(*args, **kwargs):
		try:
			return fn(*args, **kwargs)
		except DoesNotExistError:
			frappe.clear_last_message()
			return []

	return wrapper


class Workspace:
	def __init__(self, page, minimal=False):
		self.page_name = page.get("name")
		self.page_title = page.get("title")
		self.public_page = page.get("public")
		self.workspace_manager = "Workspace Manager" in frappe.get_roles()

		self.user = frappe.get_user()
		self.allowed_modules = self.get_cached("user_allowed_modules", self.get_allowed_modules)

		self.doc = frappe.get_cached_doc("Workspace", self.page_name)
		if (
			self.doc
			and self.doc.module
			and self.doc.module not in self.allowed_modules
			and not self.workspace_manager
		):
			raise frappe.PermissionError

		self.can_read = self.get_cached("user_perm_can_read", self.get_can_read_items)

		if not minimal:
			self.allowed_pages = get_allowed_pages(cache=True)
			self.allowed_reports = get_allowed_reports(cache=True)

			if self.doc.content:
				self.onboarding_list = [
					x["data"]["onboarding_name"] for x in loads(self.doc.content) if x["type"] == "onboarding"
				]
			self.onboardings = []

			self.table_counts = get_table_with_counts()
		self.restricted_doctypes = (
			frappe.cache.get_value("domain_restricted_doctypes") or build_domain_restriced_doctype_cache()
		)
		self.restricted_pages = (
			frappe.cache.get_value("domain_restricted_pages") or build_domain_restriced_page_cache()
		)

	def is_permitted(self):
		"""Returns true if Has Role is not set or the user is allowed."""
		from frappe.utils import has_common

		allowed = [d.role for d in self.doc.roles]

		custom_roles = get_custom_allowed_roles("page", self.doc.name)
		allowed.extend(custom_roles)

		if not allowed:
			return True

		roles = frappe.get_roles()

		if has_common(roles, allowed):
			return True

	def get_cached(self, cache_key, fallback_fn):
		value = frappe.cache.get_value(cache_key, user=frappe.session.user)
		if value is not None:
			return value

		value = fallback_fn()

		# Expire every six hour
		frappe.cache.set_value(cache_key, value, frappe.session.user, 21600)
		return value

	def get_can_read_items(self):
		if not self.user.can_read:
			self.user.build_permissions()

		return self.user.can_read

	def get_allowed_modules(self):
		if not self.user.allow_modules:
			self.user.build_permissions()

		return self.user.allow_modules

	def get_onboarding_doc(self, onboarding):
		# Check if onboarding is enabled
		if not frappe.get_system_settings("enable_onboarding"):
			return None

		if not self.onboarding_list:
			return None

		if frappe.db.get_value("Module Onboarding", onboarding, "is_complete"):
			return None

		doc = frappe.get_doc("Module Onboarding", onboarding)

		# Check if user is allowed
		allowed_roles = set(doc.get_allowed_roles())
		user_roles = set(frappe.get_roles())
		if not allowed_roles & user_roles:
			return None

		# Check if already complete
		if doc.check_completion():
			return None

		return doc

	def is_item_allowed(self, name, item_type):
		if frappe.session.user == "Administrator":
			return True

		item_type = item_type.lower()

		if item_type == "doctype":
			return name in (self.can_read or []) and name in (self.restricted_doctypes or [])
		if item_type == "page":
			if not self.allowed_pages:
				self.allowed_pages = get_allowed_pages(cache=True)
			return name in self.allowed_pages and name in self.restricted_pages
		if item_type == "report":
			if not self.allowed_reports:
				self.allowed_reports = get_allowed_reports(cache=True)
			return name in self.allowed_reports
		if item_type == "help":
			return True
		if item_type == "dashboard":
			return True
		if item_type == "url":
			return True

		return False

	def build_workspace(self):
		self.cards = {"items": self.get_links()}
		self.charts = {"items": self.get_charts()}
		self.shortcuts = {"items": self.get_shortcuts()}
		self.onboardings = {"items": self.get_onboardings()}
		self.quick_lists = {"items": self.get_quick_lists()}
		self.number_cards = {"items": self.get_number_cards()}
		self.custom_blocks = {"items": self.get_custom_blocks()}

	def _doctype_contains_a_record(self, name):
		exists = self.table_counts.get(name, False)

		if not exists and frappe.db.exists(name):
			if not frappe.db.get_value("DocType", name, "issingle"):
				exists = bool(frappe.get_all(name, limit=1))
			else:
				exists = True
			self.table_counts[name] = exists

		return exists

	def _prepare_item(self, item):
		if item.dependencies:
			dependencies = [dep.strip() for dep in item.dependencies.split(",")]

			incomplete_dependencies = [d for d in dependencies if not self._doctype_contains_a_record(d)]

			if len(incomplete_dependencies):
				item.incomplete_dependencies = incomplete_dependencies
			else:
				item.incomplete_dependencies = ""

		if item.onboard:
			# Mark Spotlights for initial
			if item.get("type") == "doctype":
				name = item.get("name")
				count = self._doctype_contains_a_record(name)

				item["count"] = count

		if item.get("link_type") == "DocType":
			item["description"] = frappe.get_meta(item.link_to).description

		# Translate label
		item["label"] = _(item.label) if item.label else _(item.name)

		return item

	def is_custom_block_permitted(self, custom_block_name):
		from frappe.utils import has_common

		allowed = [
			d.role for d in frappe.get_all("Has Role", fields=["role"], filters={"parent": custom_block_name})
		]

		if not allowed:
			return True

		roles = frappe.get_roles()

		if has_common(roles, allowed):
			return True

		return False

	@handle_not_exist
	def get_links(self):
		cards = self.doc.get_link_groups()

		if not self.doc.hide_custom:
			cards = cards + get_custom_reports_and_doctypes(self.doc.module)

		default_country = frappe.db.get_default("country")

		new_data = []
		for card in cards:
			new_items = []
			card = _dict(card)

			links = card.get("links", [])

			for item in links:
				item = _dict(item)

				# Condition: based on country
				if item.country and item.country != default_country:
					continue

				# Check if user is allowed to view
				if self.is_item_allowed(item.link_to, item.link_type):
					prepared_item = self._prepare_item(item)
					new_items.append(prepared_item)

			if new_items:
				if isinstance(card, _dict):
					new_card = card.copy()
				else:
					new_card = card.as_dict().copy()
				new_card["links"] = new_items
				new_card["label"] = _(new_card["label"])
				new_data.append(new_card)

		return new_data

	@handle_not_exist
	def get_charts(self):
		all_charts = []
		if frappe.has_permission("Dashboard Chart", throw=False):
			charts = self.doc.charts

			for chart in charts:
				if frappe.has_permission("Dashboard Chart", doc=chart.chart_name):
					# Translate label
					chart.label = _(chart.label) if chart.label else _(chart.chart_name)
					all_charts.append(chart)

		return all_charts

	@handle_not_exist
	def get_shortcuts(self):
		def _in_active_domains(item):
			if not item.restrict_to_domain:
				return True
			else:
				return item.restrict_to_domain in frappe.get_active_domains()

		items = []
		shortcuts = self.doc.shortcuts

		for item in shortcuts:
			new_item = item.as_dict().copy()
			if self.is_item_allowed(item.link_to, item.type) and _in_active_domains(item):
				if item.type == "Report":
					report = self.allowed_reports.get(item.link_to, {})
					if report.get("report_type") in ["Query Report", "Script Report", "Custom Report"]:
						new_item["is_query_report"] = 1
					else:
						new_item["ref_doctype"] = report.get("ref_doctype")

				# Translate label
				new_item["label"] = _(item.label) if item.label else _(item.link_to)

				items.append(new_item)

		return items

	@handle_not_exist
	def get_quick_lists(self):
		items = []
		quick_lists = self.doc.quick_lists

		for item in quick_lists:
			if self.is_item_allowed(item.document_type, "doctype"):
				new_item = item.as_dict().copy()

				# Translate label
				new_item["label"] = _(item.label) if item.label else _(item.document_type)

				items.append(new_item)

		return items

	@handle_not_exist
	def get_onboardings(self):
		if self.onboarding_list:
			for onboarding in self.onboarding_list:
				onboarding_doc = self.get_onboarding_doc(onboarding)
				if onboarding_doc:
					item = {
						"label": _(onboarding),
						"title": _(onboarding_doc.title),
						"subtitle": _(onboarding_doc.subtitle),
						"success": _(onboarding_doc.success_message),
						"docs_url": onboarding_doc.documentation_url,
						"items": self.get_onboarding_steps(onboarding_doc),
					}
					self.onboardings.append(item)
		return self.onboardings

	@handle_not_exist
	def get_onboarding_steps(self, onboarding_doc):
		steps = []
		for doc in onboarding_doc.get_steps():
			step = doc.as_dict().copy()
			step.label = _(doc.title)
			if step.action == "Create Entry":
				step.is_submittable = frappe.db.get_value(
					"DocType", step.reference_document, "is_submittable", cache=True
				)
			steps.append(step)

		return steps

	@handle_not_exist
	def get_number_cards(self):
		all_number_cards = []
		if frappe.has_permission("Number Card", throw=False):
			number_cards = self.doc.number_cards
			for number_card in number_cards:
				if frappe.has_permission("Number Card", doc=number_card.number_card_name):
					# Translate label
					number_card.label = (
						_(number_card.label) if number_card.label else _(number_card.number_card_name)
					)
					all_number_cards.append(number_card)

		return all_number_cards

	@handle_not_exist
	def get_custom_blocks(self):
		all_custom_blocks = []
		if frappe.has_permission("Custom HTML Block", throw=False):
			custom_blocks = self.doc.custom_blocks

			for custom_block in custom_blocks:
				if frappe.has_permission("Custom HTML Block", doc=custom_block.custom_block_name):
					if not self.is_custom_block_permitted(custom_block.custom_block_name):
						continue

					# Translate label
					custom_block.label = (
						_(custom_block.label) if custom_block.label else _(custom_block.custom_block_name)
					)
					all_custom_blocks.append(custom_block)

		return all_custom_blocks


@frappe.whitelist()
@frappe.read_only()
def get_desktop_page(page):
	"""Applies permissions, customizations and returns the configruration for a page
	on desk.

	Args:
	        page (json): page data

	Returns:
	        dict: dictionary of cards, charts and shortcuts to be displayed on website
	"""
	try:
		workspace = Workspace(loads(page))
		#//// Neoffice — upstream builds and returns the page without ever asking
		#//// `is_permitted()`. Only `__init__` guards, and only on the MODULE, so
		#//// a workspace carrying no module is reachable by URL by anyone with a
		#//// desk account — `is_hidden` merely removes it from the sidebar.
		#////
		#//// That is how the twelve Construction workspaces stayed openable at
		#//// /app/construction-* on customer sites (measured 2026-08-27). Their
		#//// shortcuts are of type URL, so `is_item_allowed()` has nothing to
		#//// filter either and the whole page renders.
		#////
		#//// This costs nothing where nobody set roles: `is_permitted()` returns
		#//// True when the roles table is empty, which is the case for 49 of our
		#//// 49 public workspaces today. It only bites once we deliberately put a
		#//// role on a workspace — which is how we gate the applications sold
		#//// separately (see neoffice_theme/app_visibility.py).
		if not workspace.is_permitted():
			raise frappe.PermissionError
		workspace.build_workspace()
		return {
			"charts": workspace.charts,
			"shortcuts": workspace.shortcuts,
			"cards": workspace.cards,
			"onboardings": workspace.onboardings,
			"quick_lists": workspace.quick_lists,
			"number_cards": workspace.number_cards,
			"custom_blocks": workspace.custom_blocks,
		}
	except DoesNotExistError:
		frappe.log_error("Workspace Missing")
		return {}


@frappe.whitelist()
def get_workspace_sidebar_items(current_workspace=None):
	"""Get list of sidebar items for desk"""
	has_access = "Workspace Manager" in frappe.get_roles()

	# don't get domain restricted pages
	blocked_modules = frappe.get_cached_doc("User", frappe.session.user).get_blocked_modules()
	blocked_modules.append("Dummy Module")

	# adding None to allowed_domains to include pages without domain restriction
	allowed_domains = [None, *frappe.get_active_domains()]

	filters = {
		"restrict_to_domain": ["in", allowed_domains],
		"module": ["not in", blocked_modules],
	}

	if has_access:
		filters = []

	# pages sorted based on sequence id
	order_by = "sequence_id asc"
	fields = [
		"name",
		"title",
		"for_user",
		"parent_page",
		"content",
		"public",
		"module",
		"icon",
		"indicator_color",
		"is_hidden",
		"sequence_id",
	]
	all_pages = frappe.get_all(
		"Workspace", fields=fields, filters=filters, order_by=order_by, ignore_permissions=True
	)

	#////
	import json
	# Get the path to the JSON file
	#//// Neoffice — neoffice_theme is absent on a bare bench (CI of the product forks): guard the access
	#//// like boot.py and apps.py do, otherwise get_apps() broke every page render there (#198).
	excluded_menus = {}
	if "neoffice_theme" in frappe.get_installed_apps():
		json_path = frappe.get_app_path('neoffice_theme', 'json', 'excluded_menus.json')

		# Load JSON file for excluded titles
		try:
			with open(json_path, 'r') as file:
				excluded_menus = json.load(file)
		except FileNotFoundError:
			pass

	# Get the user's view interface setting
	try:
		user_view_interface = frappe.get_doc("User", frappe.session.user).get("view_interface") or "Simplified"
	except Exception:
		user_view_interface = "Simplified"

	# Initialize exclusion set with titles from the JSON file
	excluded_titles = set()
	custom_links = {}

	# Check for the existence of user_view_interface in excluded_menus and process if exists
	if user_view_interface in excluded_menus:
		# Get "Hide" and "Link" sections, default to empty list if not present
		hide_menus = excluded_menus[user_view_interface].get("Hide", [])
		link_menus = excluded_menus[user_view_interface].get("Link", [])
		
		# Process Hide menus if not empty
		if hide_menus:
			for menu in hide_menus:
				if 'title' in menu:
					excluded_titles.add(menu['title'].lower())

		# Process Link menus if not empty
		if link_menus:
			for menu in link_menus:
				if 'title' in menu and 'link' in menu:
					custom_links[menu['title'].lower()] = menu['link']
		# Identify and add child pages of excluded pages to the exclusion set
		for page in all_pages:
			if page.get('parent_page') is not None and page['parent_page'].lower() in excluded_titles:
				excluded_titles.add(page['title'].lower())

	# Get workspaces that are part of virtual apps OR apps with manage_all_workspaces - these should NOT be excluded
	virtual_app_workspaces = set()
	try:
		app_customizations = frappe.get_all(
			"App Customization",
			filters={"enabled": 1},
			fields=["name", "is_virtual", "manage_all_workspaces"]
		)
		for app_custom in app_customizations:
			# Include workspaces from virtual apps OR apps that manage all their workspaces
			if app_custom.get("is_virtual") or app_custom.get("manage_all_workspaces"):
				workspaces = frappe.get_all(
					"App Customization Workspace",
					filters={"parent": app_custom["name"], "hidden": 0},
					pluck="workspace_name"
				)
				virtual_app_workspaces.update(workspaces)
	except Exception:
		pass  # App Customization might not exist

	# NEW: Get sort_order for ALL workspaces from App Customization
	# This will be used to sort workspaces regardless of app context
	#//// Neoffice — App Customization Workspace belongs to neoffice_theme: on a bare bench (CI of the
	#//// product forks) the table does not exist and every page render died on ProgrammingError
	#//// (#198). Same guard as boot.py; the sort order simply stays empty there.
	all_workspace_sort_orders = []
	if frappe.db.table_exists("App Customization Workspace"):
		all_workspace_sort_orders = frappe.get_all(
			"App Customization Workspace",
			filters={"hidden": 0},
			fields=["workspace_name", "sort_order", "parent"],
			order_by="sort_order asc, idx asc"
		)
	workspace_sort_order = {w['workspace_name']: w['sort_order'] for w in all_workspace_sort_orders}

	# Determine which app's workspaces to show
	# If current_workspace is provided, find which app it belongs to
	current_app_workspaces = None
	if current_workspace:
		try:
			# Find which App Customization contains this workspace
			app_workspace_link = frappe.db.get_value(
				"App Customization Workspace",
				{"workspace_name": current_workspace},
				["parent", "hidden"],
				as_dict=True
			)

			if app_workspace_link and not app_workspace_link.hidden:
				app_name = app_workspace_link.parent
				# Get app customization
				app_custom = frappe.get_doc("App Customization", app_name)

				if app_custom.enabled and (app_custom.is_virtual or app_custom.manage_all_workspaces):
					# Get all workspaces for this app (just the names, sort_order already loaded above)
					workspace_configs = frappe.get_all(
						"App Customization Workspace",
						filters={"parent": app_name, "hidden": 0},
						fields=["workspace_name"],
						order_by="sort_order asc, idx asc"
					)
					current_app_workspaces = [w['workspace_name'] for w in workspace_configs]
		except Exception as e:
			pass

	pages = []
	private_pages = []

	# Filter Page based on Permission
	for page in all_pages:
		page_title_lower = page['title'].lower()
		if page_title_lower in custom_links:
			page['custom_link'] = custom_links[page_title_lower]

		# NEW: If we're in a specific app context, ONLY show workspaces from that app
		if current_app_workspaces is not None:
			# We're in an app context - only show workspaces from this app
			if page['name'] not in current_app_workspaces:
				continue  # Skip this workspace, it's not in the current app

		# Don't exclude workspaces that are part of virtual apps
		is_in_virtual_app = page['name'] in virtual_app_workspaces
		if page_title_lower not in excluded_titles or is_in_virtual_app:
			try:
				workspace = Workspace(page, True)
				if has_access or workspace.is_permitted():
					if page.public and (has_access or not page.is_hidden) and page.title != "Welcome Workspace":
						pages.append(page)
					elif page.for_user == frappe.session.user:
						private_pages.append(page)
					page["label"] = _(page.get("name"))
			except frappe.PermissionError:
				pass
	#////

	# Sort pages by App Customization sort_order
	# Workspaces with sort_order defined will be sorted first, then by sequence_id
	# Workspaces without sort_order (999999) will appear at the end, sorted by sequence_id
	if workspace_sort_order:
		pages.sort(key=lambda p: (workspace_sort_order.get(p['name'], 999999), p.get('sequence_id', 0)))

	if private_pages:
		pages.extend(private_pages)

	if len(pages) == 0:
		pages = [frappe.get_doc("Workspace", "Welcome Workspace").as_dict()]
		pages[0]["label"] = _("Welcome Workspace")

	return {
		"pages": pages,
		"has_access": has_access,
		"has_create_access": frappe.has_permission(doctype="Workspace", ptype="create"),
	}


def get_table_with_counts():
	counts = frappe.cache.get_value("information_schema:counts")
	if not counts:
		counts = build_table_count_cache()

	return counts


def get_custom_reports_and_doctypes(module):
	return [
		_dict({"label": _("Custom Documents"), "links": get_custom_doctype_list(module)}),
		_dict({"label": _("Custom Reports"), "links": get_custom_report_list(module)}),
	]


def get_custom_doctype_list(module):
	doctypes = frappe.get_all(
		"DocType",
		fields=["name"],
		filters={"custom": 1, "istable": 0, "module": module},
		order_by="name",
	)

	return [
		{
			"type": "Link",
			"link_type": "doctype",
			"link_to": d.name,
			"label": _(d.name),
		}
		for d in doctypes
	]


def get_custom_report_list(module):
	"""Returns list on new style reports for modules."""
	reports = frappe.get_all(
		"Report",
		fields=["name", "ref_doctype", "report_type"],
		filters={"is_standard": "No", "disabled": 0, "module": module},
		order_by="name",
	)

	return [
		{
			"type": "Link",
			"link_type": "report",
			"doctype": r.ref_doctype,
			"dependencies": r.ref_doctype,
			"is_query_report": 1
			if r.report_type in ("Query Report", "Script Report", "Custom Report")
			else 0,
			"label": _(r.name),
			"link_to": r.name,
			"report_ref_doctype": r.ref_doctype,
		}
		for r in reports
	]


def _upsert_widgets(doc, parentfield, configs, doctype):
	"""Edit-aware widget save: update the existing child row when the editor
	re-saves a widget with the same label, otherwise append a new one.

	Without this, every re-saved widget was appended as a duplicate that
	clean_up() then dropped (it keeps the stale FIRST row by label), so editing
	a shortcut filter, colour, type or URL silently never persisted. It also
	blanks a URL shortcut leftover link_to (a stale DocType/Page target left
	when the type is switched to URL) which otherwise fails link validation
	with "DocType URL not found" and aborts the whole save.
	"""
	if not configs:
		return
	by_label = {}
	for row in doc.get(parentfield):
		by_label.setdefault(row.label, row)
	to_add = []
	for cfg in configs:
		cfg = dict(cfg)
		cfg.pop("name", None)
		if cfg.get("type") == "URL":
			cfg["link_to"] = None
		existing = by_label.get(cfg.get("label"))
		if existing:
			existing.update(cfg)
		else:
			to_add.append(cfg)
	if to_add:
		doc.get(parentfield).extend(new_widget(to_add, doctype, parentfield))


def save_new_widget(doc, page, blocks, new_widgets, deleted_widgets=None):
	if loads(new_widgets):
		widgets = _dict(loads(new_widgets))

		if widgets.chart:
			_upsert_widgets(doc, "charts", widgets.chart, "Workspace Chart")
		if widgets.shortcut:
			_upsert_widgets(doc, "shortcuts", widgets.shortcut, "Workspace Shortcut")
		if widgets.quick_list:
			_upsert_widgets(doc, "quick_lists", widgets.quick_list, "Workspace Quick List")
		if widgets.custom_block:
			_upsert_widgets(doc, "custom_blocks", widgets.custom_block, "Workspace Custom Block")
		if widgets.number_card:
			_upsert_widgets(doc, "number_cards", widgets.number_card, "Workspace Number Card")
		if widgets.card:
			doc.build_links_table_from_card(widgets.card)

	# Apply explicit user deletions (editor "Delete" action) BEFORE clean_up, which
	# would otherwise re-add child rows missing from content (the anti widget-loss
	# guard from 00e69dc) -- that re-add is what made shortcuts impossible to delete.
	if deleted_widgets:
		_dw = frappe.parse_json(deleted_widgets) or {}
		for _wtype in ("shortcut", "chart", "quick_list", "number_card", "custom_block"):
			_names = _dw.get(_wtype) or []
			if _names:
				_field = _wtype + "s"
				doc.set(_field, [w for w in doc.get(_field) if w.label not in _names])

	# remove duplicate and unwanted widgets
	clean_up(doc, blocks)

	try:
		doc.save(ignore_permissions=True)
	except (ValidationError, TypeError) as e:
		# Create a json string to log
		json_config = widgets and dumps(widgets, sort_keys=True, indent=4)

		# Error log body
		log = f"""
		page: {page}
		config: {json_config}
		exception: {e}
		"""
		doc.log_error("Could not save customization", log)
		return False

	return True


def clean_up(original_page, blocks):
	page_widgets = {}
	parsed_blocks = loads(blocks)

	for wid in ["shortcut", "card", "chart", "quick_list", "number_card", "custom_block"]:
		# get list of widget's name from blocks
		page_widgets[wid] = [x["data"][wid + "_name"] for x in parsed_blocks if x["type"] == wid]

	# shortcut, chart, quick_list, number_card & custom_block cleanup
	# Only remove duplicates; preserve widgets missing from content by adding them back
	for wid in ["shortcut", "chart", "quick_list", "number_card", "custom_block"]:
		seen = set()
		updated_widgets = []
		for w in original_page.get(wid + "s"):
			if w.label not in seen:
				seen.add(w.label)
				updated_widgets.append(w)

		# Add content blocks for widgets in child table but missing from content JSON
		for w in updated_widgets:
			if w.label not in page_widgets[wid]:
				parsed_blocks.append({"type": wid, "data": {wid + "_name": w.label, "col": 4}})

		original_page.set(wid + "s", updated_widgets)

	# Update content with any restored widget references
	original_page.content = dumps(parsed_blocks)

	# card cleanup
	for i, v in enumerate(original_page.links):
		if v.type == "Card Break" and v.label not in page_widgets["card"]:
			del original_page.links[i : i + v.link_count + 1]


def new_widget(config, doctype, parentfield):
	if not config:
		return []
	prepare_widget_list = []
	for idx, widget in enumerate(config):
		# Some cleanup
		widget.pop("name", None)

		# New Doc
		doc = frappe.new_doc(doctype)
		doc.update(widget)

		# Manually Set IDX
		doc.idx = idx + 1

		# Set Parent Field
		doc.parentfield = parentfield

		prepare_widget_list.append(doc)
	return prepare_widget_list


def prepare_widget(config, doctype, parentfield):
	"""Create widget child table entries with parent details

	Args:
	        config (dict): Dictionary containing widget config
	        doctype (string): Doctype name of the child table
	        parentfield (string): Parent field for the child table

	Returns:
	        TYPE: List of Document objects
	"""
	if not config:
		return []
	order = config.get("order")
	widgets = config.get("widgets")
	prepare_widget_list = []
	for idx, name in enumerate(order):
		wid_config = widgets[name].copy()
		# Some cleanup
		wid_config.pop("name", None)

		# New Doc
		doc = frappe.new_doc(doctype)
		doc.update(wid_config)

		# Manually Set IDX
		doc.idx = idx + 1

		# Set Parent Field
		doc.parentfield = parentfield

		prepare_widget_list.append(doc)
	return prepare_widget_list


@frappe.whitelist()
def update_onboarding_step(name, field, value):
	"""Update status of onboaridng step

	Args:
	        name (string): Name of the doc
	        field (string): field to be updated
	        value: Value to be updated

	"""
	from frappe.utils.telemetry import capture

	frappe.db.set_value("Onboarding Step", name, field, value)

	capture(frappe.scrub(name), app="frappe_onboarding", properties={field: value})
