import frappe
from frappe import _


def get_modules_from_all_apps_for_user(user: str | None = None) -> list[dict]:
	user = user or frappe.session.user
	all_modules = get_modules_from_all_apps()
	global_blocked_modules = frappe.get_doc("User", "Administrator").get_blocked_modules()
	user_blocked_modules = frappe.get_doc("User", user).get_blocked_modules()
	blocked_modules = global_blocked_modules + user_blocked_modules
	allowed_modules_list = [m for m in all_modules if m.get("module_name") not in blocked_modules]

	empty_tables_by_module = get_all_empty_tables_by_module()

	for module in allowed_modules_list:
		module_name = module.get("module_name")

		# Apply onboarding status
		if module_name in empty_tables_by_module:
			module["onboard_present"] = 1

	return allowed_modules_list


def get_modules_from_all_apps():
	modules_list = []
	for app in frappe.get_installed_apps():
		modules_list += get_modules_from_app(app)
	return modules_list


def get_modules_from_app(app):
	return frappe.get_all("Module Def", filters={"app_name": app}, fields=["module_name", "app_name as app"])


# //// Neoffice — the answer of get_all_empty_tables_by_module() is kept for an hour (see below).
EMPTY_TABLES_CACHE_KEY = "empty_tables_by_module"
EMPTY_TABLES_CACHE_TTL = 60 * 60


def get_all_empty_tables_by_module():
	# //// Neoffice — cached for an hour (neoffice-maintenance#805). Upstream ran an information_schema
	# //// scan plus a get_all("DocType") on EVERY call (about 40 ms on a clone of a production
	# //// database), and the call sits behind get_modules_from_all_apps_for_user(), which the
	# //// permission query conditions of Dashboard, Dashboard Chart and Number Card run for anyone who
	# //// is neither Administrator nor System Manager: every list query on those doctypes paid it again.
	# //// The answer only feeds `onboard_present`, which nothing in the apps we ship reads, and the loop
	# //// below unpacks the KEYS of each get_all row (a dict), so the answer is always empty today. The
	# //// cache keeps that behaviour exactly and drops the cost; frappe.clear_cache() (migrate, install,
	# //// bench clear-cache) drops it at once. Read with `is not None`, so an empty answer is cached
	# //// too. Drop this wrapper when the fleet runs a Frappe whose upstream caches this result.
	empty_tables_by_module = frappe.cache.get_value(EMPTY_TABLES_CACHE_KEY, expires=True)
	if empty_tables_by_module is None:
		empty_tables_by_module = compute_all_empty_tables_by_module()
		frappe.cache.set_value(
			EMPTY_TABLES_CACHE_KEY, empty_tables_by_module, expires_in_sec=EMPTY_TABLES_CACHE_TTL
		)
	return empty_tables_by_module


# //// Neoffice — the upstream body of get_all_empty_tables_by_module(), unchanged; renamed so that
# //// the public name above can cache it.
def compute_all_empty_tables_by_module():
	table_rows = frappe.qb.Field("table_rows")
	table_name = frappe.qb.Field("table_name")
	information_schema = frappe.qb.Schema("information_schema")

	empty_tables = (
		frappe.qb.from_(information_schema.tables).select(table_name).where(table_rows == 0)
	).run()

	empty_tables = {r[0] for r in empty_tables}

	results = frappe.get_all("DocType", fields=["name", "module"])
	empty_tables_by_module = {}

	for doctype, module in results:
		if f"tab{doctype}" in empty_tables:
			if module in empty_tables_by_module:
				empty_tables_by_module[module].append(doctype)
			else:
				empty_tables_by_module[module] = [doctype]
	return empty_tables_by_module


def is_domain(module):
	return module.get("category") == "Domains"


def is_module(module):
	return module.get("type") == "module"
