import frappe

from frappe.model import rename_field

def execute():
	tables = frappe.db.sql_list("show tables")
	if "tabWebsite Route" not in tables:
		frappe.rename_doc("DocType", "Website Sitemap", "Website Route", force=True)

	if "tabWebsite Template" not in tables:
		frappe.rename_doc("DocType", "Website Sitemap Config", "Website Template", force=True)

	if "tabWebsite Route Permission" not in tables:
		frappe.rename_doc("DocType", "Website Sitemap Permission", "Website Route Permission", force=True)

	for d in ("Blog Category", "Blog Post", "Web Page", "Website Route", "Website Group"):
		frappe.reload_doc("website", "doctype", frappe.scrub(d))
		rename_field_if_exists(d, "parent_website_sitemap", "parent_website_route")

	#frappe.reload_doc("website", "doctype", "website_template")
	frappe.reload_doc("website", "doctype", "website_route")
	frappe.reload_doc("website", "doctype", "website_route_permission")

	#rename_field_if_exists("Website Route", "website_sitemap_config", "website_template")
	rename_field_if_exists("Website Route Permission", "website_sitemap", "website_route")

	for d in ("blog_category", "blog_post", "web_page", "website_route", "website_group", "post", "user_vote"):
		frappe.reload_doc("website", "doctype", d)

def rename_field_if_exists(doctype, old_fieldname, new_fieldname):
	try:
		rename_field(doctype, old_fieldname, new_fieldname)
	except Exception, e:
		if e.args[0] != 1054:
			raise
