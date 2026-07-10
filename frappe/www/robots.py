import frappe

base_template_path = "www/robots.txt"


def get_context(context):
	#//// Neoffice multi-site: a resolved Website Profile can carry its own robots.txt
	profile = getattr(frappe.local, "website_profile_doc", None)

	#//// Neoffice website switch: an offline site must not be indexed at all.
	if profile is not None and "website_online" in profile and not profile.get("website_online"):
		return {"robots_txt": "User-agent: *\nDisallow: /"}

	profile_robots = (profile or {}).get("robots_txt")

	robots_txt = (
		profile_robots
		or frappe.db.get_single_value("Website Settings", "robots_txt")
		or (frappe.local.conf.robots_txt and frappe.read_file(frappe.local.conf.robots_txt))
		or ""
	)

	return {"robots_txt": robots_txt}
