# Copyright (c) 2020, Frappe Technologies and contributors
# License: MIT. See LICENSE

import json

import frappe

# import frappe
from frappe.model.document import Document


class DashboardSettings(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		chart_config: DF.Code | None
		user: DF.Link | None
	# end: auto-generated types
	pass


@frappe.whitelist()
def create_dashboard_settings(user):
	if not frappe.db.exists("Dashboard Settings", user):
		doc = frappe.new_doc("Dashboard Settings")
		doc.name = user
		# //// Neoffice — a workspace loads several charts at once and each one asks for
		# //// these settings: two requests could both find none and both insert, and the
		# //// loser showed "Duplicate Name" on a new account's first visit (maintenance#828).
		# //// The loser now returns the settings the winner created. Drop this once upstream
		# //// handles the race.
		try:
			doc.insert(ignore_permissions=True)
		except frappe.DuplicateEntryError:
			frappe.clear_last_message()
			return frappe.get_doc("Dashboard Settings", user)
		frappe.db.commit()
		return doc


def get_permission_query_conditions(user):
	if not user:
		user = frappe.session.user

	return f"""(`tabDashboard Settings`.name = {frappe.db.escape(user)})"""


@frappe.whitelist()
def save_chart_config(reset, config, chart_name):
	reset = frappe.parse_json(reset)
	doc = frappe.get_doc("Dashboard Settings", frappe.session.user)
	chart_config = frappe.parse_json(doc.chart_config) or {}

	if reset:
		chart_config[chart_name] = {}
	else:
		config = frappe.parse_json(config)
		if chart_name not in chart_config:
			chart_config[chart_name] = {}
		chart_config[chart_name].update(config)

	frappe.db.set_value("Dashboard Settings", frappe.session.user, "chart_config", json.dumps(chart_config))
