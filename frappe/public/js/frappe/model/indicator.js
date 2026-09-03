// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors

frappe.has_indicator = function (doctype) {
	// returns true if indicator is present
	if (frappe.model.is_submittable(doctype)) {
		return true;
	} else if (
		(frappe.listview_settings[doctype] || {}).get_indicator ||
		frappe.workflow.get_state_fieldname(doctype)
	) {
		return true;
	} else if (
		frappe.meta.has_field(doctype, "enabled") ||
		frappe.meta.has_field(doctype, "disabled")
	) {
		return true;
	} else if (
		frappe.meta.has_field(doctype, "status") &&
		frappe.get_meta(doctype).states.length
	) {
		return true;
	}
	return false;
};

frappe.get_indicator = function (doc, doctype, show_workflow_state) {
	if (doc.__unsaved) {
		return [__("Not Saved", null, doctype), "orange"];
	}

	if (!doctype) doctype = doc.doctype;

	let meta = frappe.get_meta(doctype);
	var workflow = frappe.workflow.workflows[doctype];
	var without_workflow = workflow ? workflow["override_status"] : true;

	var settings = frappe.listview_settings[doctype] || {};

	var is_submittable = frappe.model.is_submittable(doctype);
	let workflow_fieldname = frappe.workflow.get_state_fieldname(doctype);

	let avoid_status_override = (frappe.workflow.avoid_status_override[doctype] || []).includes(
		doc[workflow_fieldname]
	);
	// workflow
	if (
		workflow_fieldname &&
		(!without_workflow || show_workflow_state) &&
		!avoid_status_override
	) {
		var value = doc[workflow_fieldname];
		if (value) {
			let colour = "";

			if (locals["Workflow State"][value] && locals["Workflow State"][value].style) {
				colour = {
					Success: "green",
					Warning: "orange",
					Danger: "red",
					Primary: "blue",
					Inverse: "black",
					Info: "light-blue",
				}[locals["Workflow State"][value].style];
			}
			if (!colour) colour = "gray";

			return [__(value, null, doctype), colour, workflow_fieldname + ",=," + value];
		}
	}

	// draft if document is submittable
	if (is_submittable && doc.docstatus == 0 && !settings.has_indicator_for_draft) {
		//// Neoffice — upstream: "red" (6dd4d68b40, 2026-07-06 "feat(indicators): Draft=gray, Submitted=green
		//// for status pills"): Draft shared the error colour with Cancelled / Overdue / Failed for what is
		//// only a work in progress. Fleet-wide, together with utils.js#guess_style (same commit). This file
		//// also lost its final newline — whitespace only, see NEOFFICE_FORK_MARKERS.md.
		return [__("Draft", null, doctype), "gray", "docstatus,=,0"];
	}

	// cancelled
	if (is_submittable && doc.docstatus == 2 && !settings.has_indicator_for_cancelled) {
		return [__("Cancelled", null, doctype), "red", "docstatus,=,2"];
	}

	// based on document state
	if (doc.status && meta && meta.states && meta.states.find((d) => d.title === doc.status)) {
		let state = meta.states.find((d) => d.title === doc.status);
		let color_class = frappe.scrub(state.color, "-");
		return [__(doc.status, null, doctype), color_class, "status,=," + doc.status];
	}

	if (settings.get_indicator) {
		var indicator = settings.get_indicator(doc);
		if (indicator) return indicator;
	}

	// if submittable
	if (is_submittable && doc.docstatus == 1) {
		//// Neoffice — upstream: "blue" (6dd4d68b40, 2026-07-06): submitted is a validated state, not an
		//// informational one. Cancelled stays red.
		return [__("Submitted", null, doctype), "green", "docstatus,=,1"];
	}

	// based on status
	if (doc.status) {
		return [
			__(doc.status, null, doctype),
			frappe.utils.guess_colour(doc.status),
			"status,=," + doc.status,
		];
	}

	// based on enabled
	if (frappe.meta.has_field(doctype, "enabled")) {
		if (doc.enabled) {
			return [__("Enabled", null, doctype), "blue", "enabled,=,1"];
		} else {
			return [__("Disabled", null, doctype), "grey", "enabled,=,0"];
		}
	}

	// based on disabled
	if (frappe.meta.has_field(doctype, "disabled")) {
		if (doc.disabled) {
			return [__("Disabled", null, doctype), "grey", "disabled,=,1"];
		} else {
			return [__("Enabled", null, doctype), "blue", "disabled,=,0"];
		}
	}
};