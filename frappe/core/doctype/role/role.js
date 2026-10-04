// Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See LICENSE

frappe.ui.form.on("Role", {
	refresh: function (frm) {
		if (frm.doc.name === "All") {
			frm.dashboard.add_comment(
				__("Role 'All' will be given to all system + website users."),
				"yellow"
			);
		} else if (frm.doc.name === "Desk User") {
			frm.dashboard.add_comment(
				__("Role 'Desk User' will be given to all system users."),
				"yellow"
			);
		}

		frm.set_df_property("is_custom", "read_only", frappe.session.user !== "Administrator");

		//// Neoffice — upstream passes the two button labels as bare literals: no msgid is extracted, and the
		//// cockpit's form hero mirrors the label as-is, so both stayed English in a French UI.
		frm.add_custom_button(__("Role Permissions Manager"), function () {
			frappe.route_options = { role: frm.doc.name };
			frappe.set_route("permission-manager");
		});
		//// Neoffice — see the block marker above: this label goes through __() as well (486b7e3cf6
		//// "fix(i18n): the Role form buttons go through __()").
		frm.add_custom_button(__("Show Users"), function () {
			frappe.route_options = { role: frm.doc.name };
			frappe.set_route("List", "User", "Report");
		});
	},
});
