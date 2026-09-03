// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt

frappe.provide("frappe.pages");
frappe.provide("frappe.views");

frappe.views.Factory = class Factory {
	constructor(opts) {
		$.extend(this, opts);
	}

	show() {
		this.route = frappe.get_route();
		this.page_name = frappe.get_route_str();

		if (this.before_show && this.before_show() === false) return;

		if (frappe.pages[this.page_name]) {
			frappe.container.change_to(this.page_name);
			if (this.on_show) {
				this.on_show();
			}
		} else {
			if (this.route[1]) {
				this.make(this.route);
			} else {
				frappe.show_not_found(this.route);
			}
		}
	}

	//// Neoffice — upstream v15: make_page(double_column, page_name, hide_sidebar). Renamed with the
	//// sidebar-on-the-right rework (cc297ec402, 2025-11-03 "Move sidebar to right and add mobile
	//// toggle"): the third argument stopped being a boolean "hide it" and became the side to put it
	//// on, read by ui/page.js (marked there). Same shape as frappe develop (v16), which spells the
	//// parameter "sidebar_postition" here — ours fixes that typo.
	make_page(double_column, page_name, sidebar_position) {
		return frappe.make_page(double_column, page_name, sidebar_position);
	}
};

//// Neoffice — upstream v15: frappe.make_page(double_column, page_name, disable_sidebar_toggle),
//// passing that flag straight through to frappe.ui.make_app_page. Ours passes sidebar_position
//// and derives disable_sidebar_toggle: !sidebar_position from it (the two lines below) — a page
//// that names no side gets no toggle. Verbatim from frappe develop (v16) factory.js, brought in
//// by cc297ec402 (2025-11-03) together with the page.js layout-two-column template.
frappe.make_page = function (double_column, page_name, sidebar_position) {
	if (!page_name) {
		page_name = frappe.get_route_str();
	}

	const page = frappe.container.add_page(page_name);

	frappe.ui.make_app_page({
		parent: page,
		single_column: !double_column,
		sidebar_position: sidebar_position,
		disable_sidebar_toggle: !sidebar_position,
	});

	frappe.container.change_to(page_name);
	return page;
};
