// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt

frappe.views.ReportFactory = class ReportFactory extends frappe.views.Factory {
	make(route) {
		const _route = ["List", route[1], "Report"];

		if (route[2]) {
			// custom report
			_route.push(route[2]);
		}

		//// Neoffice — replace, do not push: this factory only rewrites a route
		//// the user is already on. Pushing left the pre-rewrite URL behind it in
		//// the history, so Back re-entered the rewrite and bounced forward again
		//// (see router.js set_doctype_route). Upstream pushes.
		frappe.route_flags.replace_route = true;
		frappe.set_route(_route);
	}
};
